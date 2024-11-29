package ai.traceable.blocking.config.service.v2.blockingpolicy.exclusion;

import static ai.traceable.blocking.config.service.v2.BlockingCategory.BLOCKING_CATEGORY_DATA_EXFILTRATION;
import static ai.traceable.blocking.config.service.v2.BlockingCategory.BLOCKING_CATEGORY_ENUMERATION;
import static ai.traceable.blocking.config.service.v2.BlockingCategory.BLOCKING_CATEGORY_MALICIOUS_SOURCES_RULE;
import static ai.traceable.blocking.config.service.v2.BlockingCategory.BLOCKING_CATEGORY_RATE_LIMIT;
import static ai.traceable.blocking.config.service.v2.BlockingCategory.BLOCKING_CATEGORY_THREAT_ACTOR;
import static ai.traceable.blocking.config.service.v2.BlockingCategory.BLOCKING_CATEGORY_TRANSACTION_BASED_DLP;
import static ai.traceable.blocking.config.service.v2.BlockingCategory.BLOCKING_CATEGORY_UNSPECIFIED;
import static ai.traceable.detection.exclusion.config.service.v1.ThreatActorIdentifier.THREAT_ACTOR_IDENTIFIER_ACTOR_ENTITY_ID;

import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.actor.ActorStatusDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.actor.ActorStore;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.actor.config.ActorServiceConfig;
import ai.traceable.blocking.config.service.v2.BlockingCategory;
import ai.traceable.blocking.config.service.v2.BlockingDetailsCombination;
import ai.traceable.blocking.config.service.v2.BlockingDetailsCombination.ConditionsOperator;
import ai.traceable.blocking.config.service.v2.BlockingDetailsCondition;
import ai.traceable.blocking.config.service.v2.CustomSignatureDetails;
import ai.traceable.blocking.config.service.v2.ExclusionRule;
import ai.traceable.blocking.config.service.v2.ExclusionRule.AnomalousAttributeCondition;
import ai.traceable.blocking.config.service.v2.ExclusionRule.EventCondition;
import ai.traceable.blocking.config.service.v2.ExclusionRule.EventCondition.Builder;
import ai.traceable.blocking.config.service.v2.IpDetails;
import ai.traceable.blocking.config.service.v2.IpType;
import ai.traceable.blocking.config.service.v2.IpTypeDetails;
import ai.traceable.blocking.config.service.v2.MatchExpression;
import ai.traceable.blocking.config.service.v2.MatchOperator;
import ai.traceable.blocking.config.service.v2.RegionDetails;
import ai.traceable.detection.exclusion.config.service.v1.CustomRuleEvent;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionCondition;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionModsecRule;
import ai.traceable.detection.exclusion.config.service.v1.ExclusionTarget;
import ai.traceable.detection.exclusion.config.service.v1.IpAddressCondition;
import ai.traceable.detection.exclusion.config.service.v1.IpLocationType;
import ai.traceable.detection.exclusion.config.service.v1.IpLocationTypeCondition;
import ai.traceable.detection.exclusion.config.service.v1.RegionCondition;
import ai.traceable.detection.exclusion.config.service.v1.RegionCondition.Region;
import ai.traceable.detection.exclusion.config.service.v1.SystemDefinedEventFamily;
import ai.traceable.detection.exclusion.config.service.v1.ThreatActorEvent;
import ai.traceable.modsecurity.utils.ModsecRuleUtils;
import ai.traceable.platform.actor.v1.StatusChangeSource;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;

@Slf4j
public class ExcludeRuleConverterImpl implements ExclusionRuleConverter {

  private static final String ENTITY_ID_TO_STATUS_CHANGE_SOURCE_CACHE =
      "EntityIdToStatusChangeSourceCache";
  private static final Map<ExclusionTarget, ExclusionRule.ExclusionTarget> EXCLUSION_TARGET_MAP =
      Map.of(
          ExclusionTarget.EXCLUSION_TARGET_ALLOW,
              ExclusionRule.ExclusionTarget.EXCLUSION_TARGET_ALLOW,
          ExclusionTarget.EXCLUSION_TARGET_BLOCK,
              ExclusionRule.ExclusionTarget.EXCLUSION_TARGET_BLOCK);

  private final ModsecRuleUtils modsecRuleUtils;
  private final ActorStore actorStore;
  private final ActorServiceConfig actorServiceConfig;
  private LoadingCache<ContextualKey<Void>, Map<String, StatusChangeSource>>
      entityIdToStatusChangeSource;

  @Inject
  public ExcludeRuleConverterImpl(
      ActorServiceConfig actorServiceConfig,
      ModsecRuleUtils modsecRuleUtils,
      ActorStore actorStore) {
    this.modsecRuleUtils = modsecRuleUtils;
    this.actorStore = actorStore;
    this.actorServiceConfig = actorServiceConfig;
    initCache();
  }

  private void initCache() {
    this.entityIdToStatusChangeSource =
        CacheBuilder.newBuilder()
            .maximumSize(actorServiceConfig.getCacheConfig().getMaxCacheSize())
            .expireAfterWrite(actorServiceConfig.getCacheConfig().getWriteExpirationDuration())
            .refreshAfterWrite(actorServiceConfig.getCacheConfig().getRefreshExpirationDuration())
            .recordStats()
            .build(
                CacheLoader.asyncReloading(
                    CacheLoader.from(this::loadValue), Executors.newSingleThreadExecutor()));
    PlatformMetricsRegistry.registerCacheTrackingOccupancy(
        ENTITY_ID_TO_STATUS_CHANGE_SOURCE_CACHE,
        this.entityIdToStatusChangeSource,
        Collections.emptyMap(),
        actorServiceConfig.getCacheConfig().getMaxCacheSize());
  }

  private Map<String, StatusChangeSource> loadValue(ContextualKey<Void> contextualKey) {
    return actorStore.getActiveThreatActors(contextualKey.getContext(), Optional.empty()).stream()
        .collect(
            Collectors.toMap(
                ActorStatusDetails::getEntityId, ActorStatusDetails::getStatusChangeSource));
  }

  @Override
  public ExclusionRule convert(DetectionExclusionModsecRule detectionExclusionModsecRule) {
    ExclusionRule.Builder exclusionRuleBuilder =
        ExclusionRule.newBuilder().setRuleId(detectionExclusionModsecRule.getRule().getId());

    // Add match conditions corresponding to modsec rules
    exclusionRuleBuilder.setDetails(
        BlockingDetailsCombination.newBuilder()
            .setOperator(ConditionsOperator.CONDITIONS_OPERATOR_AND)
            .addAllDetailsConditions(
                detectionExclusionModsecRule.getAssociatedModsecRuleIdsList().stream()
                    .map(
                        ruleId ->
                            BlockingDetailsCondition.newBuilder()
                                .setCustomSignatureDetails(
                                    CustomSignatureDetails.newBuilder().setRuleId(ruleId))
                                .build())
                    .collect(Collectors.toUnmodifiableList())));

    detectionExclusionModsecRule
        .getRule()
        .getRuleInfo()
        .getConditionsList()
        .forEach(condition -> convertCondition(condition, exclusionRuleBuilder));

    exclusionRuleBuilder.addAllTargets(
        detectionExclusionModsecRule.getRule().getRuleInfo().getExclusionTargetsList().stream()
            .map(EXCLUSION_TARGET_MAP::get)
            .filter(Objects::nonNull)
            .collect(Collectors.toUnmodifiableList()));

    return exclusionRuleBuilder.build();
  }

  private void convertCondition(
      DetectionExclusionCondition condition, ExclusionRule.Builder builder) {
    switch (condition.getConditionCase()) {
      case SCOPE_CONDITION:
        // Handled by modsec rule when URL scope
        break;
      case ATTRIBUTE_MATCH_CONDITION:
        // Handled by modsec rule when URL condition
        break;
      case IP_LOCATION_TYPE_CONDITION:
        addDetailsCondition(builder, convert(condition.getIpLocationTypeCondition()));
        break;
      case EVENT_CONDITION:
        convertEventCondition(condition.getEventCondition()).forEach(builder::addEventConditions);
        break;
      case ANOMALOUS_ATTRIBUTE_CONDITION:
        builder.addAnomalousAttributeConditions(
            convertAttributeCondition(condition.getAnomalousAttributeCondition()));
        break;
      case IP_ADDRESS_CONDITION:
        addDetailsCondition(builder, convert(condition.getIpAddressCondition()));
        break;
      case REGION_CONDITION:
        addDetailsCondition(builder, convert(condition.getRegionCondition()));
        break;
      default:
        log.warn("Unsupported condition type: {}", condition.getConditionCase());
    }
  }

  private void addDetailsCondition(
      ExclusionRule.Builder builder, BlockingDetailsCondition condition) {
    builder.getDetailsBuilder().addDetailsConditions(condition);
  }

  private BlockingDetailsCondition convert(IpAddressCondition ipAddressCondition) {
    BlockingDetailsCondition condition =
        BlockingDetailsCondition.newBuilder()
            .setIpDetails(
                IpDetails.newBuilder()
                    .addAllIpAddresses(ipAddressCondition.getIpAddressesList())
                    .addAllIpRanges(ipAddressCondition.getCidrIpRangesList()))
            .build();

    return wrapInNotConditionIfExcluded(ipAddressCondition.getExclude(), condition);
  }

  private BlockingDetailsCondition convert(RegionCondition regionCondition) {
    BlockingDetailsCondition condition =
        BlockingDetailsCondition.newBuilder()
            .setRegionDetails(
                RegionDetails.newBuilder()
                    .addAllRegions(
                        regionCondition.getRegionsList().stream()
                            .map(Region::getCountryIsoCode)
                            .collect(Collectors.toUnmodifiableList())))
            .build();

    return wrapInNotConditionIfExcluded(regionCondition.getExclude(), condition);
  }

  private BlockingDetailsCondition convert(IpLocationTypeCondition ipTypeLocation) {
    BlockingDetailsCondition condition =
        BlockingDetailsCondition.newBuilder()
            .setIpTypeDetails(
                IpTypeDetails.newBuilder()
                    .addAllIpTypes(
                        ipTypeLocation.getIpLocationTypesList().stream()
                            .map(this::convert)
                            .collect(Collectors.toUnmodifiableList())))
            .build();

    return wrapInNotConditionIfExcluded(ipTypeLocation.getExclude(), condition);
  }

  private List<EventCondition> convertEventCondition(
      ai.traceable.detection.exclusion.config.service.v1.EventCondition condition) {
    List<EventCondition> eventConditions = new ArrayList<>();

    // Convert SystemDefinedEvent to EventCondition
    Builder systemEventCondition =
        EventCondition.newBuilder()
            .setBlockingCategory(BlockingCategory.BLOCKING_CATEGORY_MODSECURITY);
    condition.getSystemDefinedEventsList().stream()
        .filter(
            systemEvent ->
                systemEvent.getEventFamily()
                    == SystemDefinedEventFamily.SYSTEM_DEFINED_EVENT_FAMILY_MODSEC)
        .forEach(
            systemEvent -> {
              // For modsec rules we would be working numeric id in the agent
              if (systemEvent.hasEventTypeId()) {
                systemEventCondition.addIdPrefixes(
                    String.valueOf(
                        modsecRuleUtils.getModsecCrsRuleIdNumber(systemEvent.getEventTypeId())));
              } else if (systemEvent.hasEventSubTypeId()) {
                systemEventCondition.addIds(
                    String.valueOf(
                        modsecRuleUtils.getModsecCrsRuleIdNumber(systemEvent.getEventSubTypeId())));
              }
            });
    if (!systemEventCondition.getIdsList().isEmpty()
        || !systemEventCondition.getIdPrefixesList().isEmpty()) {
      eventConditions.add(systemEventCondition.build());
    }

    condition
        .getThreatActorEventsList()
        .forEach(
            threatActorEvent -> {
              for (BlockingCategory blockingCategory : getBlockingCategory(threatActorEvent)) {
                EventCondition.Builder eventConditionBuilder = EventCondition.newBuilder();
                eventConditionBuilder.setBlockingCategory(blockingCategory);
                if (!threatActorEvent.getActorEntityId().isBlank()) {
                  eventConditionBuilder.addIds(threatActorEvent.getActorEntityId());
                }
                eventConditions.add(eventConditionBuilder.build());
              }
            });

    condition
        .getCustomRuleEventsList()
        .forEach(
            customRuleEvent -> {
              EventCondition.Builder eventConditionBuilder =
                  EventCondition.newBuilder()
                      .setBlockingCategory(getBlockingCategory(customRuleEvent));

              if (!customRuleEvent.getRuleId().isBlank()) {
                eventConditionBuilder.addIds(customRuleEvent.getRuleId());
              }

              eventConditions.add(eventConditionBuilder.build());
            });

    return eventConditions;
  }

  private List<BlockingCategory> getBlockingCategory(ThreatActorEvent threatActorEvent) {
    if (threatActorEvent
        .getThreatActorIdentifier()
        .equals(THREAT_ACTOR_IDENTIFIER_ACTOR_ENTITY_ID)) {
      log.warn(
          "Unrecognized threat actor identifier: {}", threatActorEvent.getThreatActorIdentifier());
      return List.of(BLOCKING_CATEGORY_UNSPECIFIED);
    }

    if (threatActorEvent.getActorEntityId().isBlank()) {
      return List.of(
          BLOCKING_CATEGORY_THREAT_ACTOR,
          BLOCKING_CATEGORY_RATE_LIMIT,
          BLOCKING_CATEGORY_DATA_EXFILTRATION,
          BLOCKING_CATEGORY_ENUMERATION,
          BLOCKING_CATEGORY_TRANSACTION_BASED_DLP,
          BLOCKING_CATEGORY_MALICIOUS_SOURCES_RULE);
    }

    StatusChangeSource statusChangeSource =
        this.entityIdToStatusChangeSource
            .getUnchecked(RequestContext.CURRENT.get().buildInternalContextualKey())
            .get(threatActorEvent.getActorEntityId());
    switch (statusChangeSource) {
      case STATUS_CHANGE_SOURCE_RATE_LIMIT:
        return List.of(
            BLOCKING_CATEGORY_RATE_LIMIT,
            BLOCKING_CATEGORY_DATA_EXFILTRATION,
            BLOCKING_CATEGORY_ENUMERATION,
            BLOCKING_CATEGORY_TRANSACTION_BASED_DLP);
      case STATUS_CHANGE_SOURCE_MALICIOUS_SOURCES:
        return List.of(BLOCKING_CATEGORY_MALICIOUS_SOURCES_RULE);
      default:
        return List.of(BLOCKING_CATEGORY_THREAT_ACTOR);
    }
  }

  private BlockingDetailsCondition wrapInNotConditionIfExcluded(
      boolean exclude, BlockingDetailsCondition condition) {
    if (exclude) {
      return BlockingDetailsCondition.newBuilder()
          .setDetailsCombination(
              BlockingDetailsCombination.newBuilder()
                  .setOperator(ConditionsOperator.CONDITIONS_OPERATOR_NOT)
                  .addDetailsConditions(condition))
          .build();
    }
    return condition;
  }

  private AnomalousAttributeCondition convertAttributeCondition(
      ai.traceable.detection.exclusion.config.service.v1.AnomalousAttributeCondition condition) {
    AnomalousAttributeCondition.Builder parsedConditionBuilder =
        AnomalousAttributeCondition.newBuilder();

    if (condition.hasKeyMatchCondition()) {
      parsedConditionBuilder.setKeyExpression(
          convertMatchCondition(condition.getKeyMatchCondition()));
    }

    if (condition.hasValueMatchCondition()) {
      parsedConditionBuilder.setValueExpression(
          convertMatchCondition(condition.getValueMatchCondition()));
    }

    return parsedConditionBuilder.build();
  }

  private MatchExpression convertMatchCondition(
      ai.traceable.detection.exclusion.config.service.v1.MatchCondition condition) {
    return MatchExpression.newBuilder()
        .setOperator(convertOperator(condition.getOperator()))
        .setValue(condition.getValue())
        .build();
  }

  private BlockingCategory getBlockingCategory(CustomRuleEvent event) {
    switch (event.getRuleFamily()) {
      case CUSTOM_RULE_FAMILY_SIGNATURE:
        return BlockingCategory.BLOCKING_CATEGORY_CUSTOM_SIGNATURE_RULE;
      case CUSTOM_RULE_FAMILY_RATE_LIMIT:
        return BLOCKING_CATEGORY_RATE_LIMIT;
      case CUSTOM_RULE_FAMILY_MALICIOUS_SOURCES:
        return BLOCKING_CATEGORY_MALICIOUS_SOURCES_RULE;
      case CUSTOM_RULE_FAMILY_DATA_LOSS_PREVENTION:
        return BLOCKING_CATEGORY_TRANSACTION_BASED_DLP;
      case CUSTOM_RULE_FAMILY_ENUMERATION:
        return BLOCKING_CATEGORY_ENUMERATION;
      case CUSTOM_RULE_FAMILY_UNSPECIFIED:
      default:
        log.warn("Unrecognized custom rule family: {}", event.getRuleFamily());
        return BLOCKING_CATEGORY_UNSPECIFIED;
    }
  }

  private MatchOperator convertOperator(
      ai.traceable.detection.exclusion.config.service.v1.MatchOperator oldOperator) {
    switch (oldOperator) {
      case MATCH_OPERATOR_EQUALS:
        return MatchOperator.MATCH_OPERATOR_EQUALS;
      case MATCH_OPERATOR_NOT_EQUAL:
        return MatchOperator.MATCH_OPERATOR_NOT_EQUALS;
      case MATCH_OPERATOR_MATCHES_REGEX:
        return MatchOperator.MATCH_OPERATOR_REGEX_MATCH;
      case MATCH_OPERATOR_NOT_MATCH_REGEX:
        return MatchOperator.MATCH_OPERATOR_NOT_CONTAINS;
      case MATCH_OPERATOR_GREATER_THAN:
        return MatchOperator.MATCH_OPERATOR_GREATER_THAN;
      case MATCH_OPERATOR_LESS_THAN:
        return MatchOperator.MATCH_OPERATOR_LESS_THAN;
      default:
        return MatchOperator.MATCH_OPERATOR_UNSPECIFIED;
    }
  }

  private IpType convert(IpLocationType locationType) {
    switch (locationType) {
      case IP_LOCATION_TYPE_UNSPECIFIED:
        return IpType.IP_TYPE_UNSPECIFIED;
      case IP_LOCATION_TYPE_ANONYMOUS_VPN:
        return IpType.IP_TYPE_VPN;
      case IP_LOCATION_TYPE_HOSTING_PROVIDER:
        return IpType.IP_TYPE_HOSTING_PROVIDER;
      case IP_LOCATION_TYPE_PUBLIC_PROXY:
        return IpType.IP_TYPE_PROXY;
      case IP_LOCATION_TYPE_TOR_EXIT_NODE:
        return IpType.IP_TYPE_TOR;
      case IP_LOCATION_TYPE_BOT:
        return IpType.IP_TYPE_BOT;
      default:
        throw new IllegalArgumentException("Unknown IpLocationType: " + locationType);
    }
  }
}
