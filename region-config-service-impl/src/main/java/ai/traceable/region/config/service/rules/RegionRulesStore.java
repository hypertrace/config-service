package ai.traceable.region.config.service.rules;

import static ai.traceable.region.config.service.constants.RegionConfigConstants.REGION_RULE_CONFIG_NAMESPACE;
import static ai.traceable.region.config.service.constants.RegionConfigConstants.REGION_RULE_CONFIG_RESOURCE_NAME;

import ai.traceable.config.commons.v1.AuditDetails;
import ai.traceable.config.commons.v1.AuditFilter;
import ai.traceable.config.commons.v1.CreationDetails;
import ai.traceable.config.commons.v1.LastUpdateDetails;
import ai.traceable.config.commons.v1.TimestampRange;
import ai.traceable.config.utils.TimestampConverter;
import ai.traceable.region.config.service.v1.GetRegionRulesFilter;
import ai.traceable.region.config.service.v1.RegionRule;
import ai.traceable.region.config.service.v1.RegionRuleRecord;
import ai.traceable.region.config.service.v1.RuleScope;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class RegionRulesStore
    extends IdentifiedObjectStoreWithFilter<RegionRule, GetRegionRulesFilter> {

  private final TimestampConverter timestampConverter;

  @Inject
  public RegionRulesStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator,
      TimestampConverter timestampConverter) {
    super(
        configServiceBlockingStub,
        REGION_RULE_CONFIG_NAMESPACE,
        REGION_RULE_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
    this.timestampConverter = timestampConverter;
  }

  @Override
  protected Optional<RegionRule> buildDataFromValue(Value value) {
    try {
      RegionRule.Builder builder = RegionRule.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (Exception exception) {
      log.error("Parsing the value {} into RegionRule failed with an exception", value, exception);
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(RegionRule rule) {
    return ConfigProtoConverter.convertToValue(rule);
  }

  @Override
  protected String getContextFromData(RegionRule rule) {
    return rule.getId();
  }

  @Override
  protected Optional<RegionRule> filterConfigData(RegionRule data, GetRegionRulesFilter filter) {
    return Optional.of(data)
        .filter(rule -> !(filter.hasDisabled() && rule.getDisabled() != filter.getDisabled()))
        .filter(rule -> !(filter.hasInternal() && rule.getInternal() != filter.getInternal()))
        .filter(rule -> !filter.hasHidden() || filter.getHidden() == rule.getHidden())
        .filter(
            rule ->
                filter.getRuleActionTypesCount() == 0
                    || filter.getRuleActionTypesList().contains(rule.getActionType()))
        .filter(rule -> filterRuleOnScope(rule, filter.getRuleScope()));
  }

  /**
   * Method to filter on rule-scope * If filterScope has no environment scope, always return true *
   * If filterScope has environment scope but the environment scope has no environment IDs, return
   * true only if the rule has no Environment IDs in its rule-scope. * If filterScope has
   * environment scope and the environment scope has one or more environment IDs, return true only
   * if there is at least one overlap of environment ID between the filter and the rule.
   */
  private boolean filterRuleOnScope(RegionRule ruleData, RuleScope filterScope) {
    List<String> ruleEnvironmentIds =
        ruleData.getRuleScope().getEnvironmentScope().getEnvironmentIdsList();
    if (!filterScope.hasEnvironmentScope() || ruleEnvironmentIds.isEmpty()) {
      return true;
    }

    List<String> filterEnvironmentIds = filterScope.getEnvironmentScope().getEnvironmentIdsList();
    return ruleEnvironmentIds.stream().anyMatch(filterEnvironmentIds::contains);
  }

  public List<RegionRuleRecord> getRuleRecords(
      RequestContext context, GetRegionRulesFilter filter) {
    boolean isDefaultFilter = GetRegionRulesFilter.getDefaultInstance().equals(filter);
    return getAllObjects(context).stream()
        .filter(ruleWithContext -> isDefaultFilter || matchesFilter(ruleWithContext, filter))
        .map(this::toRuleRecord)
        .collect(Collectors.toUnmodifiableList());
  }

  private boolean matchesFilter(
      ContextualConfigObject<RegionRule> ruleWithContext, GetRegionRulesFilter filter) {
    return filterConfigData(ruleWithContext.getData(), filter).isPresent()
        && matchesAuditFilter(ruleWithContext, filter.getAuditFilter());
  }

  private boolean matchesAuditFilter(
      ContextualConfigObject<RegionRule> contextual, AuditFilter auditFilter) {
    if (AuditFilter.getDefaultInstance().equals(auditFilter)) {
      return true;
    }
    return matchesCreatedRange(contextual, auditFilter)
        && matchesUpdatedRange(contextual, auditFilter)
        && matchesCreatedByContains(contextual, auditFilter)
        && matchesLastUpdatedByContains(contextual, auditFilter);
  }

  private boolean matchesCreatedRange(
      ContextualConfigObject<RegionRule> contextual, AuditFilter auditFilter) {
    if (!auditFilter.hasCreatedRange()) {
      return true;
    }
    Instant creationTimestamp = contextual.getCreationTimestamp();
    if (creationTimestamp == null) {
      return false;
    }
    return isTimestampInRange(creationTimestamp, auditFilter.getCreatedRange());
  }

  private boolean matchesUpdatedRange(
      ContextualConfigObject<RegionRule> contextual, AuditFilter auditFilter) {
    if (!auditFilter.hasUpdatedRange()) {
      return true;
    }
    Instant lastUserUpdateTimestamp = contextual.getLastUserUpdateTimestamp();
    if (lastUserUpdateTimestamp == null) {
      return false;
    }
    return isTimestampInRange(lastUserUpdateTimestamp, auditFilter.getUpdatedRange());
  }

  private boolean matchesCreatedByContains(
      ContextualConfigObject<RegionRule> contextual, AuditFilter auditFilter) {
    String createdByContains = auditFilter.getCreatedByContains();
    if (createdByContains.isEmpty()) {
      return true;
    }
    String createdByEmail = contextual.getCreatedByEmail();
    return createdByEmail != null
        && createdByEmail.toLowerCase().contains(createdByContains.toLowerCase());
  }

  private boolean matchesLastUpdatedByContains(
      ContextualConfigObject<RegionRule> contextual, AuditFilter auditFilter) {
    String lastUpdatedByContains = auditFilter.getLastUpdatedByUserContains();
    if (lastUpdatedByContains.isEmpty()) {
      return true;
    }
    String lastUserUpdateEmail = contextual.getLastUserUpdateEmail();
    return lastUserUpdateEmail != null
        && lastUserUpdateEmail.toLowerCase().contains(lastUpdatedByContains.toLowerCase());
  }

  private boolean isTimestampInRange(Instant timestamp, TimestampRange range) {
    if (range.hasStart()) {
      Instant start = timestampConverter.convertToInstant(range.getStart());
      if (timestamp.isBefore(start)) {
        return false;
      }
    }
    if (range.hasEnd()) {
      Instant end = timestampConverter.convertToInstant(range.getEnd());
      if (timestamp.isAfter(end)) {
        return false;
      }
    }
    return true;
  }

  private RegionRuleRecord toRuleRecord(ContextualConfigObject<RegionRule> contextual) {
    return RegionRuleRecord.newBuilder()
        .setRule(contextual.getData())
        .setAuditDetails(buildAuditDetails(contextual))
        .build();
  }

  private AuditDetails buildAuditDetails(ContextualConfigObject<?> contextual) {
    AuditDetails.Builder builder = AuditDetails.newBuilder();

    Instant creationTimestamp = contextual.getCreationTimestamp();
    if (creationTimestamp != null && creationTimestamp.getEpochSecond() > 0) {
      builder.setCreationDetails(
          CreationDetails.newBuilder()
              .setCreatedBy(contextual.getCreatedByEmail())
              .setCreatedAt(timestampConverter.convert(creationTimestamp))
              .build());
    }

    Instant lastUserUpdateTimestamp = contextual.getLastUserUpdateTimestamp();
    if (lastUserUpdateTimestamp != null && lastUserUpdateTimestamp.getEpochSecond() > 0) {
      builder.setLastUserUpdateDetails(
          LastUpdateDetails.newBuilder()
              .setUpdatedBy(contextual.getLastUserUpdateEmail())
              .setUpdatedAt(timestampConverter.convert(lastUserUpdateTimestamp))
              .build());
    }

    return builder.build();
  }
}
