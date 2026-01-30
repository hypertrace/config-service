package ai.traceable.malicioussources.config.service.rules;

import static ai.traceable.config.service.commons.utils.AuditFilterUtils.getVisibleUserEmail;
import static ai.traceable.malicioussources.config.service.constants.MaliciousSourcesConfigConstants.MALICIOUS_SOURCES_RULE_CONFIG_NAMESPACE;
import static ai.traceable.malicioussources.config.service.constants.MaliciousSourcesConfigConstants.MALICIOUS_SOURCES_RULE_CONFIG_RESOURCE_NAME;

import ai.traceable.config.commons.v1.AuditDetails;
import ai.traceable.config.commons.v1.AuditFilter;
import ai.traceable.config.commons.v1.CreationDetails;
import ai.traceable.config.commons.v1.LastUpdateDetails;
import ai.traceable.config.commons.v1.TimestampRange;
import ai.traceable.config.utils.TimestampConverter;
import ai.traceable.malicioussources.config.service.v1.GetRulesFilter;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRule;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleRecord;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleScope;
import com.google.protobuf.Value;
import jakarta.inject.Inject;
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
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class MaliciousSourcesRulesStore
    extends IdentifiedObjectStoreWithFilter<MaliciousSourcesRule, GetRulesFilter> {

  private final TimestampConverter timestampConverter;
  private final MaliciousSourcesConfigServiceConfig serviceConfig;

  @Inject
  public MaliciousSourcesRulesStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator,
      TimestampConverter timestampConverter,
      MaliciousSourcesConfigServiceConfig serviceConfig) {
    super(
        configServiceBlockingStub,
        MALICIOUS_SOURCES_RULE_CONFIG_NAMESPACE,
        MALICIOUS_SOURCES_RULE_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
    this.serviceConfig = serviceConfig;
    this.timestampConverter = timestampConverter;
  }

  @Override
  protected Optional<MaliciousSourcesRule> buildDataFromValue(Value value) {
    try {
      MaliciousSourcesRule.Builder builder = MaliciousSourcesRule.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (Exception exception) {
      log.error(
          "Parsing the value {} into MaliciousSourcesRule failed with an exception",
          value,
          exception);
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(MaliciousSourcesRule rule) {
    return ConfigProtoConverter.convertToValue(rule);
  }

  @Override
  protected String getContextFromData(MaliciousSourcesRule rule) {
    return rule.getId();
  }

  @Override
  protected Optional<MaliciousSourcesRule> filterConfigData(
      MaliciousSourcesRule ruleData, GetRulesFilter filter) {
    return Optional.of(ruleData)
        .filter(
            rule -> filter.getRuleIdsCount() == 0 || filter.getRuleIdsList().contains(rule.getId()))
        .filter(rule -> filterRuleOnScope(rule, filter.getRuleScope()))
        .filter(
            rule ->
                !(filter.hasDisabled()
                    && rule.getRuleStatus().getDisabled() != filter.getDisabled()))
        .filter(
            rule -> !filter.hasHidden() || filter.getHidden() == rule.getRuleStatus().getHidden())
        .filter(
            rule ->
                filter.getRuleActionTypesCount() == 0
                    || filter
                        .getRuleActionTypesList()
                        .contains(rule.getRuleInfo().getRuleAction().getActionType()));
  }

  /**
   * Method to filter on rule-scope * If filterScope has no environment scope, always return true *
   * If filterScope has environment scope but the environment scope has no environment IDs, return
   * true only if the rule has no Environment IDs in its rule-scope. * If filterScope has
   * environment scope and the environment scope has one or more environment IDs, return true only
   * if there is at least one overlap of environment ID between the filter and the rule.
   */
  private boolean filterRuleOnScope(
      MaliciousSourcesRule ruleData, MaliciousSourcesRuleScope filterScope) {
    List<String> ruleEnvironmentIds =
        ruleData.getRuleScope().getEnvironmentScope().getEnvironmentIdsList();
    if (!filterScope.hasEnvironmentScope() || ruleEnvironmentIds.isEmpty()) {
      return true;
    }

    List<String> filterEnvironmentIds = filterScope.getEnvironmentScope().getEnvironmentIdsList();
    return ruleEnvironmentIds.stream().anyMatch(filterEnvironmentIds::contains);
  }

  public List<MaliciousSourcesRuleRecord> getRuleRecords(
      RequestContext context, GetRulesFilter filter) {
    boolean isDefaultFilter = GetRulesFilter.getDefaultInstance().equals(filter);
    return getAllObjects(context).stream()
        .filter(ruleWithContext -> isDefaultFilter || matchesFilter(ruleWithContext, filter))
        .map(this::toRuleRecord)
        .collect(Collectors.toUnmodifiableList());
  }

  private boolean matchesFilter(
      ContextualConfigObject<MaliciousSourcesRule> ruleWithContext, GetRulesFilter filter) {
    return filterConfigData(ruleWithContext.getData(), filter).isPresent()
        && matchesAuditFilter(ruleWithContext, filter.getAuditFilter());
  }

  private boolean matchesAuditFilter(
      ContextualConfigObject<MaliciousSourcesRule> contextual, AuditFilter auditFilter) {
    if (AuditFilter.getDefaultInstance().equals(auditFilter)) {
      return true;
    }
    return matchesCreatedRange(contextual, auditFilter)
        && matchesUpdatedRange(contextual, auditFilter)
        && matchesCreatedByContains(contextual, auditFilter)
        && matchesLastUpdatedByContains(contextual, auditFilter);
  }

  private boolean matchesCreatedRange(
      ContextualConfigObject<MaliciousSourcesRule> contextual, AuditFilter auditFilter) {
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
      ContextualConfigObject<MaliciousSourcesRule> contextual, AuditFilter auditFilter) {
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
      ContextualConfigObject<MaliciousSourcesRule> contextual, AuditFilter auditFilter) {
    String createdByContains = auditFilter.getCreatedByContains();
    if (createdByContains.isEmpty()) {
      return true;
    }
    String createdByEmail = contextual.getCreatedByEmail();
    return createdByEmail != null
        && createdByEmail.toLowerCase().contains(createdByContains.toLowerCase());
  }

  private boolean matchesLastUpdatedByContains(
      ContextualConfigObject<MaliciousSourcesRule> contextual, AuditFilter auditFilter) {
    String lastUpdatedByContains = auditFilter.getLastUpdatedByUserContains();
    if (lastUpdatedByContains.isEmpty()) {
      return true;
    }
    String lastUserUpdateEmail =
        getVisibleUserEmail(
            contextual.getLastUserUpdateEmail(),
            contextual.getLastUpdateEmail(),
            this.serviceConfig.getUserVisibleEmailConfig());
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

  private MaliciousSourcesRuleRecord toRuleRecord(
      ContextualConfigObject<MaliciousSourcesRule> contextual) {
    return MaliciousSourcesRuleRecord.newBuilder()
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
      String userEmail =
          getVisibleUserEmail(
              contextual.getLastUserUpdateEmail(),
              contextual.getLastUpdateEmail(),
              this.serviceConfig.getUserVisibleEmailConfig());
      builder.setLastUserUpdateDetails(
          LastUpdateDetails.newBuilder()
              .setUpdatedBy(userEmail)
              .setUpdatedAt(timestampConverter.convert(lastUserUpdateTimestamp))
              .build());
    }

    return builder.build();
  }
}
