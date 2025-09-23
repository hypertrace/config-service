package ai.traceable.edge.config.service.supplier.filtering.config.context;

import static ai.traceable.protection.rules.filtering.v1.ProtectionFilteringRuleCategory.PROTECTION_FILTERING_RULE_CATEGORY_POST_DETECTION;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.edge.config.service.TraceableEdgeConfigSupplier;
import ai.traceable.edge.config.service.config.TraceableEdgeConfig;
import ai.traceable.edge.config.service.v1.AgentCapabilities;
import ai.traceable.edge.config.service.v1.ConfigPayloads;
import ai.traceable.edge.config.service.v1.ConfigRequestElement;
import ai.traceable.edge.config.service.v1.ConfigResponseElement;
import ai.traceable.protection.engine.config.filtering.v1.PostDetectionFilteringConfigContext;
import ai.traceable.protection.engine.config.filtering.v1.PostDetectionFilteringRuleConfig;
import ai.traceable.protection.engine.config.filtering.v1.PostDetectionFilteringTarget;
import ai.traceable.protection.processing.common.v1.StringList;
import ai.traceable.protection.rules.filtering.v1.ProtectionFilteringRule;
import ai.traceable.protection.rules.filtering.v1.ProtectionFilteringRuleEvaluation;
import ai.traceable.protection.rules.filtering.v1.ProtectionFilteringRuleTarget;
import ai.traceable.protection.rules.filtering.v1.ProtectionFilteringRulesFilter;
import ai.traceable.protection.rules.filtering.v1.ProtectionFilteringRulesProvider;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.grpc.Status;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = @Inject)
public class PostDetectionFilteringConfigContextSupplier implements TraceableEdgeConfigSupplier {
  private static final String CONFIG_TYPE =
      PostDetectionFilteringConfigContext.class.getSimpleName();
  private static final ProtectionFilteringRulesFilter CATEGORY_FILTER =
      ProtectionFilteringRulesFilter.newBuilder()
          .setRuleCategory(PROTECTION_FILTERING_RULE_CATEGORY_POST_DETECTION)
          .build();

  private final ProtectionFilteringRulesProvider rulesProvider;
  private final UuidGenerator uuidGenerator;
  private final TraceableEdgeConfig config;
  private final FeatureCachingClient featureCachingClient;

  @Override
  public String getConfigType() {
    return CONFIG_TYPE;
  }

  @Override
  public ConfigResponseElement getConfigs(
      RequestContext requestContext,
      String environment,
      ConfigRequestElement requestElement,
      AgentCapabilities agentCapabilities) {
    log.debug(
        "Received request for PostDetectionFilteringConfigContext for tenantId: {}",
        requestContext.getTenantId());
    PostDetectionFilteringConfigContext context;
    if (!featureCachingClient.isProtectionEnginePostDetectionFilteringEnabledForTenant(
        requestContext)) {
      log.debug(
          "Protection engine post detection filtering not enabled for tenant: {}",
          requestContext.getTenantId());
      context = PostDetectionFilteringConfigContext.getDefaultInstance();
    } else {
      List<ProtectionFilteringRulesFilter> filters = this.buildApplicableFilters(agentCapabilities);
      List<ProtectionFilteringRule> rules = rulesProvider.getProtectionFilteringRules(filters);
      context = this.buildContext(rules);
    }
    ConfigPayloads payloads =
        ConfigPayloads.newBuilder().addConfigBytes(context.toByteString()).build();
    log.debug(
        "Returning PostDetectionFilteringConfigContext for tenantId: {}",
        requestContext.getTenantId());
    return ConfigResponseElement.newBuilder()
        .setHash(uuidGenerator.generateId(payloads))
        .setConfigType(CONFIG_TYPE)
        .setConfigPayloads(payloads)
        .setRefreshAfterDuration(config.getAgentPollingFrequency(getConfigType()))
        .setEnabled(true)
        .addSupportedAgentCapabilities(agentCapabilities)
        .build();
  }

  private List<ProtectionFilteringRulesFilter> buildApplicableFilters(
      AgentCapabilities agentCapabilities) {
    List<ProtectionFilteringRulesFilter> filters =
        ProtectionFilteringRulesFilterUtils.buildFilters(agentCapabilities);
    return ImmutableList.<ProtectionFilteringRulesFilter>builder()
        .addAll(filters)
        .add(CATEGORY_FILTER)
        .build();
  }

  private PostDetectionFilteringConfigContext buildContext(List<ProtectionFilteringRule> rules) {
    PostDetectionFilteringConfigContext.Builder builder =
        PostDetectionFilteringConfigContext.newBuilder();
    rules.stream().map(this::convertToRuleConfig).forEach(builder::addRuleConfigs);
    return builder.build();
  }

  private PostDetectionFilteringRuleConfig convertToRuleConfig(ProtectionFilteringRule rule) {
    PostDetectionFilteringRuleConfig.Builder builder =
        PostDetectionFilteringRuleConfig.newBuilder();

    builder.setRuleId(rule.getRuleId());
    this.setTargetsAtBuilder(rule, builder);
    this.setEvaluationAtBuilder(rule.getRuleEvaluation(), builder);
    return builder.build();
  }

  private void setTargetsAtBuilder(
      ProtectionFilteringRule rule, PostDetectionFilteringRuleConfig.Builder builder) {
    rule.getRuleDefinition()
        .getPostDetectionFilteringRuleDefinition()
        .getRuleTargetsList()
        .forEach(target -> builder.addTargets(convertTarget(target)));
  }

  private void setEvaluationAtBuilder(
      ProtectionFilteringRuleEvaluation evaluation,
      PostDetectionFilteringRuleConfig.Builder builder) {
    switch (evaluation.getEvaluationCase()) {
      case BLACK_BOX_IDENTIFIER:
        builder.setBlackBoxEvaluationIdentifier(evaluation.getBlackBoxIdentifier());
        break;
      case EVALUATION_NOT_SET:
      default:
        throw Status.INTERNAL
            .withDescription(String.format("Invalid evaluation %s", evaluation))
            .asRuntimeException();
    }
  }

  private static PostDetectionFilteringTarget convertTarget(ProtectionFilteringRuleTarget target) {
    PostDetectionFilteringTarget.Builder builder = PostDetectionFilteringTarget.newBuilder();
    switch (target.getTargetCase()) {
      case THREAT_RULE_IDS:
        return builder
            .setThreatRuleIds(
                StringList.newBuilder().addAllValues(target.getThreatRuleIds().getValuesList()))
            .build();
      case THREAT_TYPE_IDS:
        return builder
            .setThreatTypeIds(
                StringList.newBuilder().addAllValues(target.getThreatTypeIds().getValuesList()))
            .build();
      case MATCHED_ATTRIBUTE_VALUE_LEARNT_TYPES:
        return builder
            .setMatchedAttributeValueLearntTypes(
                StringList.newBuilder()
                    .addAllValues(target.getMatchedAttributeValueLearntTypes().getValuesList()))
            .build();
      case MATCHED_ATTRIBUTE_VALUE_OBSERVED_TYPES:
        return builder
            .setMatchedAttributeValueObservedTypes(
                StringList.newBuilder()
                    .addAllValues(target.getMatchedAttributeValueObservedTypes().getValuesList()))
            .build();
      case TARGET_NOT_SET:
      default:
        throw Status.INTERNAL
            .withDescription(String.format("Invalid rule target: %s", target))
            .asRuntimeException();
    }
  }
}
