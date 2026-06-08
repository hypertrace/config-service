package ai.traceable.edge.config.service.supplier.filtering.config.context;

import static ai.traceable.protection.rules.filtering.v1.ProtectionFilteringRuleCategory.PROTECTION_FILTERING_RULE_CATEGORY_PRE_DETECTION;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.edge.config.service.TraceableEdgeConfigSupplier;
import ai.traceable.edge.config.service.config.TraceableEdgeConfig;
import ai.traceable.edge.config.service.v1.AgentCapabilities;
import ai.traceable.edge.config.service.v1.ConfigPayloads;
import ai.traceable.edge.config.service.v1.ConfigRequestElement;
import ai.traceable.edge.config.service.v1.ConfigResponseElement;
import ai.traceable.protection.engine.config.filtering.v1.PreDetectionFilteringConfigContext;
import ai.traceable.protection.engine.config.filtering.v1.PreDetectionFilteringRuleConfig;
import ai.traceable.protection.rules.filtering.v1.ProtectionFilteringRule;
import ai.traceable.protection.rules.filtering.v1.ProtectionFilteringRuleEvaluation;
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
public class PreDetectionFilteringConfigContextSupplier implements TraceableEdgeConfigSupplier {
  private static final String CONFIG_TYPE =
      PreDetectionFilteringConfigContext.class.getSimpleName();
  private static final ProtectionFilteringRulesFilter CATEGORY_FILTER =
      ProtectionFilteringRulesFilter.newBuilder()
          .setRuleCategory(PROTECTION_FILTERING_RULE_CATEGORY_PRE_DETECTION)
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
        "Received request for PreDetectionFilteringConfigContext for tenantId: {}",
        requestContext.getTenantId());
    PreDetectionFilteringConfigContext context;
    if (!featureCachingClient.isProtectionEnginePreDetectionFilteringEnabledForTenant(
        requestContext)) {
      log.debug(
          "Pre detection filtering config not enabled for tenant: {}",
          requestContext.getTenantId());
      context = PreDetectionFilteringConfigContext.getDefaultInstance();
    } else {
      List<ProtectionFilteringRulesFilter> filters = this.buildApplicableFilters(agentCapabilities);
      List<ProtectionFilteringRule> rules = rulesProvider.getProtectionFilteringRules(filters);
      context = this.buildContext(rules);
    }

    ConfigPayloads payloads =
        ConfigPayloads.newBuilder().addConfigBytes(context.toByteString()).build();
    log.debug(
        "Returning PreDetectionFilteringConfigContext for tenantId: {}",
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

  private ImmutableList<ProtectionFilteringRulesFilter> buildApplicableFilters(
      AgentCapabilities agentCapabilities) {
    List<ProtectionFilteringRulesFilter> filters =
        ProtectionFilteringRulesFilterUtils.buildFilters(agentCapabilities);
    return ImmutableList.<ProtectionFilteringRulesFilter>builder()
        .addAll(filters)
        .add(CATEGORY_FILTER)
        .build();
  }

  private PreDetectionFilteringConfigContext buildContext(List<ProtectionFilteringRule> rules) {
    PreDetectionFilteringConfigContext.Builder builder =
        PreDetectionFilteringConfigContext.newBuilder();
    rules.stream().map(this::convertToRuleConfig).forEach(builder::addRuleConfigs);
    return builder.build();
  }

  private PreDetectionFilteringRuleConfig convertToRuleConfig(ProtectionFilteringRule rule) {
    PreDetectionFilteringRuleConfig.Builder builder = PreDetectionFilteringRuleConfig.newBuilder();
    builder.setRuleId(rule.getRuleId());
    this.setEvaluationAtBuilder(rule.getRuleEvaluation(), builder);
    return builder.build();
  }

  private void setEvaluationAtBuilder(
      ProtectionFilteringRuleEvaluation evaluation,
      PreDetectionFilteringRuleConfig.Builder builder) {
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
}
