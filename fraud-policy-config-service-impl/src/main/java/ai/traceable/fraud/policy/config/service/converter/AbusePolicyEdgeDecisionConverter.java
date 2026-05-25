package ai.traceable.fraud.policy.config.service.converter;

import static ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleCategory.EDGE_DECISION_RULE_CATEGORY_ABUSE_DETECTION;
import static ai.traceable.edge.decision.config.service.v1.EdgeDecisionType.EDGE_DECISION_TYPE_BLOCK;

import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.edge.decision.config.service.v1.EdgeDecision;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRule;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleScope;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleScopeCondition;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleStatus;
import ai.traceable.edge.decision.config.service.v1.EnvironmentScope;
import ai.traceable.edge.decision.config.service.v1.PolicyKind;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfig;
import ai.traceable.fraud.policy.config.service.v1.AbuseActionType;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicy;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyData;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

/**
 * Converts AbusePolicy objects (BLOCK action) into EdgeDecisionEngineConfig for the Edge Decision
 * System.
 *
 * <p>Delegates template-specific conversion to {@link TemplateEdgeDecisionConverter}
 * implementations, entity-to-JEXL resolution to {@link EntityJexlResolver}, and scope resolution to
 * {@link EntityScopeResolver}.
 */
@Slf4j
@Singleton
public class AbusePolicyEdgeDecisionConverter {

  public static final String ABUSE_POLICY_ID_PREFIX = "abuse-";
  private final EntityJexlResolver entityJexlResolver;
  private final EntityScopeResolver entityScopeResolver;
  private final Map<AbusePolicyData.TemplateConfigCase, TemplateEdgeDecisionConverter>
      templateConverters;

  @Inject
  public AbusePolicyEdgeDecisionConverter(
      EntityJexlResolver entityJexlResolver,
      EntityScopeResolver entityScopeResolver,
      Map<AbusePolicyData.TemplateConfigCase, TemplateEdgeDecisionConverter> templateConverters) {
    this.entityJexlResolver = entityJexlResolver;
    this.entityScopeResolver = entityScopeResolver;
    this.templateConverters = templateConverters;
  }

  public EdgeDecisionEngineConfig convert(
      RequestContext requestContext, List<AbusePolicy> abusePolicies) {
    // Filter eligible policies once (enabled + BLOCK action + known template)
    List<AbusePolicy> eligiblePolicies =
        abusePolicies.stream()
            .filter(p -> p.getData().getEnabled())
            .filter(
                p ->
                    p.getData().getAction().getActionType()
                        == AbuseActionType.ABUSE_ACTION_TYPE_BLOCK)
            .filter(p -> templateConverters.containsKey(p.getData().getTemplateConfigCase()))
            .collect(Collectors.toList());

    // Collect all derived entity IDs needed across eligible policies
    Set<String> allDerivedEntityIds =
        eligiblePolicies.stream()
            .flatMap(
                p -> {
                  TemplateEdgeDecisionConverter c =
                      templateConverters.get(p.getData().getTemplateConfigCase());
                  return c.getRequiredDerivedEntityIds(p.getData()).stream();
                })
            .collect(Collectors.toSet());

    // Batch-fetch all entity derivation configs
    Map<String, EntityDerivationConfig> entityConfigMap =
        entityJexlResolver.fetchConfigs(requestContext, allDerivedEntityIds);

    // Resolve all entities to DerivationRules once (not per-policy)
    Map<String, List<DerivationRule>> entityRulesMap =
        entityJexlResolver.resolveAll(requestContext, entityConfigMap);
    Map<String, String> entityVariableNames =
        entityJexlResolver.resolveVariableNames(entityConfigMap);

    EdgeDecisionEngineConfig.Builder builder = EdgeDecisionEngineConfig.newBuilder();
    for (AbusePolicy policy : eligiblePolicies) {
      TemplateEdgeDecisionConverter converter =
          templateConverters.get(policy.getData().getTemplateConfigCase());
      convertAbusePolicy(requestContext, policy, entityRulesMap, entityVariableNames, converter)
          .ifPresent(builder::addDecisionRules);
    }
    return builder.build();
  }

  private Optional<EdgeDecisionRule> convertAbusePolicy(
      RequestContext requestContext,
      AbusePolicy abusePolicy,
      Map<String, List<DerivationRule>> entityRulesMap,
      Map<String, String> entityVariableNames,
      TemplateEdgeDecisionConverter converter) {
    try {
      AbusePolicyData data = abusePolicy.getData();

      EdgeDecision edgeDecision =
          EdgeDecision.newBuilder()
              .setEdgeDecisionType(EDGE_DECISION_TYPE_BLOCK)
              .setThreatType(data.getName())
              .addAllSpanAttributes(converter.buildSpanAttributes(data, entityVariableNames))
              .build();

      EdgeDecisionRule.Builder ruleBuilder =
          EdgeDecisionRule.newBuilder()
              .setId(buildPolicyId(abusePolicy.getId()))
              .setName(data.getName())
              .setVersion(data.getVersion())
              .setRuleCategory(EDGE_DECISION_RULE_CATEGORY_ABUSE_DETECTION)
              .setRuleStatus(
                  EdgeDecisionRuleStatus.newBuilder().setDisabled(!data.getEnabled()).build())
              .setRuleDefinition(
                  converter.buildRuleDefinition(data, entityRulesMap, entityVariableNames))
              .setRuleDecision(edgeDecision)
              .setPolicyKind(PolicyKind.POLICY_KIND_BOT_MITIGATION)
              .setPolicyId(buildPolicyId(abusePolicy.getId()));

      if (!data.getDescription().isEmpty()) {
        ruleBuilder.setDescription(data.getDescription());
      }

      buildRuleScope(requestContext, data).ifPresent(ruleBuilder::setRuleScope);

      return Optional.of(ruleBuilder.build());
    } catch (Exception ex) {
      log.warn("Unable to convert abuse policy to edge decision rule: {}", abusePolicy.getId(), ex);
      return Optional.empty();
    }
  }

  private Optional<EdgeDecisionRuleScope> buildRuleScope(
      RequestContext requestContext, AbusePolicyData data) {
    if (!data.hasScope()) {
      return Optional.empty();
    }

    EdgeDecisionRuleScope.Builder scopeBuilder = EdgeDecisionRuleScope.newBuilder();

    if (data.getScope().hasEnvironmentScope()) {
      List<String> envIds =
          data.getScope().getEnvironmentScope().getEnvironmentIdsList().stream()
              .filter(env -> !env.isEmpty())
              .collect(Collectors.toList());
      if (!envIds.isEmpty()) {
        scopeBuilder.addScopeConditions(
            EdgeDecisionRuleScopeCondition.newBuilder()
                .setEnvironmentScope(EnvironmentScope.newBuilder().addAllEnvironments(envIds)));
      }
    }

    // Resolve api_scope to url_regex_scope, http_method_scope, service_scope
    if (data.getScope().hasApiScope()) {
      entityScopeResolver
          .resolveApiScope(requestContext, data.getScope().getApiScope())
          .forEach(scopeBuilder::addScopeConditions);
    }

    return scopeBuilder.getScopeConditionsList().isEmpty()
        ? Optional.empty()
        : Optional.of(scopeBuilder.build());
  }

  private String buildPolicyId(String abusePolicyId) {
    return ABUSE_POLICY_ID_PREFIX + abusePolicyId;
  }
}
