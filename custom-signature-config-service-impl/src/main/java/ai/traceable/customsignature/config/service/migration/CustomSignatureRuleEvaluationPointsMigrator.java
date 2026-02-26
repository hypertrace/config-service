package ai.traceable.customsignature.config.service.migration;

import ai.traceable.customsignature.config.service.modsec.ModsecRulesSupportChecker;
import ai.traceable.customsignature.config.service.rules.CustomSignatureRulesEdgeDecisionFilter;
import ai.traceable.customsignature.config.service.rules.CustomSignatureRulesManager;
import ai.traceable.customsignature.config.service.rules.CustomSignatureRulesValidator;
import ai.traceable.customsignature.config.service.v1.ClauseGroup;
import ai.traceable.customsignature.config.service.v1.CreateCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.EventType;
import ai.traceable.customsignature.config.service.v1.GetRulesFilter;
import ai.traceable.customsignature.config.service.v1.RuleEffect;
import ai.traceable.customsignature.config.service.v1.RuleEvaluationPoint;
import ai.traceable.customsignature.config.service.v1.UpdateCustomSignatureRuleRequest;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class CustomSignatureRuleEvaluationPointsMigrator {
  private final CustomSignatureRulesManager customSignatureRulesManager;

  @Inject
  public CustomSignatureRuleEvaluationPointsMigrator(
      CustomSignatureRulesManager customSignatureRulesManager) {
    this.customSignatureRulesManager = customSignatureRulesManager;
  }

  public CreateCustomSignatureRuleRequest migrateCreateCustomSignatureRuleRequest(
      CreateCustomSignatureRuleRequest createCustomSignatureRuleRequest) {
    return migrateRequest(
        createCustomSignatureRuleRequest,
        request -> request.hasEffect() ? request.getEffect() : null,
        request -> request.hasDefinition() ? request.getDefinition().getClauseGroup() : null,
        (request, effect) -> request.toBuilder().setEffect(effect).build());
  }

  public UpdateCustomSignatureRuleRequest migrateUpdateCustomSignatureRuleRequest(
      UpdateCustomSignatureRuleRequest updateCustomSignatureRuleRequest) {
    return migrateRequest(
        updateCustomSignatureRuleRequest,
        request ->
            request.hasRule() && request.getRule().hasEffect()
                ? request.getRule().getEffect()
                : null,
        request ->
            request.hasRule() && request.getRule().hasDefinition()
                ? request.getRule().getDefinition().getClauseGroup()
                : null,
        (request, effect) ->
            request.toBuilder()
                .setRule(request.getRule().toBuilder().setEffect(effect).build())
                .build());
  }

  public void migrateRules(RequestContext requestContext) {
    requestContext = requestContext.withUserTrackingSuppressed();
    customSignatureRulesManager
        .getCustomSignatureRules(requestContext, GetRulesFilter.getDefaultInstance())
        .forEach(this::migrateRule);
  }

  private <T> T migrateRequest(
      T request,
      Function<T, RuleEffect> effectExtractor,
      Function<T, ClauseGroup> clauseGroupExtractor,
      BiFunction<T, RuleEffect, T> requestUpdater) {

    RuleEffect effect = effectExtractor.apply(request);
    ClauseGroup clauseGroup = clauseGroupExtractor.apply(request);

    if (effect == null || !effect.getRuleEvaluationPointsList().isEmpty() || clauseGroup == null) {
      return request;
    }

    List<RuleEvaluationPoint> evaluationPoints = getRuleEvaluationPoints(effect, clauseGroup);
    RuleEffect updatedEffect =
        effect.toBuilder().addAllRuleEvaluationPoints(evaluationPoints).build();

    return requestUpdater.apply(request, updatedEffect);
  }

  private void migrateRule(CustomSignatureRule rule) {
    if (Objects.isNull(rule)) {
      return;
    }

    if (!rule.hasEffect() || rule.getEffect().getRuleEvaluationPointsList().isEmpty()) {
      List<RuleEvaluationPoint> ruleEvaluationPoints =
          getRuleEvaluationPoints(rule.getEffect(), rule.getDefinition().getClauseGroup());
      CustomSignatureRule.Builder ruleBuilder = rule.toBuilder();

      if (rule.hasEffect()) {
        ruleBuilder.setEffect(
            rule.getEffect().toBuilder().addAllRuleEvaluationPoints(ruleEvaluationPoints).build());
      } else {
        ruleBuilder.setEffect(
            RuleEffect.newBuilder().addAllRuleEvaluationPoints(ruleEvaluationPoints).build());
      }

      CustomSignatureRule migratedRule = ruleBuilder.build();
      customSignatureRulesManager.updateCustomSignatureRule(
          RequestContext.CURRENT.get().withUserTrackingSuppressed(), migratedRule);
    }
  }

  private List<RuleEvaluationPoint> getRuleEvaluationPoints(
      RuleEffect ruleEffect, ClauseGroup clauseGroup) {
    List<RuleEvaluationPoint> ruleEvaluationPoints = new ArrayList<>();

    // default case
    ruleEvaluationPoints.add(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM);

    // check for edge
    if (CustomSignatureRulesEdgeDecisionFilter.isConvertibleRule(ruleEffect, clauseGroup)) {
      ruleEvaluationPoints.add(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE);
    }

    // check for agent
    if (ModsecRulesSupportChecker.isInlineRuleMappingSupported(clauseGroup)) {
      EventType ruleEventType = ruleEffect.getEventType();
      if (ruleEventType == EventType.EVENT_TYPE_ALLOW
          || ruleEventType == EventType.EVENT_TYPE_DETECTION_AND_BLOCKING
          || CustomSignatureRulesValidator.isRuleOfEventTypeAlertAndContainsHeaderInjection(
              ruleEffect)) {
        ruleEvaluationPoints.add(RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT);
      }
    }

    return ruleEvaluationPoints;
  }
}
