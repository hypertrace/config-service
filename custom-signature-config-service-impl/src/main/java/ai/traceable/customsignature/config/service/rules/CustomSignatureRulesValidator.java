package ai.traceable.customsignature.config.service.rules;

import static ai.traceable.customsignature.config.service.v1.EventType.EVENT_TYPE_ALLOW;
import static ai.traceable.customsignature.config.service.v1.EventType.EVENT_TYPE_DETECTION_AND_BLOCKING;
import static ai.traceable.customsignature.config.service.v1.EventType.EVENT_TYPE_NORMAL_DETECTION;
import static ai.traceable.customsignature.config.service.v1.EventType.EVENT_TYPE_TESTING_DETECTION;
import static ai.traceable.customsignature.config.service.v1.MatchCategory.MATCH_CATEGORY_REQUEST;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;

import ai.traceable.customsignature.config.service.modsec.ModsecRulesManager;
import ai.traceable.customsignature.config.service.modsec.ModsecRulesSupportChecker;
import ai.traceable.customsignature.config.service.v1.AgentModification;
import ai.traceable.customsignature.config.service.v1.AgentRuleEffect;
import ai.traceable.customsignature.config.service.v1.BodyModification;
import ai.traceable.customsignature.config.service.v1.BulkDeleteCustomSignatureRulesRequest;
import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.ClauseGroup;
import ai.traceable.customsignature.config.service.v1.ClauseOperator;
import ai.traceable.customsignature.config.service.v1.CreateCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.DeleteCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.EventSeverity;
import ai.traceable.customsignature.config.service.v1.EventType;
import ai.traceable.customsignature.config.service.v1.ExpiryDetails;
import ai.traceable.customsignature.config.service.v1.FieldValue;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureEdgeDecisionRulesRequest;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesRequest;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureRulesRequest;
import ai.traceable.customsignature.config.service.v1.GetRulesFilter;
import ai.traceable.customsignature.config.service.v1.HeaderInjection;
import ai.traceable.customsignature.config.service.v1.MatchCategory;
import ai.traceable.customsignature.config.service.v1.RuleDefinition;
import ai.traceable.customsignature.config.service.v1.RuleEffect;
import ai.traceable.customsignature.config.service.v1.RuleEffectWithModifications;
import ai.traceable.customsignature.config.service.v1.RuleEvaluationPoint;
import ai.traceable.customsignature.config.service.v1.RuleScope;
import ai.traceable.customsignature.config.service.v1.RuleSource;
import ai.traceable.customsignature.config.service.v1.UpdateCustomSignatureRuleRequest;
import io.grpc.Status;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Set;

public class CustomSignatureRulesValidator implements RulesValidator {

  private static final Set<EventType> INVALID_RESPONSE_AND_ATTRIBUTE_EVENT_TYPES =
      Set.of(EventType.EVENT_TYPE_ALLOW, EVENT_TYPE_DETECTION_AND_BLOCKING);
  private static final Integer CUSTOM_LABELS_LIMIT = 5;

  private final ModsecRulesManager modsecRulesManager;
  private final ClauseGroupValidator clauseGroupValidator;

  @Inject
  public CustomSignatureRulesValidator(
      ModsecRulesManager modsecRulesManager, ClauseGroupValidator clauseGroupValidator) {
    this.modsecRulesManager = modsecRulesManager;
    this.clauseGroupValidator = clauseGroupValidator;
  }

  @Override
  public Status validate(CreateCustomSignatureRuleRequest request) {
    if (request.getName().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Create custom signature rule should have a valid name.");
    }

    if (!request.hasDefinition()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Create custom signature rule should have a valid definition.");
    }

    if (!request.hasEffect()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Create custom signature rule should have a valid effect.");
    }

    ClauseGroup clauseGroup = request.getDefinition().getClauseGroup();

    Status status =
        validateRuleEffect(
            request.getEffect(), hasResponseOrAttribute(clauseGroup.getClausesList()), clauseGroup);
    if (status != Status.OK) {
      return status;
    }

    status = validateRuleDefinition(request.getDefinition(), request.getEffect().getEventType());
    if (status != Status.OK) {
      return status;
    }

    status = validateExpiry(request.getEffect(), request.getBlockingExpiryDetails());
    if (status != Status.OK) {
      return status;
    }

    status = validateRuleScope(request.getRuleScope());
    if (status != Status.OK) {
      return status;
    }

    if (ModsecRulesSupportChecker.isInlineRuleMappingSupported(clauseGroup)
        && modsecRulesManager.containsModsecConvertibleClauses(clauseGroup)) {
      return modsecRulesManager.validateModsecRule(request.getName(), request.getDefinition());
    }

    return Status.OK;
  }

  @Override
  public Status validate(UpdateCustomSignatureRuleRequest request) {
    CustomSignatureRule rule = request.getRule();

    if (rule.getId().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Update custom signature rule should have a valid id.");
    }

    if (rule.getName().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Update custom signature rule should have a valid name.");
    }

    if (!rule.getRuleSource().equals(RuleSource.RULE_SOURCE_UNSPECIFIED)) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Update request does not allow to update rule source for rule with id: %s",
                  rule.getId()))
          .asRuntimeException();
    }

    if (!rule.hasDefinition()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Update custom signature rule should have a valid definition.");
    }

    if (!rule.hasEffect()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Update custom signature rule should have a valid effect.");
    }

    ClauseGroup clauseGroup = rule.getDefinitionOrBuilder().getClauseGroup();
    List<Clause> clauses = clauseGroup.getClausesList();
    boolean hasResponseOrAttribute = hasResponseOrAttribute(clauses);

    Status status = validateRuleEffect(rule.getEffect(), hasResponseOrAttribute, clauseGroup);
    if (status != Status.OK) {
      return status;
    }

    status = validateRuleDefinition(rule.getDefinition(), rule.getEffect().getEventType());
    if (status != Status.OK) {
      return status;
    }

    status = validateExpiry(rule.getEffect(), rule.getBlockingExpiryDetails());
    if (status != Status.OK) {
      return status;
    }

    status = validateRuleScope(rule.getRuleScope());
    if (status != Status.OK) {
      return status;
    }

    if (ModsecRulesSupportChecker.isInlineRuleMappingSupported(clauseGroup)
        && modsecRulesManager.containsModsecConvertibleClauses(clauseGroup)) {
      return modsecRulesManager.validateModsecRule(rule.getName(), rule.getDefinition());
    }

    return Status.OK;
  }

  @Override
  public Status validate(DeleteCustomSignatureRuleRequest request) {
    String ruleId = request.getId();
    if (ruleId.isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Delete custom signature rule should have a valid id");
    }

    return Status.OK;
  }

  @Override
  public Status validate(BulkDeleteCustomSignatureRulesRequest request) {
    if (request.getIdsCount() == 0) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Bulk delete custom signature rules should have at least one id");
    }
    for (String id : request.getIdsList()) {
      if (id.isEmpty()) {
        return Status.INVALID_ARGUMENT.withDescription(
            "Bulk delete custom signature rules should not contain empty ids");
      }
    }
    return Status.OK;
  }

  @Override
  public Status validate(GetCustomSignatureEdgeDecisionRulesRequest request) {
    return validateFilter(request.getRulesFilter());
  }

  @Override
  public Status validate(GetCustomSignatureRulesRequest request) {
    return validateFilter(request.getFilter());
  }

  @Override
  public Status validate(GetCustomSignatureModsecRulesRequest request) {
    return validateFilter(request.getFilter());
  }

  public static boolean isRuleOfEventTypeAlertAndContainsHeaderInjection(RuleEffect ruleEffect) {
    EventType ruleEventType = ruleEffect.getEventType();
    return (ruleEventType == EventType.EVENT_TYPE_NORMAL_DETECTION
            || ruleEventType == EVENT_TYPE_TESTING_DETECTION)
        && ruleEffect.getEffectsList().stream()
            .filter(RuleEffectWithModifications::hasAgentRuleEffect)
            .map(RuleEffectWithModifications::getAgentRuleEffect)
            .flatMap(agentRuleEffect -> agentRuleEffect.getAgentModificationsList().stream())
            .anyMatch(AgentModification::hasHeaderInjection);
  }

  private Status validateFilter(GetRulesFilter rulesFilter) {
    if (rulesFilter.hasFilterEdgeDecisionRules() && !rulesFilter.getFilterEdgeDecisionRules()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Use get edge decision rules api and not call this api with filterEdgeDecisionRules set to false");
    }
    return Status.OK;
  }

  private Status validateRuleEffect(
      RuleEffect ruleEffect,
      boolean hasMatchCategoryResponseOrAttributeKeyValueExpression,
      ClauseGroup clauseGroup) {
    EventType eventType = ruleEffect.getEventType();
    if (eventType == EventType.EVENT_TYPE_UNSPECIFIED) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Custom Signature Rule Effect should have a valid event type.");
    }
    if (eventType == EVENT_TYPE_NORMAL_DETECTION
        && ruleEffect.getEventSeverity() == EventSeverity.EVENT_SEVERITY_UNSPECIFIED) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Custom Signature Rule Effect with alert action should have a valid event severity.");
    }
    if (eventType == EVENT_TYPE_DETECTION_AND_BLOCKING
        && ruleEffect.getEffectsList().stream()
            .flatMap(effect -> effect.getAgentRuleEffect().getAgentModificationsList().stream())
            .anyMatch(AgentModification::hasHeaderInjection)) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Custom Signature Rule Effect with block action should not have header injection.");
    }

    if (hasMatchCategoryResponseOrAttributeKeyValueExpression
        && INVALID_RESPONSE_AND_ATTRIBUTE_EVENT_TYPES.contains(ruleEffect.getEventType())) {
      return Status.INVALID_ARGUMENT.withDescription(
          String.format(
              "Custom signature rule with a response category or a attribute clause is not compatible with the specified event type %s.",
              ruleEffect.getEventType()));
    }

    Status status = validateForRuleEvaluationPoints(ruleEffect, clauseGroup);
    if (status != Status.OK) {
      return status;
    }

    ruleEffect.getEffectsList().forEach(this::validateRuleEffectWithModification);

    return Status.OK;
  }

  private Status validateForRuleEvaluationPoints(RuleEffect ruleEffect, ClauseGroup clauseGroup) {
    List<RuleEvaluationPoint> ruleEvaluationPoints = ruleEffect.getRuleEvaluationPointsList();

    if (ruleEvaluationPoints.isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription("RuleEvaluationPoints cannot be empty.");
    }

    if (ruleEvaluationPoints.contains(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)) {
      if (!CustomSignatureRulesEdgeDecisionFilter.isConvertibleRule(ruleEffect, clauseGroup)) {
        return Status.INVALID_ARGUMENT.withDescription("Rule is not EDGE-compatible.");
      }
    }

    if (ruleEvaluationPoints.contains(
        RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT)) {
      /*
       * ModSec rules only support AND operators for clause groups.
       * OR operators are unsupported as they are computationally expensive in ModSec's rule evaluation engine.
       */
      if (clauseGroup.getClauseOperator() == ClauseOperator.CLAUSE_OPERATOR_OR) {
        return Status.INVALID_ARGUMENT.withDescription(
            "Rules evaluated at INLINE_TRACING_AGENT cannot have OR clause operator.");
      }

      if (clauseGroup.getClausesList().stream().anyMatch(Clause::hasClauseGroup)) {
        return Status.INVALID_ARGUMENT.withDescription(
            "Rules evaluated at INLINE_TRACING_AGENT cannot have nested clauses.");
      }

      if (!ModsecRulesSupportChecker.isInlineRuleMappingSupported(clauseGroup)) {
        return Status.INVALID_ARGUMENT.withDescription("Rule is not AGENT-compatible.");
      }

      EventType eventType = ruleEffect.getEventType();
      boolean isCompatibleEventType =
          eventType == EVENT_TYPE_ALLOW
              || eventType == EVENT_TYPE_DETECTION_AND_BLOCKING
              || isRuleOfEventTypeAlertAndContainsHeaderInjection(ruleEffect);
      if (!isCompatibleEventType) {
        return Status.INVALID_ARGUMENT.withDescription("Rule is not AGENT-compatible");
      }
    }

    return Status.OK;
  }

  private void validateRuleEffectWithModification(RuleEffectWithModifications effect) {
    if (!effect.hasAgentRuleEffect()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Modification rule effect should have at least one modification.")
          .asRuntimeException();
    }
    validateNonDefaultPresenceOrThrow(
        effect.getAgentRuleEffect(), RuleEffectWithModifications.AGENT_RULE_EFFECT_FIELD_NUMBER);
    AgentRuleEffect agentRuleEffect = effect.getAgentRuleEffect();
    if (agentRuleEffect.getAgentModificationsList().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Agent rule effect should have at least one modification.")
          .asRuntimeException();
    }

    agentRuleEffect
        .getAgentModificationsList()
        .forEach(
            agentModification -> {
              // Check which oneof field is set and validate accordingly
              if (agentModification.hasHeaderInjection()) {
                HeaderInjection headerInjection = agentModification.getHeaderInjection();
                validateNonDefaultPresenceOrThrow(
                    headerInjection, HeaderInjection.HEADER_CATEGORY_FIELD_NUMBER);
                validateNonDefaultPresenceOrThrow(
                    headerInjection, HeaderInjection.HEADER_NAME_FIELD_NUMBER);
                validateNonDefaultPresenceOrThrow(
                    headerInjection.getValue(), FieldValue.STATIC_VALUE_FIELD_NUMBER);
              } else if (agentModification.hasStatusCodeModification()) {
                // Do nothing
              } else if (agentModification.hasBodyModification()) {
                BodyModification bodyModification = agentModification.getBodyModification();
                if (agentModification
                    .getBodyModification()
                    .getLocationCategory()
                    .equals(MATCH_CATEGORY_REQUEST)) {
                  throw Status.INVALID_ARGUMENT
                      .withDescription("Request body modification not supported yet.")
                      .asRuntimeException();
                }
                validateNonDefaultPresenceOrThrow(
                    bodyModification.getBodyValue(), FieldValue.STATIC_VALUE_FIELD_NUMBER);
              } else {
                throw Status.INVALID_ARGUMENT
                    .withDescription("Agent modification must specify a valid modification type.")
                    .asRuntimeException();
              }
            });
  }

  private Status validateRuleDefinition(RuleDefinition ruleDefinition, EventType eventType) {
    if (ruleDefinition.getLabelsMap().size() > CustomSignatureRulesValidator.CUSTOM_LABELS_LIMIT) {
      return Status.INVALID_ARGUMENT.withDescription("Custom labels limit exceeded");
    }

    if (!ruleDefinition.hasClauseGroup()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Create custom signature rule definition should have a valid clause group.");
    }

    return clauseGroupValidator.validateClauseGroup(ruleDefinition.getClauseGroup(), eventType);
  }

  private Status validateExpiry(RuleEffect ruleEffect, ExpiryDetails expiry) {
    if (expiry.hasExpiryDuration()) {
      if (!(ruleEffect.getEventType().equals(EVENT_TYPE_DETECTION_AND_BLOCKING)
          || ruleEffect.getEventType().equals(EVENT_TYPE_ALLOW))) {
        return Status.INVALID_ARGUMENT.withDescription(
            "Expiry duration can be specified only for blocking or allow event type");
      }
      try {
        Duration.parse(expiry.getExpiryDuration());
      } catch (DateTimeParseException e) {
        return Status.INVALID_ARGUMENT.withDescription("Blocking expiry duration can't be parsed");
      }
    }
    return Status.OK;
  }

  private boolean hasResponseOrAttribute(List<Clause> clauses) {
    return clauses.stream()
        .anyMatch(
            clause ->
                clause
                        .getMatchExpression()
                        .getMatchCategory()
                        .equals(MatchCategory.MATCH_CATEGORY_RESPONSE)
                    || clause
                        .getKeyValueExpression()
                        .getMatchCategory()
                        .equals(MatchCategory.MATCH_CATEGORY_RESPONSE)
                    || clause.hasAttributeKeyValueExpression());
  }

  private Status validateRuleScope(RuleScope scope) {
    if (scope.hasEnvironmentScope()) {
      List<String> environmentIdList = scope.getEnvironmentScope().getEnvironmentIdsList();
      if (environmentIdList.isEmpty()) {
        return Status.INVALID_ARGUMENT.withDescription(
            "Environment scope should have at least one environment");
      } else {
        return environmentIdList.stream().anyMatch(String::isEmpty)
            ? Status.INVALID_ARGUMENT.withDescription("Environment id should not be empty string.")
            : Status.OK;
      }
    } else {
      return Status.OK;
    }
  }
}
