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
import ai.traceable.customsignature.config.service.v1.BulkUpdateCustomSignatureRulesRequest;
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
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureEvaluationConfigContextRequest;
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
import org.hypertrace.core.grpcutils.context.ContextualStatusExceptionBuilder;

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
  public void validate(GetCustomSignatureEvaluationConfigContextRequest request) {
    if (request.getRuleEvaluationPoint() == RuleEvaluationPoint.RULE_EVALUATION_POINT_UNSPECIFIED) {
      throw ContextualStatusExceptionBuilder.from(Status.INVALID_ARGUMENT)
          .withExternalMessage("Rule evaluation point must be specified")
          .buildRuntimeException();
    }

    // Validate rule_version
    if (request.getRuleVersion()
        == ai.traceable.customsignature.config.service.v1.CustomModsecRuleVersion
            .CUSTOM_MODSEC_RULE_VERSION_UNSPECIFIED) {
      throw ContextualStatusExceptionBuilder.from(Status.INVALID_ARGUMENT)
          .withExternalMessage("Rule version must be specified")
          .buildRuntimeException();
    }

    // Validate event_type
    if (request.getEventType() == EventType.EVENT_TYPE_UNSPECIFIED) {
      throw ContextualStatusExceptionBuilder.from(Status.INVALID_ARGUMENT)
          .withExternalMessage("Event type must be specified")
          .buildRuntimeException();
    }
  }

  @Override
  public void validate(CreateCustomSignatureRuleRequest request) {
    if (request.getName().isEmpty()) {
      throw ContextualStatusExceptionBuilder.from(Status.INVALID_ARGUMENT)
          .withExternalMessage("Create custom signature rule should have a valid name.")
          .buildRuntimeException();
    }

    if (!request.hasDefinition()) {
      throw ContextualStatusExceptionBuilder.from(Status.INVALID_ARGUMENT)
          .withExternalMessage("Create custom signature rule should have a valid definition.")
          .buildRuntimeException();
    }

    if (!request.hasEffect()) {
      throw ContextualStatusExceptionBuilder.from(Status.INVALID_ARGUMENT)
          .withExternalMessage("Create custom signature rule should have a valid effect.")
          .buildRuntimeException();
    }

    ClauseGroup clauseGroup = request.getDefinition().getClauseGroup();

    validateRuleEffect(
        request.getEffect(), hasResponseOrAttribute(clauseGroup.getClausesList()), clauseGroup);
    validateRuleDefinition(request.getDefinition(), request.getEffect().getEventType());
    validateExpiry(request.getEffect(), request.getBlockingExpiryDetails());
    validateRuleScope(request.getRuleScope());

    if (ModsecRulesSupportChecker.isInlineRuleMappingSupported(clauseGroup)
        && modsecRulesManager.containsModsecConvertibleClauses(clauseGroup)) {
      modsecRulesManager.validateModsecRule(request.getName(), request.getDefinition());
    }
  }

  @Override
  public void validate(UpdateCustomSignatureRuleRequest request) {
    CustomSignatureRule rule = request.getRule();

    if (rule.getId().isEmpty()) {
      throw ContextualStatusExceptionBuilder.from(Status.INVALID_ARGUMENT)
          .withExternalMessage("Update custom signature rule should have a valid id.")
          .buildRuntimeException();
    }

    if (rule.getName().isEmpty()) {
      throw ContextualStatusExceptionBuilder.from(Status.INVALID_ARGUMENT)
          .withExternalMessage("Update custom signature rule should have a valid name.")
          .buildRuntimeException();
    }

    if (!rule.getRuleSource().equals(RuleSource.RULE_SOURCE_UNSPECIFIED)) {
      throw ContextualStatusExceptionBuilder.from(Status.INVALID_ARGUMENT)
          .withExternalMessage(
              String.format(
                  "Update request does not allow to update rule source for rule with id: %s",
                  rule.getId()))
          .buildRuntimeException();
    }

    if (!rule.hasDefinition()) {
      throw ContextualStatusExceptionBuilder.from(Status.INVALID_ARGUMENT)
          .withExternalMessage("Update custom signature rule should have a valid definition.")
          .buildRuntimeException();
    }

    if (!rule.hasEffect()) {
      throw ContextualStatusExceptionBuilder.from(Status.INVALID_ARGUMENT)
          .withExternalMessage("Update custom signature rule should have a valid effect.")
          .buildRuntimeException();
    }

    ClauseGroup clauseGroup = rule.getDefinitionOrBuilder().getClauseGroup();
    List<Clause> clauses = clauseGroup.getClausesList();
    boolean hasResponseOrAttribute = hasResponseOrAttribute(clauses);

    validateRuleEffect(rule.getEffect(), hasResponseOrAttribute, clauseGroup);
    validateRuleDefinition(rule.getDefinition(), rule.getEffect().getEventType());
    validateExpiry(rule.getEffect(), rule.getBlockingExpiryDetails());
    validateRuleScope(rule.getRuleScope());

    if (ModsecRulesSupportChecker.isInlineRuleMappingSupported(clauseGroup)
        && modsecRulesManager.containsModsecConvertibleClauses(clauseGroup)) {
      modsecRulesManager.validateModsecRule(rule.getName(), rule.getDefinition());
    }
  }

  @Override
  public void validate(DeleteCustomSignatureRuleRequest request) {
    String ruleId = request.getId();
    if (ruleId.isEmpty()) {
      throw ContextualStatusExceptionBuilder.from(Status.INVALID_ARGUMENT)
          .withExternalMessage("Delete custom signature rule should have a valid id")
          .buildRuntimeException();
    }
  }

  @Override
  public void validate(BulkDeleteCustomSignatureRulesRequest request) {
    if (request.getIdsCount() == 0) {
      throw ContextualStatusExceptionBuilder.from(Status.INVALID_ARGUMENT)
          .withExternalMessage("Bulk delete custom signature rules should have at least one id")
          .buildRuntimeException();
    }
    for (String id : request.getIdsList()) {
      if (id.isEmpty()) {
        throw ContextualStatusExceptionBuilder.from(Status.INVALID_ARGUMENT)
            .withExternalMessage("Bulk delete custom signature rules should not contain empty ids")
            .buildRuntimeException();
      }
    }
  }

  @Override
  public void validate(GetCustomSignatureEdgeDecisionRulesRequest request) {
    validateFilter(request.getRulesFilter());
  }

  @Override
  public void validate(GetCustomSignatureRulesRequest request) {
    validateFilter(request.getFilter());
  }

  @Override
  public void validate(GetCustomSignatureModsecRulesRequest request) {
    validateFilter(request.getFilter());
  }

  @Override
  public void validate(BulkUpdateCustomSignatureRulesRequest request) {
    if (request.getIdsList().isEmpty()) {
      throw ContextualStatusExceptionBuilder.from(Status.INVALID_ARGUMENT)
          .withExternalMessage(
              "Bulk update Custom Signature rules request should have at least one id")
          .buildRuntimeException();
    }
    if (request.getIdsList().stream().anyMatch(String::isEmpty)) {
      throw ContextualStatusExceptionBuilder.from(Status.INVALID_ARGUMENT)
          .withExternalMessage(
              "Bulk update Custom Signature rules request should not have empty ids")
          .buildRuntimeException();
    }
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

  private void validateFilter(GetRulesFilter rulesFilter) {
    if (rulesFilter.hasFilterEdgeDecisionRules() && !rulesFilter.getFilterEdgeDecisionRules()) {
      throw ContextualStatusExceptionBuilder.from(Status.INVALID_ARGUMENT)
          .withExternalMessage(
              "Use get edge decision rules api and not call this api with filterEdgeDecisionRules set to false.")
          .buildRuntimeException();
    }
  }

  private void validateRuleEffect(
      RuleEffect ruleEffect,
      boolean hasMatchCategoryResponseOrAttributeKeyValueExpression,
      ClauseGroup clauseGroup) {
    EventType eventType = ruleEffect.getEventType();
    if (eventType == EventType.EVENT_TYPE_UNSPECIFIED) {
      throw ContextualStatusExceptionBuilder.from(Status.INVALID_ARGUMENT)
          .withExternalMessage("Custom Signature Rule Effect should have a valid event type.")
          .buildRuntimeException();
    }

    if (eventType == EVENT_TYPE_NORMAL_DETECTION
        && ruleEffect.getEventSeverity() == EventSeverity.EVENT_SEVERITY_UNSPECIFIED) {
      throw ContextualStatusExceptionBuilder.from(Status.INVALID_ARGUMENT)
          .withExternalMessage(
              "Custom Signature Rule Effect with alert action should have a valid event severity.")
          .buildRuntimeException();
    }

    if (eventType == EVENT_TYPE_DETECTION_AND_BLOCKING
        && ruleEffect.getEffectsList().stream()
            .flatMap(effect -> effect.getAgentRuleEffect().getAgentModificationsList().stream())
            .anyMatch(AgentModification::hasHeaderInjection)) {
      throw ContextualStatusExceptionBuilder.from(Status.INVALID_ARGUMENT)
          .withExternalMessage(
              "Custom Signature Rule Effect with block action should not have header injection.")
          .buildRuntimeException();
    }

    if (hasMatchCategoryResponseOrAttributeKeyValueExpression
        && INVALID_RESPONSE_AND_ATTRIBUTE_EVENT_TYPES.contains(ruleEffect.getEventType())) {
      throw ContextualStatusExceptionBuilder.from(Status.INVALID_ARGUMENT)
          .withExternalMessage(
              String.format(
                  "Custom signature rule with a response category or a attribute clause is not compatible with the specified event type %s.",
                  ruleEffect.getEventType()))
          .buildRuntimeException();
    }

    validateForRuleEvaluationPoints(ruleEffect, clauseGroup);
    ruleEffect.getEffectsList().forEach(this::validateRuleEffectWithModification);
  }

  private void validateForRuleEvaluationPoints(RuleEffect ruleEffect, ClauseGroup clauseGroup) {
    List<RuleEvaluationPoint> ruleEvaluationPoints = ruleEffect.getRuleEvaluationPointsList();

    if (ruleEvaluationPoints.isEmpty()) {
      throw ContextualStatusExceptionBuilder.from(Status.INVALID_ARGUMENT)
          .withExternalMessage("RuleEvaluationPoints cannot be empty.")
          .buildRuntimeException();
    }

    if (ruleEvaluationPoints.contains(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)) {
      if (!CustomSignatureRulesEdgeDecisionFilter.isConvertibleRule(ruleEffect, clauseGroup)) {
        throw ContextualStatusExceptionBuilder.from(Status.INVALID_ARGUMENT)
            .withExternalMessage("Rule is not EDGE-compatible.")
            .buildRuntimeException();
      }
    }

    if (ruleEvaluationPoints.contains(
        RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT)) {
      /*
       * ModSec rules only support AND operators for clause groups.
       * OR operators are unsupported as they are computationally expensive in ModSec's rule evaluation engine.
       */
      if (clauseGroup.getClauseOperator() == ClauseOperator.CLAUSE_OPERATOR_OR) {
        throw ContextualStatusExceptionBuilder.from(Status.INVALID_ARGUMENT)
            .withExternalMessage(
                "Rules evaluated at INLINE_TRACING_AGENT cannot have OR clause operator.")
            .buildRuntimeException();
      }

      if (clauseGroup.getClausesList().stream().anyMatch(Clause::hasClauseGroup)) {
        throw ContextualStatusExceptionBuilder.from(Status.INVALID_ARGUMENT)
            .withExternalMessage(
                "Rules evaluated at INLINE_TRACING_AGENT cannot have nested clauses.")
            .buildRuntimeException();
      }

      if (!ModsecRulesSupportChecker.isInlineRuleMappingSupported(clauseGroup)) {
        throw ContextualStatusExceptionBuilder.from(Status.INVALID_ARGUMENT)
            .withExternalMessage("Rule is not AGENT-compatible.")
            .buildRuntimeException();
      }

      EventType eventType = ruleEffect.getEventType();
      boolean isCompatibleEventType =
          eventType == EVENT_TYPE_ALLOW
              || eventType == EVENT_TYPE_DETECTION_AND_BLOCKING
              || isRuleOfEventTypeAlertAndContainsHeaderInjection(ruleEffect);
      if (!isCompatibleEventType) {
        throw ContextualStatusExceptionBuilder.from(Status.INVALID_ARGUMENT)
            .withExternalMessage("Rule is not AGENT-compatible.")
            .buildRuntimeException();
      }
    }
  }

  private void validateRuleEffectWithModification(RuleEffectWithModifications effect) {
    if (!effect.hasAgentRuleEffect()) {
      throw ContextualStatusExceptionBuilder.from(Status.INVALID_ARGUMENT)
          .withExternalMessage("Modification rule effect should have at least one modification.")
          .buildRuntimeException();
    }

    validateNonDefaultPresenceOrThrow(
        effect.getAgentRuleEffect(), RuleEffectWithModifications.AGENT_RULE_EFFECT_FIELD_NUMBER);
    AgentRuleEffect agentRuleEffect = effect.getAgentRuleEffect();
    if (agentRuleEffect.getAgentModificationsList().isEmpty()) {
      throw ContextualStatusExceptionBuilder.from(Status.INVALID_ARGUMENT)
          .withExternalMessage("Agent rule effect should have at least one modification.")
          .buildRuntimeException();
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
                  throw ContextualStatusExceptionBuilder.from(Status.INVALID_ARGUMENT)
                      .withExternalMessage("Request body modification not supported yet.")
                      .buildRuntimeException();
                }
                validateNonDefaultPresenceOrThrow(
                    bodyModification.getBodyValue(), FieldValue.STATIC_VALUE_FIELD_NUMBER);
              } else {
                throw ContextualStatusExceptionBuilder.from(Status.INVALID_ARGUMENT)
                    .withExternalMessage(
                        "Agent modification must specify a valid modification type.")
                    .buildRuntimeException();
              }
            });
  }

  private void validateRuleDefinition(RuleDefinition ruleDefinition, EventType eventType) {
    if (ruleDefinition.getLabelsMap().size() > CustomSignatureRulesValidator.CUSTOM_LABELS_LIMIT) {
      throw ContextualStatusExceptionBuilder.from(Status.INVALID_ARGUMENT)
          .withExternalMessage("Custom labels limit exceeded.")
          .buildRuntimeException();
    }

    if (!ruleDefinition.hasClauseGroup()) {
      throw ContextualStatusExceptionBuilder.from(Status.INVALID_ARGUMENT)
          .withExternalMessage(
              "Create custom signature rule definition should have a valid clause group.")
          .buildRuntimeException();
    }

    Status clauseGroupStatus =
        clauseGroupValidator.validateClauseGroup(ruleDefinition.getClauseGroup(), eventType);
    if (!clauseGroupStatus.isOk()) {
      throw ContextualStatusExceptionBuilder.from(clauseGroupStatus)
          .withExternalMessage(clauseGroupStatus.getDescription())
          .buildRuntimeException();
    }
  }

  private void validateExpiry(RuleEffect ruleEffect, ExpiryDetails expiry) {
    if (expiry.hasExpiryDuration()) {
      if (!(ruleEffect.getEventType().equals(EVENT_TYPE_DETECTION_AND_BLOCKING)
          || ruleEffect.getEventType().equals(EVENT_TYPE_ALLOW))) {
        throw ContextualStatusExceptionBuilder.from(Status.INVALID_ARGUMENT)
            .withExternalMessage(
                "Expiry duration can be specified only for blocking or allow event type.")
            .buildRuntimeException();
      }
      try {
        Duration.parse(expiry.getExpiryDuration());
      } catch (DateTimeParseException e) {
        throw ContextualStatusExceptionBuilder.from(Status.INVALID_ARGUMENT)
            .withExternalMessage("Blocking expiry duration can't be parsed.")
            .buildRuntimeException();
      }
    }
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

  private void validateRuleScope(RuleScope scope) {
    if (scope.hasEnvironmentScope()) {
      List<String> environmentIdList = scope.getEnvironmentScope().getEnvironmentIdsList();
      if (environmentIdList.isEmpty()) {
        throw ContextualStatusExceptionBuilder.from(Status.INVALID_ARGUMENT)
            .withExternalMessage("Environment scope should have at least one environment.")
            .buildRuntimeException();
      } else {
        if (environmentIdList.stream().anyMatch(String::isEmpty)) {
          throw ContextualStatusExceptionBuilder.from(Status.INVALID_ARGUMENT)
              .withExternalMessage("Environment id should not be empty string.")
              .buildRuntimeException();
        }
      }
    }
  }
}
