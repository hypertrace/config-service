package ai.traceable.customsignature.config.service.rules;

import static ai.traceable.customsignature.config.service.v1.EventType.EVENT_TYPE_DETECTION_AND_BLOCKING;
import static ai.traceable.customsignature.config.service.v1.MatchCategory.MATCH_CATEGORY_REQUEST;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;

import ai.traceable.customsignature.config.service.modsec.ModsecRulesManager;
import ai.traceable.customsignature.config.service.v1.AgentModification;
import ai.traceable.customsignature.config.service.v1.AgentRuleEffect;
import ai.traceable.customsignature.config.service.v1.BodyModification;
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
import ai.traceable.customsignature.config.service.v1.RuleScope;
import ai.traceable.customsignature.config.service.v1.RuleSource;
import ai.traceable.customsignature.config.service.v1.UpdateCustomSignatureRuleRequest;
import io.grpc.Status;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Set;

class CustomSignatureRulesValidator implements RulesValidator {

  private static final Set<EventType> INVALID_RESPONSE_AND_ATTRIBUTE_EVENT_TYPES =
      Set.of(EventType.EVENT_TYPE_ALLOW, EVENT_TYPE_DETECTION_AND_BLOCKING);

  private static final Integer CUSTOM_LABELS_LIMIT = 5;

  private final ModsecRulesManager modsecRulesManager;

  private final ClauseValidator clauseValidator;

  @Inject
  public CustomSignatureRulesValidator(
      ModsecRulesManager modsecRulesManager, ClauseValidator clauseValidator) {
    this.modsecRulesManager = modsecRulesManager;
    this.clauseValidator = clauseValidator;
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
    Status status;
    ClauseGroup clauseGroup = request.getDefinition().getClauseGroup();
    if ((status =
            validateRuleEffect(
                request.getEffect(), hasResponseOrAttribute(clauseGroup.getClausesList())))
        != Status.OK) {
      return status;
    }

    if ((status =
            validateRuleDefinition(request.getDefinition(), request.getEffect().getEventType()))
        != Status.OK) {
      return status;
    }

    if ((status = validateExpiry(request.getEffect(), request.getBlockingExpiryDetails()))
        != Status.OK) {
      return status;
    }

    if ((status = validateRuleScope(request.getRuleScope())) != Status.OK) {
      return status;
    }
    if (modsecRulesManager.isInlineRuleMappingSupported(clauseGroup)
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
          "Create custom signature rule should have a valid definition.");
    }

    if (!rule.hasEffect()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Create custom signature rule should have a valid effect.");
    }
    Status status;
    List<Clause> clauses = rule.getDefinition().getClauseGroup().getClausesList();
    boolean hasResponseOrAttribute = hasResponseOrAttribute(clauses);
    if ((status = validateRuleEffect(rule.getEffect(), hasResponseOrAttribute)) != Status.OK) {
      return status;
    }

    if ((status = validateRuleDefinition(rule.getDefinition(), rule.getEffect().getEventType()))
        != Status.OK) {
      return status;
    }

    if ((status = validateExpiry(rule.getEffect(), rule.getBlockingExpiryDetails())) != Status.OK) {
      return status;
    }

    if ((status = validateRuleScope(rule.getRuleScope())) != Status.OK) {
      return status;
    }
    ClauseGroup clauseGroup = rule.getDefinition().getClauseGroup();
    if (modsecRulesManager.isInlineRuleMappingSupported(clauseGroup)
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

  private Status validateFilter(GetRulesFilter rulesFilter) {
    if (rulesFilter.hasFilterEdgeDecisionRules() && !rulesFilter.getFilterEdgeDecisionRules()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Use get edge decision rules api and not call this api with filterEdgeDecisionRules set to false");
    }
    return Status.OK;
  }

  private Status validateRuleEffect(
      RuleEffect ruleEffect, boolean hasMatchCategoryResponseOrAttributeKeyValueExpression) {
    EventType eventType = ruleEffect.getEventType();
    if (eventType == EventType.EVENT_TYPE_UNSPECIFIED) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Custom Signature Rule Effect should have a valid event type.");
    }
    if (eventType == EventType.EVENT_TYPE_NORMAL_DETECTION
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
    ruleEffect.getEffectsList().forEach(this::validateRuleEffectWithModification);
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
    return validateClauseGroup(ruleDefinition.getClauseGroup(), eventType);
  }

  private Status validateClauseGroup(ClauseGroup clauseGroup, EventType eventType) {
    if (clauseGroup.getClauseOperator() == ClauseOperator.CLAUSE_OPERATOR_UNSPECIFIED) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Custom Signature Rule Definition clause group should have a valid clause operator.");
    }
    if (clauseGroup.getClausesList().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Custom Signature Rule Definition clause group should have at least one clause.");
    }
    Status status;
    for (Clause clause : clauseGroup.getClausesList()) {
      if ((status = clauseValidator.validateClause(clause, eventType)) != Status.OK) {
        return status;
      }
    }
    // custom signature rule containing SecRule clause should not have OR operator or nested clauses
    // since that's not yet supported in platform
    // examples of such rules:
    //  - (SecRuleClause) OR (KeyValueExpression)
    // - (SecRuleClause) AND (KeyValueExpression OR IpAddressExpression)
    if (containsSecRuleClause(clauseGroup)
        && (containsNestedClause(clauseGroup)
            || clauseGroup.getClauseOperator().equals(ClauseOperator.CLAUSE_OPERATOR_OR))) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Custom Signature Rule Definition clause group with sec rule clause "
              + "should not have nested clauses or OR operator.");
    }
    return Status.OK;
  }

  private boolean containsSecRuleClause(ClauseGroup clauseGroup) {
    return clauseGroup.getClausesList().stream()
        .anyMatch(
            clause ->
                clause.hasCustomSecRule()
                    || (clause.hasClauseGroup() && containsSecRuleClause(clause.getClauseGroup())));
  }

  private boolean containsNestedClause(ClauseGroup clauseGroup) {
    return clauseGroup.getClausesList().stream().anyMatch(Clause::hasClauseGroup);
  }

  private Status validateExpiry(RuleEffect ruleEffect, ExpiryDetails expiry) {
    if (expiry.hasExpiryDuration()) {
      if (!ruleEffect.getEventType().equals(EVENT_TYPE_DETECTION_AND_BLOCKING)) {
        return Status.INVALID_ARGUMENT.withDescription(
            "Blocking expiry duration can be specified only for blocking event type");
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
