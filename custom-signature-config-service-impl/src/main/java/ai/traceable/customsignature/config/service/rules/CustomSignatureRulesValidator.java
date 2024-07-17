package ai.traceable.customsignature.config.service.rules;

import static ai.traceable.customsignature.config.service.v1.MatchKey.MATCH_KEY_HOST;
import static ai.traceable.customsignature.config.service.v1.MatchKey.MATCH_KEY_HTTP_METHOD;
import static ai.traceable.customsignature.config.service.v1.MatchKey.MATCH_KEY_QUERY_PARAMS_COUNT;
import static ai.traceable.customsignature.config.service.v1.MatchKey.MATCH_KEY_URL;
import static ai.traceable.customsignature.config.service.v1.MatchKey.MATCH_KEY_USER_AGENT;
import static ai.traceable.customsignature.config.service.v1.MatchOperator.MATCH_OPERATOR_GREATER_THAN;
import static ai.traceable.customsignature.config.service.v1.MatchOperator.MATCH_OPERATOR_LESS_THAN;
import static ai.traceable.customsignature.config.service.v1.MatchOperator.MATCH_OPERATOR_MATCHES_REGEX;
import static ai.traceable.customsignature.config.service.v1.MatchOperator.MATCH_OPERATOR_NOT_MATCH_REGEX;
import static ai.traceable.modsecurity.rule.secrule.ModsecRuleConstants.SEC_RULE;
import static ai.traceable.modsecurity.rule.secrule.ModsecRuleConstants.SEC_RULE_DIRECTIVES_WITH_CHAIN_KEYWORDS_REGEX;
import static ai.traceable.modsecurity.rule.secrule.ModsecRuleConstants.SEC_RULE_ID_REGEX;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;

import ai.traceable.customsignature.config.service.modsec.ModsecRulesManager;
import ai.traceable.customsignature.config.service.v1.AttributeKeyValueExpression;
import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.ClauseGroup;
import ai.traceable.customsignature.config.service.v1.ClauseOperator;
import ai.traceable.customsignature.config.service.v1.CreateCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.CustomSecRule;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.DeleteCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.EventSeverity;
import ai.traceable.customsignature.config.service.v1.EventType;
import ai.traceable.customsignature.config.service.v1.ExpiryDetails;
import ai.traceable.customsignature.config.service.v1.KeyValueExpression;
import ai.traceable.customsignature.config.service.v1.KeyValueTag;
import ai.traceable.customsignature.config.service.v1.MatchCategory;
import ai.traceable.customsignature.config.service.v1.MatchExpression;
import ai.traceable.customsignature.config.service.v1.MatchKey;
import ai.traceable.customsignature.config.service.v1.MatchOperator;
import ai.traceable.customsignature.config.service.v1.RuleDefinition;
import ai.traceable.customsignature.config.service.v1.RuleEffect;
import ai.traceable.customsignature.config.service.v1.RuleScope;
import ai.traceable.customsignature.config.service.v1.StringCondition;
import ai.traceable.customsignature.config.service.v1.UpdateCustomSignatureRuleRequest;
import com.google.re2j.Matcher;
import com.google.re2j.Pattern;
import com.google.re2j.PatternSyntaxException;
import io.grpc.Status;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Set;
import javax.inject.Inject;

class CustomSignatureRulesValidator implements RulesValidator {

  private static final String UTF_8_REGEX_PREFIX = "(*UTF8)";
  private static final Set<EventType> INVALID_RESPONSE_AND_ATTRIBUTE_EVENT_TYPES =
      Set.of(EventType.EVENT_TYPE_ALLOW, EventType.EVENT_TYPE_DETECTION_AND_BLOCKING);

  private static final Set<MatchKey> INVALID_RESPONSE_MATCH_KEYS =
      Set.of(
          MATCH_KEY_URL,
          MATCH_KEY_QUERY_PARAMS_COUNT,
          MATCH_KEY_HOST,
          MATCH_KEY_HTTP_METHOD,
          MATCH_KEY_USER_AGENT);

  private final ModsecRulesManager modsecRulesManager;

  @Inject
  public CustomSignatureRulesValidator(ModsecRulesManager modsecRulesManager) {
    this.modsecRulesManager = modsecRulesManager;
  }

  @Override
  public Status validate(CreateCustomSignatureRuleRequest request) {
    if (request.getName().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Create custom signature rule should have a valid name.");
    }

    Status status;

    boolean hasResponseOrAttribute =
        hasResponseOrAttribute(request.getDefinition().getClauseGroup().getClausesList());
    if (!request.hasEffect()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Create custom signature rule should have a valid effect.");
    }
    if ((status = validateRuleEffect(request.getEffect(), hasResponseOrAttribute)) != Status.OK) {
      return status;
    }

    if (!request.hasDefinition()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Create custom signature rule should have a valid definition.");
    }
    if ((status = validateRuleDefinition(request.getDefinition())) != Status.OK) {
      return status;
    }

    if ((status = validateExpiry(request.getBlockingExpiryDetails())) != Status.OK) {
      return status;
    }

    if ((status = validateRuleScope(request.getRuleScope())) != Status.OK) {
      return status;
    }
    if (INVALID_RESPONSE_AND_ATTRIBUTE_EVENT_TYPES.contains(request.getEffect().getEventType())) {
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

    Status status;

    boolean hasResponseOrAttribute =
        hasResponseOrAttribute(rule.getDefinition().getClauseGroup().getClausesList());
    if (!rule.hasEffect()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Create custom signature rule should have a valid effect.");
    }
    if ((status = validateRuleEffect(rule.getEffect(), hasResponseOrAttribute)) != Status.OK) {
      return status;
    }

    if (!rule.hasDefinition()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Create custom signature rule should have a valid definition.");
    }
    if ((status = validateRuleDefinition(rule.getDefinition())) != Status.OK) {
      return status;
    }

    if ((status = validateExpiry(rule.getBlockingExpiryDetails())) != Status.OK) {
      return status;
    }

    if ((status = validateRuleScope(rule.getRuleScope())) != Status.OK) {
      return status;
    }
    if (INVALID_RESPONSE_AND_ATTRIBUTE_EVENT_TYPES.contains(rule.getEffect().getEventType())) {
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

    if (hasMatchCategoryResponseOrAttributeKeyValueExpression
        && INVALID_RESPONSE_AND_ATTRIBUTE_EVENT_TYPES.contains(ruleEffect.getEventType())) {
      return Status.INVALID_ARGUMENT.withDescription(
          String.format(
              "Custom signature rule with a response category or a attribute clause is not compatible with the specified event type %s.",
              ruleEffect.getEventType()));
    }
    return Status.OK;
  }

  private Status validateRuleDefinition(RuleDefinition ruleDefinition) {
    if (!ruleDefinition.hasClauseGroup()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Create custom signature rule definition should have a valid clause group.");
    }
    return validateClauseGroup(ruleDefinition.getClauseGroup());
  }

  private Status validateClauseGroup(ClauseGroup clauseGroup) {
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
      if ((status = validateClause(clause)) != Status.OK) {
        return status;
      }
    }
    return Status.OK;
  }

  private Status validateClause(Clause clause) {
    switch (clause.getClauseCase()) {
      case MATCH_EXPRESSION:
        return validateMatchExpression(clause.getMatchExpression());
      case KEY_VALUE_EXPRESSION:
        return validateKeyValueExpression(clause.getKeyValueExpression());
      case ATTRIBUTE_KEY_VALUE_EXPRESSION:
        return validateAttributeKeyValueExpression(clause.getAttributeKeyValueExpression());
      case CUSTOM_SEC_RULE:
        return validateCustomSecRule(clause.getCustomSecRule());
      default:
        return Status.INVALID_ARGUMENT.withDescription(
            "Custom Signature Rule Clause should have a valid expression");
    }
  }

  private Status validateMatchExpression(MatchExpression matchExpression) {
    if (matchExpression.getMatchKey() == MatchKey.MATCH_KEY_UNSPECIFIED) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Custom Signature Rule match expression should have a valid match key.");
    }
    if (matchExpression.getMatchOperator() == MatchOperator.MATCH_OPERATOR_UNSPECIFIED) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Custom Signature Rule match expression should have a valid match operator.");
    }
    if (matchExpression.getMatchCategory().equals(MatchCategory.MATCH_CATEGORY_RESPONSE)
        && INVALID_RESPONSE_MATCH_KEYS.contains(matchExpression.getMatchKey())) {
      return Status.INVALID_ARGUMENT.withDescription(
          String.format(
              "Invalid match key : %s for match category : %s for custom signature rule",
              matchExpression.getMatchKey(), matchExpression.getMatchCategory()));
    }
    if (isInvalidMathematicalOperation(matchExpression)) {
      return Status.INVALID_ARGUMENT.withDescription(
          String.format(
              "Custom Signature Rule match expression should have numerical value for match operator : %s",
              matchExpression.getMatchOperator()));
    }
    if (matchExpression.getMatchOperator() == MATCH_OPERATOR_MATCHES_REGEX
        || matchExpression.getMatchOperator() == MATCH_OPERATOR_NOT_MATCH_REGEX) {
      return validateRegex(matchExpression.getMatchValue());
    }
    return Status.OK;
  }

  private boolean isInvalidMathematicalOperation(MatchExpression matchExpression) {
    return (matchExpression.getMatchOperator().equals(MATCH_OPERATOR_GREATER_THAN)
            || matchExpression.getMatchOperator().equals(MATCH_OPERATOR_LESS_THAN))
        && !isNumber(matchExpression.getMatchValue());
  }

  private Status validateKeyValueExpression(KeyValueExpression keyValueExpression) {
    if (keyValueExpression.getTag() == KeyValueTag.KEY_VALUE_TAG_UNSPECIFIED) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Custom Signature Rule key-value expression should have a valid tag.");
    }
    return validateExpression(
        false,
        keyValueExpression.getMatchKey(),
        keyValueExpression.getKeyMatchOperator(),
        keyValueExpression.getMatchValue(),
        keyValueExpression.getValueMatchOperator());
  }

  private Status validateAttributeKeyValueExpression(
      AttributeKeyValueExpression attributeKeyValueExpression) {
    if (!attributeKeyValueExpression.hasKeyCondition()) {
      Status status =
          validateExpression(
              true,
              attributeKeyValueExpression.getMatchKey(),
              attributeKeyValueExpression.getKeyMatchOperator(),
              attributeKeyValueExpression.getMatchValue(),
              attributeKeyValueExpression.getValueMatchOperator());
      if (status != Status.OK) {
        return Status.INVALID_ARGUMENT.withDescription(
            String.format(
                "Invalid attribute key value expression : %s", attributeKeyValueExpression));
      }
      return Status.OK;
    }
    validateStringCondition(attributeKeyValueExpression.getKeyCondition());
    if (attributeKeyValueExpression.hasValueCondition()) {
      validateStringCondition(attributeKeyValueExpression.getValueCondition());
    }
    return Status.OK;
  }

  private void validateStringCondition(StringCondition stringCondition) {
    validateNonDefaultPresenceOrThrow(stringCondition, StringCondition.OPERATOR_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(stringCondition, StringCondition.VALUE_FIELD_NUMBER);
    if (stringCondition.getOperator() == MATCH_OPERATOR_MATCHES_REGEX
        || stringCondition.getOperator() == MATCH_OPERATOR_NOT_MATCH_REGEX) {
      validateRegex(stringCondition.getValue());
    }
  }

  private Status validateExpression(
      boolean isEmptyValueAllowed,
      String matchKey,
      MatchOperator keyMatchOperator,
      String matchValue,
      MatchOperator valueMatchOperator) {
    if (matchKey.isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Custom Signature Rule expression should have a valid match key.");
    }
    if (keyMatchOperator == MatchOperator.MATCH_OPERATOR_UNSPECIFIED) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Custom Signature Rule expression should have a valid key match operator.");
    }
    if (!isEmptyValueAllowed && matchValue.isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Custom Signature Rule expression should have a valid match value.");
    }
    if (!matchValue.isEmpty() && valueMatchOperator == MatchOperator.MATCH_OPERATOR_UNSPECIFIED) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Custom Signature Rule expression should have a valid value match operator.");
    }
    if (valueMatchOperator == MATCH_OPERATOR_MATCHES_REGEX
        || valueMatchOperator == MATCH_OPERATOR_NOT_MATCH_REGEX) {
      return validateRegex(matchValue);
    }
    return Status.OK;
  }

  private Status validateExpiry(ExpiryDetails expiry) {
    if (expiry.hasExpiryDuration()) {
      try {
        Duration.parse(expiry.getExpiryDuration());
      } catch (DateTimeParseException e) {
        return Status.INVALID_ARGUMENT.withDescription("Blocking expiry duration can't be parsed");
      }
    }
    return Status.OK;
  }

  private Status validateRegex(String regexPattern) {
    if (regexPattern.startsWith(UTF_8_REGEX_PREFIX)) {
      return Status.OK;
    }
    // compiling an invalid regex throws PatternSyntaxException
    try {
      Pattern.compile(regexPattern);
      return Status.OK;
    } catch (PatternSyntaxException e) {
      return Status.INVALID_ARGUMENT
          .withCause(e)
          .withDescription("Invalid Regex Value for the custom signature rule expression");
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

  private Status validateCustomSecRule(CustomSecRule rule) {
    String inputSecRule = rule.getInputSecRule();
    if (!inputSecRule.startsWith(SEC_RULE)) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Custom Sec Rule should be start with keyword SecRule");
    }

    if (!SEC_RULE_ID_REGEX.matcher(inputSecRule).find()) {
      return Status.INVALID_ARGUMENT.withDescription("Sec Rule actions should start with \"id: ");
    }

    if (checkChainKeywords(inputSecRule)) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Chain keyword should be there between any 2 directives containing SecAction or SecRule or SecRuleScript");
    }
    if (!rule.getSanitisedSecRule().isBlank()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Sanitized Sec Rule should be empty in create/update request");
    }
    return Status.OK;
  }

  public boolean checkChainKeywords(String inputSecRule) {
    Matcher matcher = SEC_RULE_DIRECTIVES_WITH_CHAIN_KEYWORDS_REGEX.matcher(inputSecRule);
    return matcher.matches();
  }

  private boolean isNumber(String value) {
    try {
      Double.parseDouble(value);
      return true;
    } catch (Exception e) {
      return false;
    }
  }
}
