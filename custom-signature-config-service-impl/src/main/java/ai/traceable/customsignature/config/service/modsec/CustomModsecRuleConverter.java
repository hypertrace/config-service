package ai.traceable.customsignature.config.service.modsec;

import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.KeyValueExpression;
import ai.traceable.customsignature.config.service.v1.KeyValueTag;
import ai.traceable.customsignature.config.service.v1.MatchCategory;
import ai.traceable.customsignature.config.service.v1.MatchExpression;
import ai.traceable.customsignature.config.service.v1.MatchKey;
import ai.traceable.customsignature.config.service.v1.MatchOperator;
import ai.traceable.modsecurity.rule.api.v1.CustomModsecKeyValueMatchClause;
import ai.traceable.modsecurity.rule.api.v1.CustomModsecMatchExpression;
import ai.traceable.modsecurity.rule.api.v1.CustomModsecRule;
import ai.traceable.modsecurity.rule.api.v1.CustomModsecRuleClause;
import ai.traceable.modsecurity.rule.api.v1.CustomModsecValueMatchClause;
import ai.traceable.modsecurity.rule.api.v1.CustomSecRuleClause;
import ai.traceable.modsecurity.rule.api.v1.RequestKeyValueMatchMetadata;
import ai.traceable.modsecurity.rule.api.v1.RequestValueMatchMetadata;
import ai.traceable.modsecurity.rule.api.v1.ResponseKeyValueMatchMetadata;
import ai.traceable.modsecurity.rule.api.v1.ResponseValueMatchMetadata;
import ai.traceable.modsecurity.rule.conversion.ModsecRuleConverter;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class CustomModsecRuleConverter {

  private final ModsecRuleConverter modsecRuleConverter;

  @Inject
  public CustomModsecRuleConverter(ModsecRuleConverter modsecRuleConverter) {
    this.modsecRuleConverter = modsecRuleConverter;
  }

  public String getValidatedModsecRule(
      long ruleId, String ruleUuid, String ruleMsg, List<Clause> clauses) throws Exception {
    return modsecRuleConverter.getValidatedModsecRule(
        createCustomModsecRule(ruleId, ruleUuid, ruleMsg, clauses, Optional.empty()));
  }

  public String getValidatedModsecRuleWithCustomLogMsg(
      long ruleId, String ruleUuid, String ruleMsg, List<Clause> clauses, String logMsg)
      throws Exception {
    return modsecRuleConverter.getValidatedModsecRule(
        createCustomModsecRule(ruleId, ruleUuid, ruleMsg, clauses, Optional.ofNullable(logMsg)));
  }

  public String getJNIValidatedModsecRuleWithCustomLogMsg(
      long ruleId, String ruleUuid, String ruleMsg, List<Clause> clauses, String logMsg)
      throws Exception {
    return modsecRuleConverter.getJNIValidatedModsecRule(
        createCustomModsecRule(ruleId, ruleUuid, ruleMsg, clauses, Optional.ofNullable(logMsg)));
  }

  private CustomModsecRule createCustomModsecRule(
      long ruleId, String ruleUuid, String ruleMsg, List<Clause> clauses, Optional<String> logMsg) {
    if (clauses.isEmpty()) {
      throw new IllegalArgumentException(
          "There should be at least one valid clause in the rule: " + ruleId);
    }
    CustomModsecRule.Builder builder =
        CustomModsecRule.newBuilder()
            .setRuleId(ruleId)
            .setRuleUuid(ruleUuid)
            .setRuleMsg(ruleMsg)
            .addAllAndClauses(
                clauses.stream().map(this::convert).collect(Collectors.toUnmodifiableList()));
    logMsg.ifPresent(builder::setLogMessage);
    return builder.build();
  }

  private CustomModsecRuleClause convert(Clause clause) {
    switch (clause.getClauseCase()) {
      case MATCH_EXPRESSION:
        return convert(clause.getMatchExpression());
      case KEY_VALUE_EXPRESSION:
        return convert(clause.getKeyValueExpression());
      case CUSTOM_SEC_RULE:
        return convert(clause.getCustomSecRule().getInputSecRule());
      default:
        throw new IllegalArgumentException("Unknown clause case: " + clause.getClauseCase());
    }
  }

  private CustomModsecRuleClause convert(String inputSecRule) {
    return CustomModsecRuleClause.newBuilder()
        .setCustomSecRuleClause(CustomSecRuleClause.newBuilder().setInputSecRule(inputSecRule))
        .build();
  }

  private CustomModsecRuleClause convert(MatchExpression expression) {
    CustomModsecRuleClause.Builder clauseBuilder = CustomModsecRuleClause.newBuilder();
    CustomModsecMatchExpression matchExpression =
        convert(expression.getMatchOperator(), expression.getMatchValue());

    Optional<CustomModsecValueMatchClause> valueMatchClause = Optional.empty();
    Optional<RequestValueMatchMetadata> requestMetadata =
        this.getRequestValueMatchMetadata(expression.getMatchKey());
    if (requestMetadata.isPresent()) {
      valueMatchClause =
          Optional.of(
              CustomModsecValueMatchClause.newBuilder()
                  .setRequestValueMetadata(requestMetadata.get())
                  .setValueMatchExpression(matchExpression)
                  .build());
    }
    if (valueMatchClause.isEmpty()) {
      valueMatchClause =
          this.getResponseValueMatchMetadata(expression.getMatchKey())
              .map(
                  responseValueMatchMetadata ->
                      CustomModsecValueMatchClause.newBuilder()
                          .setResponseValueMetadata(responseValueMatchMetadata)
                          .setValueMatchExpression(matchExpression)
                          .build());
    }

    if (valueMatchClause.isPresent()) {
      return clauseBuilder.setValueMatchClause(valueMatchClause.get()).build();
    }

    CustomModsecKeyValueMatchClause.Builder builder = CustomModsecKeyValueMatchClause.newBuilder();
    if (expression.getMatchKey().name().endsWith("_NAME")) {
      builder.setKeyMatchExpression(matchExpression);
    } else if (expression.getMatchKey().name().endsWith("_VALUE")) {
      builder.setValueMatchExpression(matchExpression);
    } else {
      throw new IllegalArgumentException(
          String.format("Unsupported match key: %s", expression.getMatchKey()));
    }

    if (expression.getMatchCategory() == MatchCategory.MATCH_CATEGORY_RESPONSE) {
      builder.setResponseMetadata(getResponseKeyValueMatchMetadata(expression.getMatchKey()));
    } else { // request
      builder.setRequestMetadata(getRequestKeyValueMatchMetadata(expression.getMatchKey()));
    }
    return clauseBuilder.setKeyValueMatchClause(builder).build();
  }

  private CustomModsecRuleClause convert(KeyValueExpression expression) {
    CustomModsecKeyValueMatchClause.Builder builder =
        CustomModsecKeyValueMatchClause.newBuilder()
            .setKeyMatchExpression(
                convert(expression.getKeyMatchOperator(), expression.getMatchKey()))
            .setValueMatchExpression(
                convert(expression.getValueMatchOperator(), expression.getMatchValue()));

    if (expression.getMatchCategory() == MatchCategory.MATCH_CATEGORY_RESPONSE) {
      builder.setResponseMetadata(getResponseKeyValueMatchMetadata(expression.getTag()));
    } else { // request
      builder.setRequestMetadata(getRequestKeyValueMatchMetadata(expression.getTag()));
    }
    return CustomModsecRuleClause.newBuilder().setKeyValueMatchClause(builder).build();
  }

  private CustomModsecMatchExpression convert(MatchOperator operator, String matchValue) {
    return CustomModsecMatchExpression.newBuilder()
        .setValueMatchOperator(convert(operator))
        .setMatchValue(matchValue)
        .build();
  }

  private CustomModsecMatchExpression.MatchOperator convert(MatchOperator operator) {
    switch (operator) {
      case MATCH_OPERATOR_EQUALS:
        return CustomModsecMatchExpression.MatchOperator.MATCH_OPERATOR_EQUALS;
      case MATCH_OPERATOR_NOT_EQUAL:
        return CustomModsecMatchExpression.MatchOperator.MATCH_OPERATOR_NOT_EQUAL;
      case MATCH_OPERATOR_MATCHES_REGEX:
        return CustomModsecMatchExpression.MatchOperator.MATCH_OPERATOR_MATCHES_REGEX;
      case MATCH_OPERATOR_NOT_MATCH_REGEX:
        return CustomModsecMatchExpression.MatchOperator.MATCH_OPERATOR_NOT_MATCH_REGEX;
      case MATCH_OPERATOR_CONTAINS:
        return CustomModsecMatchExpression.MatchOperator.MATCH_OPERATOR_CONTAINS;
      case MATCH_OPERATOR_NOT_CONTAIN:
        return CustomModsecMatchExpression.MatchOperator.MATCH_OPERATOR_NOT_CONTAIN;
      case MATCH_OPERATOR_GREATER_THAN:
        return CustomModsecMatchExpression.MatchOperator.MATCH_OPERATOR_GREATER_THAN;
      case MATCH_OPERATOR_LESS_THAN:
        return CustomModsecMatchExpression.MatchOperator.MATCH_OPERATOR_LESS_THAN;
      default:
        throw new IllegalArgumentException("Unknown match operator: " + operator);
    }
  }

  private Optional<RequestValueMatchMetadata> getRequestValueMatchMetadata(MatchKey key) {
    switch (key) {
      case MATCH_KEY_URL:
        return Optional.of(RequestValueMatchMetadata.REQUEST_VALUE_MATCH_METADATA_URL);
      case MATCH_KEY_HOST:
        return Optional.of(RequestValueMatchMetadata.REQUEST_VALUE_MATCH_METADATA_HOST);
      case MATCH_KEY_HTTP_METHOD:
        return Optional.of(RequestValueMatchMetadata.REQUEST_VALUE_MATCH_METADATA_HTTP_METHOD);
      case MATCH_KEY_USER_AGENT:
        return Optional.of(RequestValueMatchMetadata.REQUEST_VALUE_MATCH_METADATA_USER_AGENT);
      case MATCH_KEY_BODY:
        return Optional.of(RequestValueMatchMetadata.REQUEST_VALUE_MATCH_METADATA_BODY);
      case MATCH_KEY_BODY_SIZE:
        return Optional.of(RequestValueMatchMetadata.REQUEST_VALUE_MATCH_METADATA_BODY_SIZE);
      case MATCH_KEY_QUERY_PARAMS_COUNT:
        return Optional.of(
            RequestValueMatchMetadata.REQUEST_VALUE_MATCH_METADATA_QUERY_PARAMS_COUNT);
      case MATCH_KEY_HEADERS_COUNT:
        return Optional.of(RequestValueMatchMetadata.REQUEST_VALUE_MATCH_METADATA_HEADERS_COUNT);
      case MATCH_KEY_COOKIES_COUNT:
        return Optional.of(RequestValueMatchMetadata.REQUEST_VALUE_MATCH_METADATA_COOKIES_COUNT);
      default:
        return Optional.empty();
    }
  }

  private RequestKeyValueMatchMetadata getRequestKeyValueMatchMetadata(MatchKey key) {
    switch (key) {
      case MATCH_KEY_HEADER_NAME:
      case MATCH_KEY_HEADER_VALUE:
        return RequestKeyValueMatchMetadata.REQUEST_KEY_VALUE_MATCH_METADATA_HEADER;
      case MATCH_KEY_PARAMETER_NAME:
      case MATCH_KEY_PARAMETER_VALUE:
        return RequestKeyValueMatchMetadata.REQUEST_KEY_VALUE_MATCH_METADATA_PARAMETER;
      case MATCH_KEY_QUERY_PARAMETER_NAME:
      case MATCH_KEY_QUERY_PARAMETER_VALUE:
        return RequestKeyValueMatchMetadata.REQUEST_KEY_VALUE_MATCH_METADATA_QUERY_PARAMETER;
      case MATCH_KEY_BODY_PARAMETER_NAME:
      case MATCH_KEY_BODY_PARAMETER_VALUE:
        return RequestKeyValueMatchMetadata.REQUEST_KEY_VALUE_MATCH_METADATA_BODY_PARAMETER;
      case MATCH_KEY_COOKIE_NAME:
      case MATCH_KEY_COOKIE_VALUE:
        return RequestKeyValueMatchMetadata.REQUEST_KEY_VALUE_MATCH_METADATA_COOKIE;
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported key: %s for request key-value match", key));
    }
  }

  private RequestKeyValueMatchMetadata getRequestKeyValueMatchMetadata(KeyValueTag keyValueTag) {
    switch (keyValueTag) {
      case KEY_VALUE_TAG_HEADER:
        return RequestKeyValueMatchMetadata.REQUEST_KEY_VALUE_MATCH_METADATA_HEADER;
      case KEY_VALUE_TAG_PARAMETER:
        return RequestKeyValueMatchMetadata.REQUEST_KEY_VALUE_MATCH_METADATA_PARAMETER;
      case KEY_VALUE_TAG_QUERY_PARAMETER:
        return RequestKeyValueMatchMetadata.REQUEST_KEY_VALUE_MATCH_METADATA_QUERY_PARAMETER;
      case KEY_VALUE_TAG_BODY_PARAMETER:
        return RequestKeyValueMatchMetadata.REQUEST_KEY_VALUE_MATCH_METADATA_BODY_PARAMETER;
      case KEY_VALUE_TAG_COOKIE:
        return RequestKeyValueMatchMetadata.REQUEST_KEY_VALUE_MATCH_METADATA_COOKIE;
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported tag: %s for request key-value match", keyValueTag));
    }
  }

  private Optional<ResponseValueMatchMetadata> getResponseValueMatchMetadata(MatchKey key) {
    switch (key) {
      case MATCH_KEY_STATUS_CODE:
        return Optional.of(ResponseValueMatchMetadata.RESPONSE_VALUE_MATCH_METADATA_STATUS_CODE);
      case MATCH_KEY_BODY:
        return Optional.of(ResponseValueMatchMetadata.RESPONSE_VALUE_MATCH_METADATA_BODY);
      case MATCH_KEY_BODY_SIZE:
        return Optional.of(ResponseValueMatchMetadata.RESPONSE_VALUE_MATCH_METADATA_BODY_SIZE);
      case MATCH_KEY_HEADERS_COUNT:
        return Optional.of(ResponseValueMatchMetadata.RESPONSE_VALUE_MATCH_METADATA_HEADERS_COUNT);
      default:
        return Optional.empty();
    }
  }

  private ResponseKeyValueMatchMetadata getResponseKeyValueMatchMetadata(MatchKey key) {
    switch (key) {
      case MATCH_KEY_HEADER_NAME:
      case MATCH_KEY_HEADER_VALUE:
        return ResponseKeyValueMatchMetadata.RESPONSE_KEY_VALUE_MATCH_METADATA_HEADER;
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported key: %s for response key-value match", key));
    }
  }

  private ResponseKeyValueMatchMetadata getResponseKeyValueMatchMetadata(KeyValueTag keyValueTag) {
    if (Objects.requireNonNull(keyValueTag) == KeyValueTag.KEY_VALUE_TAG_HEADER) {
      return ResponseKeyValueMatchMetadata.RESPONSE_KEY_VALUE_MATCH_METADATA_HEADER;
    }
    throw new IllegalArgumentException(
        String.format("Unsupported tag: %s for response key-value match", keyValueTag));
  }
}
