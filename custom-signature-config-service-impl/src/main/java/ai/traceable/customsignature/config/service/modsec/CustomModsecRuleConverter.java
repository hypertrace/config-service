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
import ai.traceable.modsecurity.rule.api.v1.RequestKeyValueMatchMetadata;
import ai.traceable.modsecurity.rule.api.v1.RequestValueMatchMetadata;
import ai.traceable.modsecurity.rule.api.v1.ResponseKeyValueMatchMetadata;
import ai.traceable.modsecurity.rule.api.v1.ResponseValueMatchMetadata;
import ai.traceable.modsecurity.rule.conversion.ModsecRuleConverter;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.inject.Inject;

public class CustomModsecRuleConverter {

  private final ModsecRuleConverter modsecRuleConverter;

  @Inject
  public CustomModsecRuleConverter(ModsecRuleConverter modsecRuleConverter) {
    this.modsecRuleConverter = modsecRuleConverter;
  }

  public String getValidatedModsecRule(
      long ruleId, String ruleUuid, String ruleMsg, List<Clause> clauses) throws Exception {
    return modsecRuleConverter.getModsecRule(
        createCustomModsecRule(ruleId, ruleUuid, ruleMsg, clauses, Optional.empty()));
  }

  public String getValidatedModsecRuleWithCustomLogMsg(
      long ruleId, String ruleUuid, String ruleMsg, List<Clause> clauses, String logMsg)
      throws Exception {
    return modsecRuleConverter.getModsecRule(
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
      default:
        throw new IllegalArgumentException("Unknown clause case: " + clause.getClauseCase());
    }
  }

  private CustomModsecRuleClause convert(MatchExpression expression) {
    CustomModsecRuleClause.Builder clauseBuilder = CustomModsecRuleClause.newBuilder();
    CustomModsecMatchExpression matchExpression =
        convert(expression.getMatchOperator(), expression.getMatchValue());

    Optional<CustomModsecValueMatchClause> valueMatchClause =
        getRequestValueMatchMetadata(expression.getMatchCategory(), expression.getMatchKey())
            .map(
                metadata ->
                    CustomModsecValueMatchClause.newBuilder().setRequestValueMetadata(metadata))
            .or(
                () ->
                    getResponseValueMatchMetadata(
                            expression.getMatchCategory(), expression.getMatchKey())
                        .map(
                            metadata ->
                                CustomModsecValueMatchClause.newBuilder()
                                    .setResponseValueMetadata(metadata)))
            .map(builder -> builder.setValueMatchExpression(matchExpression).build());

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

    switch (expression.getMatchCategory()) {
      case MATCH_CATEGORY_RESPONSE:
        builder.setResponseMetadata(getResponseKeyValueMatchMetadata(expression.getMatchKey()));
        break;
      default: // request
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

    switch (expression.getMatchCategory()) {
      case MATCH_CATEGORY_RESPONSE:
        builder.setResponseMetadata(getResponseKeyValueMatchMetadata(expression.getTag()));
        break;
      default: // request
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

  private Optional<RequestValueMatchMetadata> getRequestValueMatchMetadata(
      MatchCategory category, MatchKey key) {
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
        if (!category.equals(MatchCategory.MATCH_CATEGORY_RESPONSE)) {
          return Optional.of(RequestValueMatchMetadata.REQUEST_VALUE_MATCH_METADATA_BODY);
        }
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

  private Optional<ResponseValueMatchMetadata> getResponseValueMatchMetadata(
      MatchCategory category, MatchKey key) {
    switch (key) {
      case MATCH_KEY_STATUS_CODE:
        return Optional.of(ResponseValueMatchMetadata.RESPONSE_VALUE_MATCH_METADATA_STATUS_CODE);
      case MATCH_KEY_BODY:
        if (category.equals(MatchCategory.MATCH_CATEGORY_RESPONSE)) {
          return Optional.of(ResponseValueMatchMetadata.RESPONSE_VALUE_MATCH_METADATA_BODY);
        }
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
    switch (keyValueTag) {
      case KEY_VALUE_TAG_HEADER:
        return ResponseKeyValueMatchMetadata.RESPONSE_KEY_VALUE_MATCH_METADATA_HEADER;
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported tag: %s for response key-value match", keyValueTag));
    }
  }
}
