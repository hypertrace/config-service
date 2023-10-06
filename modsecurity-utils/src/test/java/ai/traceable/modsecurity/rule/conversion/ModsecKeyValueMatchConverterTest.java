package ai.traceable.modsecurity.rule.conversion;

import static ai.traceable.modsecurity.rule.api.v1.CustomModsecMatchExpression.MatchOperator.MATCH_OPERATOR_CONTAINS;
import static ai.traceable.modsecurity.rule.api.v1.CustomModsecMatchExpression.MatchOperator.MATCH_OPERATOR_EQUALS;
import static ai.traceable.modsecurity.rule.api.v1.CustomModsecMatchExpression.MatchOperator.MATCH_OPERATOR_MATCHES_REGEX;
import static ai.traceable.modsecurity.rule.api.v1.CustomModsecMatchExpression.MatchOperator.MATCH_OPERATOR_NOT_CONTAIN;
import static ai.traceable.modsecurity.rule.api.v1.CustomModsecMatchExpression.MatchOperator.MATCH_OPERATOR_NOT_EQUAL;
import static ai.traceable.modsecurity.rule.api.v1.CustomModsecMatchExpression.MatchOperator.MATCH_OPERATOR_NOT_MATCH_REGEX;
import static ai.traceable.modsecurity.rule.api.v1.RequestKeyValueMatchMetadata.REQUEST_KEY_VALUE_MATCH_METADATA_BODY_PARAMETER;
import static ai.traceable.modsecurity.rule.api.v1.RequestKeyValueMatchMetadata.REQUEST_KEY_VALUE_MATCH_METADATA_COOKIE;
import static ai.traceable.modsecurity.rule.api.v1.RequestKeyValueMatchMetadata.REQUEST_KEY_VALUE_MATCH_METADATA_HEADER;
import static ai.traceable.modsecurity.rule.api.v1.RequestKeyValueMatchMetadata.REQUEST_KEY_VALUE_MATCH_METADATA_PARAMETER;
import static ai.traceable.modsecurity.rule.api.v1.RequestKeyValueMatchMetadata.REQUEST_KEY_VALUE_MATCH_METADATA_QUERY_PARAMETER;
import static ai.traceable.modsecurity.rule.api.v1.ResponseKeyValueMatchMetadata.RESPONSE_KEY_VALUE_MATCH_METADATA_HEADER;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.modsecurity.rule.api.v1.CustomModsecKeyValueMatchClause;
import ai.traceable.modsecurity.rule.api.v1.CustomModsecMatchExpression;
import ai.traceable.modsecurity.rule.api.v1.CustomModsecRule;
import ai.traceable.modsecurity.rule.api.v1.CustomModsecRuleClause;
import ai.traceable.modsecurity.rule.api.v1.RequestKeyValueMatchMetadata;
import ai.traceable.modsecurity.rule.api.v1.ResponseKeyValueMatchMetadata;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ModsecKeyValueMatchConverterTest {

  private static final long RULE_ID_SEED = 200001;
  private static final UuidGenerator uuidGenerator = new UuidGenerator();

  static List<CustomModsecRule> getSampleCustomModsecRules() {
    List<CustomModsecRule> customModsecRules = new ArrayList<>();

    String key = "food";
    String keyRegex = "(fruit|vegetable)";
    String value = "ca";
    String valueRegex = "(p|r)[a-z]+";

    List<CustomModsecMatchExpression.MatchOperator> operators =
        List.of(
            MATCH_OPERATOR_EQUALS,
            MATCH_OPERATOR_NOT_EQUAL,
            MATCH_OPERATOR_MATCHES_REGEX,
            MATCH_OPERATOR_NOT_MATCH_REGEX,
            MATCH_OPERATOR_CONTAINS,
            MATCH_OPERATOR_NOT_CONTAIN);

    List<CustomModsecMatchExpression.MatchOperator> regexOperators =
        List.of(MATCH_OPERATOR_MATCHES_REGEX, MATCH_OPERATOR_NOT_MATCH_REGEX);

    List<RequestKeyValueMatchMetadata> requestMetadata =
        List.of(
            REQUEST_KEY_VALUE_MATCH_METADATA_HEADER,
            REQUEST_KEY_VALUE_MATCH_METADATA_PARAMETER,
            REQUEST_KEY_VALUE_MATCH_METADATA_QUERY_PARAMETER,
            REQUEST_KEY_VALUE_MATCH_METADATA_BODY_PARAMETER,
            REQUEST_KEY_VALUE_MATCH_METADATA_COOKIE);

    customModsecRules.addAll(
        createCustomModsecRequestKeyValueMatchRules(requestMetadata, key, value, operators));
    customModsecRules.addAll(
        createCustomModsecRequestKeyValueMatchRules(
            requestMetadata, keyRegex, valueRegex, regexOperators));
    customModsecRules.addAll(
        createCustomModsecResponseKeyValueMatchRules(
            List.of(RESPONSE_KEY_VALUE_MATCH_METADATA_HEADER), key, value, operators));
    customModsecRules.addAll(
        createCustomModsecResponseKeyValueMatchRules(
            List.of(RESPONSE_KEY_VALUE_MATCH_METADATA_HEADER),
            keyRegex,
            valueRegex,
            regexOperators));

    customModsecRules.sort(Comparator.comparing(CustomModsecRule::toString));
    AtomicLong ruleId = new AtomicLong(RULE_ID_SEED);
    return customModsecRules.stream()
        .map(rule -> rule.toBuilder().setRuleId(ruleId.getAndIncrement()).build())
        .collect(Collectors.toList());
  }

  private static List<CustomModsecRule> createCustomModsecRequestKeyValueMatchRules(
      List<RequestKeyValueMatchMetadata> requestMetadata,
      String key,
      String value,
      List<CustomModsecMatchExpression.MatchOperator> matchOperators) {
    return requestMetadata.stream()
        .flatMap(
            metadata ->
                matchOperators.stream()
                    .flatMap(
                        op1 ->
                            Stream.concat(
                                Stream.of(
                                    createCustomModsecRule(metadata, null, null, op1, value),
                                    createCustomModsecRule(metadata, op1, key, null, null)),
                                matchOperators.stream()
                                    .map(
                                        op2 ->
                                            createCustomModsecRule(
                                                metadata, op1, key, op2, value)))))
        .collect(Collectors.toList());
  }

  private static List<CustomModsecRule> createCustomModsecResponseKeyValueMatchRules(
      List<ResponseKeyValueMatchMetadata> responseMetadata,
      String key,
      String value,
      List<CustomModsecMatchExpression.MatchOperator> matchOperators) {
    return responseMetadata.stream()
        .flatMap(
            metadata ->
                matchOperators.stream()
                    .flatMap(
                        op1 ->
                            Stream.concat(
                                Stream.of(
                                    createCustomModsecRule(metadata, null, null, op1, value),
                                    createCustomModsecRule(metadata, op1, key, null, null)),
                                matchOperators.stream()
                                    .map(
                                        op2 ->
                                            createCustomModsecRule(
                                                metadata, op1, key, op2, value)))))
        .collect(Collectors.toList());
  }

  private static CustomModsecRule createCustomModsecRule(
      RequestKeyValueMatchMetadata requestMetadata,
      CustomModsecMatchExpression.MatchOperator keyOperator,
      String key,
      CustomModsecMatchExpression.MatchOperator valueOperator,
      String value) {
    String ruleMsg =
        String.join(
            ":",
            requestMetadata.name(),
            keyOperator == null ? "KEY_NULL" : "KEY_" + keyOperator,
            valueOperator == null ? "VALUE_NULL" : "VALUE_" + valueOperator);
    CustomModsecKeyValueMatchClause.Builder clauseBuilder =
        CustomModsecKeyValueMatchClause.newBuilder().setRequestMetadata(requestMetadata);
    return createCustomModsecRule(clauseBuilder, ruleMsg, keyOperator, key, valueOperator, value);
  }

  private static CustomModsecRule createCustomModsecRule(
      ResponseKeyValueMatchMetadata responseMetadata,
      CustomModsecMatchExpression.MatchOperator keyOperator,
      String key,
      CustomModsecMatchExpression.MatchOperator valueOperator,
      String value) {
    String ruleMsg =
        String.join(
            ":",
            responseMetadata.name(),
            keyOperator == null ? "KEY_NULL" : "KEY_" + keyOperator,
            valueOperator == null ? "VALUE_NULL" : "VALUE_" + valueOperator);
    CustomModsecKeyValueMatchClause.Builder clauseBuilder =
        CustomModsecKeyValueMatchClause.newBuilder().setResponseMetadata(responseMetadata);
    return createCustomModsecRule(clauseBuilder, ruleMsg, keyOperator, key, valueOperator, value);
  }

  private static CustomModsecRule createCustomModsecRule(
      CustomModsecKeyValueMatchClause.Builder clauseBuilderWithMetadata,
      String ruleMsg,
      CustomModsecMatchExpression.MatchOperator keyOperator,
      String key,
      CustomModsecMatchExpression.MatchOperator valueOperator,
      String value) {
    String ruleUuid = uuidGenerator.generateId(ruleMsg);
    if (keyOperator != null) {
      clauseBuilderWithMetadata.setKeyMatchExpression(
          CustomModsecMatchExpression.newBuilder()
              .setValueMatchOperator(keyOperator)
              .setMatchValue(key));
    }
    if (valueOperator != null) {
      clauseBuilderWithMetadata.setValueMatchExpression(
          CustomModsecMatchExpression.newBuilder()
              .setValueMatchOperator(valueOperator)
              .setMatchValue(value));
    }
    return CustomModsecRule.newBuilder()
        .setRuleMsg(ruleMsg)
        .setRuleUuid(ruleUuid)
        .addAndClauses(
            CustomModsecRuleClause.newBuilder().setKeyValueMatchClause(clauseBuilderWithMetadata))
        .build();
  }
}
