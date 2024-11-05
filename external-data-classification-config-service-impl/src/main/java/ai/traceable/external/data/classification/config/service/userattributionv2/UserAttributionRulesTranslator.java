package ai.traceable.external.data.classification.config.service.userattributionv2;

import static ai.traceable.external.data.classification.config.service.userattributionv2.UserAttributionConstants.END_USER_ID_ATTRIBUTE_KEY;
import static ai.traceable.external.data.classification.config.service.userattributionv2.UserAttributionConstants.END_USER_ROLE_ATTRIBUTE_KEY;
import static ai.traceable.external.data.classification.config.service.userattributionv2.UserAttributionConstants.RULE_ATTRIBUTE_KEY_SUFFIX;
import static ai.traceable.external.data.classification.config.service.v1.Operator.OPERATOR_EQUALS;
import static ai.traceable.userattribution.config.service.v2.ObfuscationStrategy.OBFUSCATION_STRATEGY_HASH;

import ai.traceable.external.data.classification.config.service.v1.AttributeFilter;
import ai.traceable.external.data.classification.config.service.v1.AttributePredicate;
import ai.traceable.external.data.classification.config.service.v1.DataType;
import ai.traceable.external.data.classification.config.service.v1.DataType.DataTypeMatchRule;
import ai.traceable.external.data.classification.config.service.v1.PathPredicate;
import ai.traceable.external.data.classification.config.service.v1.SpanFilter;
import ai.traceable.external.data.classification.config.service.v1.StringPredicate;
import ai.traceable.userattribution.config.service.v2.UserAttributionRule;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class UserAttributionRulesTranslator {

  public List<DataType> translateUserAttributionRules(
      List<UserAttributionRule> userAttributionRules) {
    Stream<DataType> userIdDataTypes = getObfuscatedUserIdRuleIds(userAttributionRules);
    Stream<DataType> userRoleDataTypes = getObfuscatedUserRoleRuleIds(userAttributionRules);
    Stream<DataType> customTokenDataTypes = getObfuscatedCustomTokenRuleIds(userAttributionRules);
    return Stream.of(userIdDataTypes, userRoleDataTypes, customTokenDataTypes)
        .flatMap(item -> item)
        .collect(Collectors.toUnmodifiableList());
  }

  private Stream<DataType> getObfuscatedUserIdRuleIds(
      List<UserAttributionRule> userAttributionRules) {
    return userAttributionRules.stream()
        .filter(
            rule ->
                rule.getData().hasUserIdRule()
                    && rule.getData()
                        .getUserIdRule()
                        .getTokenObfuscationStrategy()
                        .equals(OBFUSCATION_STRATEGY_HASH))
        .map(rule -> createDataTypeRule(END_USER_ID_ATTRIBUTE_KEY, rule.getId()));
  }

  private Stream<DataType> getObfuscatedUserRoleRuleIds(
      List<UserAttributionRule> userAttributionRules) {
    return userAttributionRules.stream()
        .filter(
            rule ->
                rule.getData().hasUserRoleRule()
                    && rule.getData()
                        .getUserRoleRule()
                        .getTokenObfuscationStrategy()
                        .equals(OBFUSCATION_STRATEGY_HASH))
        .map(rule -> createDataTypeRule(END_USER_ROLE_ATTRIBUTE_KEY, rule.getId()));
  }

  private Stream<DataType> getObfuscatedCustomTokenRuleIds(
      List<UserAttributionRule> userAttributionRules) {
    return userAttributionRules.stream()
        .flatMap(this::getCustomTokenRulesStream)
        .map(entry -> createDataTypeRule(entry.getKey(), entry.getValue()));
  }

  private Stream<Map.Entry<String, String>> getCustomTokenRulesStream(UserAttributionRule rule) {
    return rule.getData().getCustomTokenRulesMap().entrySet().stream()
        .filter(
            entry ->
                entry.getValue().getTokenObfuscationStrategy().equals(OBFUSCATION_STRATEGY_HASH))
        .map(entry -> Map.entry(entry.getKey(), rule.getId()));
  }

  private DataType createDataTypeRule(String attributeKey, String ruleId) {
    // This rule represent obfuscation of attributes added by user attribution rules
    StringPredicate.Builder attributeKeyRuleIdKeyPredicate =
        StringPredicate.newBuilder()
            .setValue(attributeKey + RULE_ATTRIBUTE_KEY_SUFFIX)
            .setOperator(OPERATOR_EQUALS);
    StringPredicate.Builder attributeKeyRuleIdValuePredicate =
        StringPredicate.newBuilder().setValue(ruleId).setOperator(OPERATOR_EQUALS);
    StringPredicate.Builder attributeKeyPredicate =
        StringPredicate.newBuilder().setValue(attributeKey).setOperator(OPERATOR_EQUALS);
    return DataType.newBuilder()
        .setDataTypeId(ruleId)
        .setTransformation(DataType.DataTransformation.DATA_TRANSFORMATION_OBFUSCATE)
        .addMatchRules(
            DataTypeMatchRule.newBuilder()
                .setSpanFilter(
                    SpanFilter.newBuilder()
                        .addRequiredMatchingAttributes(
                            AttributePredicate.newBuilder()
                                .setNamePredicate(attributeKeyRuleIdKeyPredicate)
                                .setValuePredicate(attributeKeyRuleIdValuePredicate)))
                .setAttributeFilter(AttributeFilter.newBuilder().addPrefixes(attributeKey))
                .setResult(DataType.Result.RESULT_MATCH)
                .setPathPredicate(
                    PathPredicate.newBuilder().setPathSegmentPredicate(attributeKeyPredicate)))
        .build();
  }
}
