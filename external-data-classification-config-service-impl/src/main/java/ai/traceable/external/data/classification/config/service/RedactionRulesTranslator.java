package ai.traceable.external.data.classification.config.service;

import ai.traceable.external.data.classification.config.service.v1.AttributeFilter;
import ai.traceable.external.data.classification.config.service.v1.DataType;
import ai.traceable.external.data.classification.config.service.v1.DataType.DataTransformation;
import ai.traceable.external.data.classification.config.service.v1.DataType.DataTypeMatchRule;
import ai.traceable.external.data.classification.config.service.v1.DataType.Result;
import ai.traceable.external.data.classification.config.service.v1.KeyValuePredicate;
import ai.traceable.external.data.classification.config.service.v1.Operator;
import ai.traceable.external.data.classification.config.service.v1.SpanFilter;
import ai.traceable.external.data.classification.config.service.v1.StringPredicate;
import ai.traceable.sensitivedata.config.service.v1.Condition;
import ai.traceable.sensitivedata.config.service.v1.Condition.AttributeRegexMatch;
import ai.traceable.sensitivedata.config.service.v1.RedactionRule;
import ai.traceable.sensitivedata.config.service.v1.RedactionStrategy;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

class RedactionRulesTranslator {

  public List<DataType> translateRedactionRules(List<RedactionRule> redactionRules) {
    return redactionRules.stream()
        .map(this::translateRedactionRule)
        .collect(Collectors.toUnmodifiableList());
  }

  private DataType translateRedactionRule(RedactionRule redactionRule) {
    DataType.Builder dataTypeBuilder =
        DataType.newBuilder()
            .setDataTypeId(redactionRule.getId())
            .setSessionIdentifier(redactionRule.getSessionIdentifier());
    Optional<DataTransformation> maybeDataTransformation =
        translateRedactionStrategy(redactionRule.getRedactionStrategy());
    maybeDataTransformation.ifPresent(dataTypeBuilder::setTransformation);
    return dataTypeBuilder.addMatchRules(getTranslatedDataTypeMatchRule(redactionRule)).build();
  }

  private DataTypeMatchRule getTranslatedDataTypeMatchRule(RedactionRule redactionRule) {
    DataTypeMatchRule.Builder dataTypeMatchRuleBuilder = DataTypeMatchRule.newBuilder();
    dataTypeMatchRuleBuilder.setResult(Result.RESULT_MATCH);
    dataTypeMatchRuleBuilder.setKeyPredicate(
        StringPredicate.newBuilder()
            .setValue(redactionRule.getRegex())
            .setOperator(Operator.OPERATOR_MATCHES_REGEX));
    SpanFilter.Builder spanFilterBuilder = SpanFilter.newBuilder();
    AttributeFilter.Builder attributeFilterBuilder = AttributeFilter.newBuilder();
    for (Condition condition : redactionRule.getConditionsList()) {
      if (!condition.hasAttributeRegexMatch()) {
        continue;
      }
      AttributeRegexMatch attributeRegexMatch = condition.getAttributeRegexMatch();
      if (!redactionRule.getFqn()) {
        attributeFilterBuilder.addPrefixes(attributeRegexMatch.getKey());
      }
      StringPredicate keyPredicate =
          StringPredicate.newBuilder()
              .setOperator(Operator.OPERATOR_EQUALS)
              .setValue(attributeRegexMatch.getKey())
              .build();
      StringPredicate valuePredicate =
          StringPredicate.newBuilder()
              .setOperator(Operator.OPERATOR_MATCHES_REGEX)
              .setValue(attributeRegexMatch.getRegex())
              .build();
      spanFilterBuilder.addRequiredMatchingAttributes(
          KeyValuePredicate.newBuilder()
              .setKeyPredicate(keyPredicate)
              .setValuePredicate(valuePredicate));
    }
    return dataTypeMatchRuleBuilder
        .setSpanFilter(spanFilterBuilder)
        .setAttributeFilter(attributeFilterBuilder)
        .build();
  }

  private Optional<DataTransformation> translateRedactionStrategy(RedactionStrategy strategy) {
    switch (strategy) {
      case REDACTION_STRATEGY_REDACT:
        return Optional.of(DataTransformation.DATA_TRANSFORMATION_REDACT);
      case REDACTION_STRATEGY_HASH:
        return Optional.of(DataTransformation.DATA_TRANSFORMATION_OBFUSCATE);
      default:
        return Optional.empty();
    }
  }
}
