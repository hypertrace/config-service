package ai.traceable.external.data.classification.config.service;

import static ai.traceable.external.data.classification.config.service.v1.Operator.*;

import ai.traceable.external.data.classification.config.service.v1.AttributeFilter;
import ai.traceable.external.data.classification.config.service.v1.AttributePredicate;
import ai.traceable.external.data.classification.config.service.v1.DataType;
import ai.traceable.external.data.classification.config.service.v1.DataType.DataTransformation;
import ai.traceable.external.data.classification.config.service.v1.DataType.DataTypeMatchRule;
import ai.traceable.external.data.classification.config.service.v1.DataType.Result;
import ai.traceable.external.data.classification.config.service.v1.Operator;
import ai.traceable.external.data.classification.config.service.v1.PathPredicate;
import ai.traceable.external.data.classification.config.service.v1.PathValuePredicate;
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
    switch (redactionRule.getMatchType()) {
      case MATCH_TYPE_VALUE:
        dataTypeMatchRuleBuilder.setPathValuePredicate(
            PathValuePredicate.newBuilder()
                .setValuePredicate(
                    this.buildStringPredicate(OPERATOR_MATCHES_REGEX, redactionRule.getRegex())));
        break;
      default:
        dataTypeMatchRuleBuilder.setPathPredicate(
            PathPredicate.newBuilder()
                .setPathSegmentPredicate(
                    this.buildStringPredicate(OPERATOR_MATCHES_REGEX, redactionRule.getRegex())));
        break;
    }

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
      spanFilterBuilder.addRequiredMatchingAttributes(
          AttributePredicate.newBuilder()
              .setNamePredicate(buildStringPredicate(OPERATOR_EQUALS, attributeRegexMatch.getKey()))
              .setValuePredicate(
                  buildStringPredicate(OPERATOR_MATCHES_REGEX, attributeRegexMatch.getRegex())));
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
      case REDACTION_STRATEGY_RAW:
      case UNRECOGNIZED:
      case REDACTION_STRATEGY_UNSPECIFIED:
      default:
        return Optional.empty();
    }
  }

  private StringPredicate buildStringPredicate(Operator operator, String value) {
    return StringPredicate.newBuilder().setValue(value).setOperator(operator).build();
  }
}
