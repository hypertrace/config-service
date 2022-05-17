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
import ai.traceable.sensitivedata.config.service.v1.Parameter;
import ai.traceable.sensitivedata.config.service.v1.RedactionRule;
import ai.traceable.sensitivedata.config.service.v1.RedactionStrategy;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class RedactionRulesTranslator {

  // this should be in sync with id in PiiFilterConfigServiceImpl in sensitive data config service
  // impl and id in RedactionRulesDao in data classification config service impl
  private static final String LEGACY_SENSITIVE_HEADERS_DATA_TYPE_ID =
      "legacy-datatype-sensitive-headers-id";
  private static final List<String> SENSITIVE_HEADERS_PREFIXES_LIST =
      List.of(
          "rpc.response.metadata",
          "http.response.header",
          "rpc.request.metadata",
          "http.request.header");

  public List<DataType> translateRedactionRules(List<RedactionRule> redactionRules) {
    return redactionRules.stream()
        .map(this::translateRedactionRule)
        .collect(Collectors.toUnmodifiableList());
  }

  public Optional<DataType> translateDataTypeForSensitiveHeaders(
      List<Parameter> sensitiveHeaderParameters, RedactionStrategy redactionStrategy) {
    Optional<DataTransformation> maybeDataTransformation =
        translateRedactionStrategyForHeaders(redactionStrategy);
    List<DataTypeMatchRule> matchRules =
        sensitiveHeaderParameters.stream()
            .map(Parameter::getName)
            .distinct()
            .map(this::translateHeaderNameToMatchRule)
            .collect(Collectors.toUnmodifiableList());
    return maybeDataTransformation
        .filter(transformation -> !matchRules.isEmpty())
        .map(
            transformation ->
                DataType.newBuilder()
                    .setDataTypeId(LEGACY_SENSITIVE_HEADERS_DATA_TYPE_ID)
                    .setTransformation(transformation)
                    .addAllMatchRules(matchRules)
                    .build());
  }

  private Optional<DataTransformation> translateRedactionStrategyForHeaders(
      RedactionStrategy redactionStrategy) {
    switch (redactionStrategy) {
      case REDACTION_STRATEGY_HASH:
        return Optional.of(DataTransformation.DATA_TRANSFORMATION_OBFUSCATE);
      case REDACTION_STRATEGY_REDACT:
        return Optional.of(DataTransformation.DATA_TRANSFORMATION_REDACT);
      case REDACTION_STRATEGY_RAW:
        return Optional.empty();
      case REDACTION_STRATEGY_UNSPECIFIED:
      default:
        log.error(
            "This redaction strategy is not supported for sensitive headers! {}",
            redactionStrategy);
        return Optional.empty();
    }
  }

  private DataTypeMatchRule translateHeaderNameToMatchRule(String name) {
    return DataTypeMatchRule.newBuilder()
        .setResult(Result.RESULT_MATCH)
        .setPathPredicate(
            PathPredicate.newBuilder()
                .setPathSegmentPredicate(
                    StringPredicate.newBuilder().setValue(name).setOperator(OPERATOR_EQUALS)))
        .setAttributeFilter(
            AttributeFilter.newBuilder().addAllPrefixes(SENSITIVE_HEADERS_PREFIXES_LIST))
        .build();
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
