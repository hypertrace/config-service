package ai.traceable.aiapp.protection.config.service.firewall.cache;

import ai.traceable.data.classification.config.service.v1.DataSetInfo.DataSuppression;
import ai.traceable.data.classification.config.service.v1.DataTypeRule;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.Action;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.KeyValuePattern;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.Location;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.ScopedPattern;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.StringPattern;
import ai.traceable.protection.processing.common.v1.AttributeType;
import ai.traceable.protection.processing.common.v1.MessageType;
import ai.traceable.protection.processing.common.v1.utils.PrefixBuilder;
import ai.traceable.protection.processor.datatype.v1.AttributeFilter;
import ai.traceable.protection.processor.datatype.v1.DataTransformationStrategy;
import ai.traceable.protection.processor.datatype.v1.DataType;
import ai.traceable.protection.processor.datatype.v1.DataTypeMatchRule;
import ai.traceable.protection.processor.datatype.v1.MatchRuleResult;
import ai.traceable.protection.processor.datatype.v1.ObfuscationStrategy;
import ai.traceable.protection.processor.datatype.v1.PathPredicate;
import ai.traceable.protection.processor.datatype.v1.PathPredicateType;
import ai.traceable.protection.processor.datatype.v1.PathValuePredicate;
import ai.traceable.protection.processor.datatype.v1.RawValueStrategy;
import ai.traceable.protection.processor.datatype.v1.RedactionStrategy;
import ai.traceable.protection.processor.datatype.v1.StringPredicate;
import ai.traceable.protection.processor.datatype.v1.StringPredicateOperator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProtectionEngineDataTypeTranslator {

  private static final String REQUEST_HEADER_PREFIX =
      PrefixBuilder.buildAppendablePrefix(
          MessageType.MESSAGE_TYPE_REQUEST, AttributeType.ATTRIBUTE_TYPE_HEADER);
  private static final String RESPONSE_HEADER_PREFIX =
      PrefixBuilder.buildAppendablePrefix(
          MessageType.MESSAGE_TYPE_RESPONSE, AttributeType.ATTRIBUTE_TYPE_HEADER);
  private static final String REQUEST_BODY_PREFIX =
      PrefixBuilder.buildAppendablePrefix(
          MessageType.MESSAGE_TYPE_REQUEST, AttributeType.ATTRIBUTE_TYPE_BODY_PARAM);
  private static final String RESPONSE_BODY_PREFIX =
      PrefixBuilder.buildAppendablePrefix(
          MessageType.MESSAGE_TYPE_RESPONSE, AttributeType.ATTRIBUTE_TYPE_BODY_PARAM);
  private static final String REQUEST_COOKIE_PREFIX =
      PrefixBuilder.buildAppendablePrefix(
          MessageType.MESSAGE_TYPE_REQUEST, AttributeType.ATTRIBUTE_TYPE_COOKIE);
  private static final String RESPONSE_COOKIE_PREFIX =
      PrefixBuilder.buildAppendablePrefix(
          MessageType.MESSAGE_TYPE_RESPONSE, AttributeType.ATTRIBUTE_TYPE_COOKIE);
  private static final String QUERY_PARAM_PREFIX =
      PrefixBuilder.buildAppendablePrefix(
          MessageType.MESSAGE_TYPE_REQUEST, AttributeType.ATTRIBUTE_TYPE_QUERY_PARAM);
  private static final String URL_PREFIX =
      PrefixBuilder.buildAppendablePrefix(
          MessageType.MESSAGE_TYPE_REQUEST, AttributeType.ATTRIBUTE_TYPE_URL);

  private static final List<String> REQUEST_HEADER_PREFIXES = List.of(REQUEST_HEADER_PREFIX);
  private static final List<String> RESPONSE_HEADER_PREFIXES = List.of(RESPONSE_HEADER_PREFIX);
  private static final List<String> REQUEST_BODY_PREFIXES = List.of(REQUEST_BODY_PREFIX);
  private static final List<String> RESPONSE_BODY_PREFIXES = List.of(RESPONSE_BODY_PREFIX);
  private static final List<String> REQUEST_COOKIE_PREFIXES = List.of(REQUEST_COOKIE_PREFIX);
  private static final List<String> RESPONSE_COOKIE_PREFIXES = List.of(RESPONSE_COOKIE_PREFIX);
  private static final List<String> QUERY_PARAM_PREFIXES = List.of(QUERY_PARAM_PREFIX, URL_PREFIX);

  private static final List<String> ANY_LOCATION_PREFIXES =
      ImmutableList.copyOf(
          Iterables.concat(
              REQUEST_HEADER_PREFIXES,
              RESPONSE_HEADER_PREFIXES,
              REQUEST_COOKIE_PREFIXES,
              RESPONSE_COOKIE_PREFIXES,
              QUERY_PARAM_PREFIXES,
              REQUEST_BODY_PREFIXES,
              RESPONSE_BODY_PREFIXES));

  private static final List<String> EMPTY_PREFIXES = List.of();

  List<DataType> translateDataTypes(
      List<ai.traceable.data.classification.config.service.v1.DataType> resolvedDataTypes,
      Optional<String> environmentName) {
    return resolvedDataTypes.stream()
        .map(resolvedDataType -> translateDataType(resolvedDataType, environmentName))
        .flatMap(Optional::stream)
        .collect(Collectors.toUnmodifiableList());
  }

  private Optional<DataType> translateDataType(
      ai.traceable.data.classification.config.service.v1.DataType resolvedDataType,
      Optional<String> environmentName) {
    List<DataTypeMatchRule> matchRules =
        resolvedDataType.getRule().getScopedPatternsList().stream()
            .filter(scopedPattern -> matchScope(scopedPattern, environmentName))
            .map(this::translateScopedPattern)
            .flatMap(Optional::stream)
            .collect(Collectors.toUnmodifiableList());
    if (matchRules.isEmpty()) {
      return Optional.empty();
    }
    DataType.Builder dataTypeBuilder = DataType.newBuilder();
    dataTypeBuilder.setId(resolvedDataType.getId());
    translateTransformationStrategy(resolvedDataType.getRule().getDataSuppression())
        .ifPresent(dataTypeBuilder::setTransformationStrategy);
    if (resolvedDataType.getRule().hasSuppressionPattern()) {
      dataTypeBuilder.setSuppressionPattern(resolvedDataType.getRule().getSuppressionPattern());
    }
    dataTypeBuilder.addAllMatchRules(matchRules);
    return Optional.of(dataTypeBuilder.build());
  }

  private boolean matchScope(ScopedPattern scopedPattern, Optional<String> environmentName) {
    switch (scopedPattern.getScopeCase()) {
      case ENVIRONMENT_SCOPE:
        return environmentName
            .map(
                actualName ->
                    scopedPattern
                        .getEnvironmentScope()
                        .getEnvironmentIdsList()
                        .contains(actualName))
            .orElse(false);
      case API_SCOPE:
        return false;
      case GLOBAL_SCOPE:
      case SCOPE_NOT_SET:
      default:
        return true;
    }
  }

  private Optional<DataTypeMatchRule> translateScopedPattern(ScopedPattern scopedPattern) {
    DataTypeMatchRule.Builder builder = DataTypeMatchRule.newBuilder();
    translateLocations(scopedPattern.getLocationsList()).ifPresent(builder::setAttributeFilter);
    builder.setResult(translateAction(scopedPattern.getAction()));
    switch (scopedPattern.getPatternCase()) {
      case KEY_PATTERN:
        return Optional.of(
            builder
                .setPathPredicate(
                    translatePathPattern(
                        scopedPattern.getKeyPattern(),
                        PathPredicateType.PATH_PREDICATE_TYPE_PATH_SEGMENT))
                .build());
      case KEY_VALUE_PATTERN:
        return Optional.of(
            builder
                .setPathValuePredicate(
                    translatePathValuePattern(
                        scopedPattern.getKeyValuePattern(),
                        PathPredicateType.PATH_PREDICATE_TYPE_PATH_SEGMENT))
                .build());
      case LEAF_KEY_PATTERN:
        return Optional.of(
            builder
                .setPathPredicate(
                    translatePathPattern(
                        scopedPattern.getLeafKeyPattern(),
                        PathPredicateType.PATH_PREDICATE_TYPE_LEAF_PATH_SEGMENT))
                .build());
      case LEAF_KEY_VALUE_PATTERN:
        return Optional.of(
            builder
                .setPathValuePredicate(
                    translatePathValuePattern(
                        scopedPattern.getLeafKeyValuePattern(),
                        PathPredicateType.PATH_PREDICATE_TYPE_LEAF_PATH_SEGMENT))
                .build());
      case PATTERN_NOT_SET:
      default:
        log.error("Unsupported scoped pattern type: {}", scopedPattern);
        return Optional.empty();
    }
  }

  private PathPredicate translatePathPattern(
      StringPattern pathPattern, PathPredicateType predicateType) {
    return PathPredicate.newBuilder()
        .setType(predicateType)
        .setPathSegmentPredicate(translateStringPattern(pathPattern))
        .build();
  }

  private PathValuePredicate translatePathValuePattern(
      KeyValuePattern keyValuePattern, PathPredicateType predicateType) {
    return PathValuePredicate.newBuilder()
        .setPathPredicate(translatePathPattern(keyValuePattern.getKeyPattern(), predicateType))
        .setValuePredicate(translateStringPattern(keyValuePattern.getValuePattern()))
        .build();
  }

  private StringPredicate translateStringPattern(StringPattern pattern) {
    return StringPredicate.newBuilder()
        .setValue(pattern.getValue())
        .setOperator(translateOperator(pattern.getOperator()))
        .build();
  }

  private StringPredicateOperator translateOperator(DataTypeRule.Operator operator) {
    switch (operator) {
      case OPERATOR_EQUALS:
        return StringPredicateOperator.STRING_PREDICATE_OPERATOR_EQUALS;
      case OPERATOR_MATCHES_REGEX:
        return StringPredicateOperator.STRING_PREDICATE_OPERATOR_REGEX;
      case OPERATOR_UNSPECIFIED:
      case UNRECOGNIZED:
      default:
        return StringPredicateOperator.STRING_PREDICATE_OPERATOR_UNSPECIFIED;
    }
  }

  private MatchRuleResult translateAction(Action action) {
    switch (action) {
      case ACTION_MATCH:
        return MatchRuleResult.MATCH_RULE_RESULT_MATCH;
      case ACTION_IGNORE:
        return MatchRuleResult.MATCH_RULE_RESULT_IGNORE;
      case ACTION_UNSPECIFIED:
      case UNRECOGNIZED:
      default:
        return MatchRuleResult.MATCH_RULE_RESULT_UNSPECIFIED;
    }
  }

  private Optional<AttributeFilter> translateLocations(List<Location> locations) {
    List<String> prefixes =
        locations.stream()
            .map(this::translateLocation)
            .flatMap(Collection::stream)
            .distinct()
            .collect(Collectors.toUnmodifiableList());
    if (prefixes.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(AttributeFilter.newBuilder().addAllPrefixes(prefixes).build());
  }

  private List<String> translateLocation(Location location) {
    switch (location) {
      case LOCATION_REQUEST_HEADER:
        return REQUEST_HEADER_PREFIXES;
      case LOCATION_RESPONSE_HEADER:
        return RESPONSE_HEADER_PREFIXES;
      case LOCATION_REQUEST_BODY:
        return REQUEST_BODY_PREFIXES;
      case LOCATION_RESPONSE_BODY:
        return RESPONSE_BODY_PREFIXES;
      case LOCATION_REQUEST_COOKIE:
        return REQUEST_COOKIE_PREFIXES;
      case LOCATION_RESPONSE_COOKIE:
        return RESPONSE_COOKIE_PREFIXES;
      case LOCATION_QUERY:
        return QUERY_PARAM_PREFIXES;
      case LOCATION_ANY:
        return ANY_LOCATION_PREFIXES;
      case LOCATION_PATH:
      case LOCATION_UNSPECIFIED:
      case UNRECOGNIZED:
      default:
        log.error("Received unsupported location for translation: {}", location);
        return EMPTY_PREFIXES;
    }
  }

  Optional<DataTransformationStrategy> translateTransformationStrategy(
      DataSuppression dataSuppression) {
    switch (dataSuppression) {
      case DATA_SUPPRESSION_REDACT:
        return Optional.of(
            DataTransformationStrategy.newBuilder()
                .setRedactionStrategy(RedactionStrategy.getDefaultInstance())
                .build());
      case DATA_SUPPRESSION_OBFUSCATE:
        return Optional.of(
            DataTransformationStrategy.newBuilder()
                .setObfuscationStrategy(ObfuscationStrategy.getDefaultInstance())
                .build());
      case DATA_SUPPRESSION_RAW:
        return Optional.of(
            DataTransformationStrategy.newBuilder()
                .setRawValueStrategy(RawValueStrategy.getDefaultInstance())
                .build());
      case DATA_SUPPRESSION_UNSPECIFIED:
        return Optional.empty();
      case UNRECOGNIZED:
      default:
        log.error("Received unsupported data suppression mode: {}", dataSuppression);
        return Optional.empty();
    }
  }
}
