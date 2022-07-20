package ai.traceable.external.data.classification.config.service;

import static ai.traceable.external.data.classification.config.service.v1.GetDataClassificationConfigRequest.PredicateSupportLevel.PREDICATE_SUPPORT_LEVEL_LEAF_PATH_SEGMENT;

import ai.traceable.data.classification.config.service.v1.DataSetInfo.DataSuppression;
import ai.traceable.data.classification.config.service.v1.DataTypeRule;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.Action;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.KeyValuePattern;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.Location;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.ScopedPattern;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.StringPattern;
import ai.traceable.external.data.classification.config.service.v1.AttributeFilter;
import ai.traceable.external.data.classification.config.service.v1.DataType;
import ai.traceable.external.data.classification.config.service.v1.DataType.DataTransformation;
import ai.traceable.external.data.classification.config.service.v1.DataType.DataTypeMatchRule;
import ai.traceable.external.data.classification.config.service.v1.DataType.Result;
import ai.traceable.external.data.classification.config.service.v1.GetDataClassificationConfigRequest.PredicateSupportLevel;
import ai.traceable.external.data.classification.config.service.v1.Operator;
import ai.traceable.external.data.classification.config.service.v1.PathPredicate;
import ai.traceable.external.data.classification.config.service.v1.PathValuePredicate;
import ai.traceable.external.data.classification.config.service.v1.StringPredicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class DataClassificationRulesTranslator {
  private static final String HTTP_REQUEST_HEADER = "http.request.header";
  private static final String RPC_REQUEST_METADATA = "rpc.request.metadata";
  private static final String HTTP_RESPONSE_HEADER = "http.response.header";
  private static final String RPC_RESPONSE_METADATA = "rpc.response.metadata";
  private static final String HTTP_URL = "http.url";
  private static final String HTTP_TARGET = "http.target";
  private static final String HTTP_REQUEST_BODY = "http.request.body";
  private static final String HTTP_RESPONSE_BODY = "http.response.body";
  private static final String RPC_REQUEST_BODY = "rpc.request.body";
  private static final String RPC_RESPONSE_BODY = "rpc.response.body";
  private static final String HTTP_REQUEST_COOKIE = "http.request.header.cookie";
  private static final String HTTP_RESPONSE_COOKIE = "http.response.header.set-cookie";
  private static final List<String> REQUEST_BODY_PREFIXES_LIST =
      List.of(HTTP_REQUEST_BODY, RPC_REQUEST_BODY);
  private static final List<String> RESPONSE_BODY_PREFIXES_LIST =
      List.of(HTTP_RESPONSE_BODY, RPC_RESPONSE_BODY);
  private static final List<String> REQUEST_HEADERS_PREFIXES_LIST =
      List.of(HTTP_REQUEST_HEADER, RPC_REQUEST_METADATA);
  private static final List<String> RESPONSE_HEADERS_PREFIXES_LIST =
      List.of(HTTP_RESPONSE_HEADER, RPC_RESPONSE_METADATA);
  // The agent does not break down the query params to separate attributes, instead the url is
  // matched by a data parsing rule which breaks down the query params into child keys
  private static final List<String> HTTP_URL_LIST = List.of(HTTP_URL, HTTP_TARGET);

  // Any location really means any of the other defined locations rather than any possible location
  private static final List<String> ANY_LOCATION_PREFIXES_LIST =
      ImmutableList.copyOf(
          Iterables.concat(
              REQUEST_HEADERS_PREFIXES_LIST,
              RESPONSE_HEADERS_PREFIXES_LIST,
              List.of(HTTP_REQUEST_COOKIE),
              List.of(HTTP_RESPONSE_COOKIE),
              HTTP_URL_LIST,
              REQUEST_BODY_PREFIXES_LIST,
              RESPONSE_BODY_PREFIXES_LIST));
  private static final List<String> EMPTY_PREFIXES_LIST = List.of();

  List<DataType> translateDataTypes(
      List<ai.traceable.data.classification.config.service.v1.DataType> dataTypes,
      Map<String, DataSuppression> dataTypesToDataSuppressionMap,
      Optional<String> environmentName,
      PredicateSupportLevel predicateSupportLevel) {
    return dataTypes.stream()
        .map(
            dataType ->
                translateDataType(
                    dataType,
                    dataTypesToDataSuppressionMap.get(dataType.getId()),
                    environmentName,
                    predicateSupportLevel))
        .flatMap(Optional::stream)
        .collect(Collectors.toUnmodifiableList());
  }

  private Optional<DataType> translateDataType(
      ai.traceable.data.classification.config.service.v1.DataType dataType,
      DataSuppression dataSuppression,
      Optional<String> environmentName,
      PredicateSupportLevel predicateSupportLevel) {
    DataType.Builder dataTypeBuilder = DataType.newBuilder();
    dataTypeBuilder.setDataTypeId(dataType.getId());
    DataTypeRule rule = dataType.getRule();
    if (rule.hasSuppressionPattern()) {
      dataTypeBuilder.setSuppressionPattern(rule.getSuppressionPattern());
    }
    translateDataSuppression(dataSuppression).ifPresent(dataTypeBuilder::setTransformation);
    List<DataTypeMatchRule> matchRules = new ArrayList<>();
    rule.getScopedPatternsList()
        .forEach(
            scopedPattern -> {
              if (environmentName.isEmpty()
                  || scopedPattern.hasGlobalScope()
                  || (scopedPattern.hasEnvironmentScope()
                      && scopedPattern
                          .getEnvironmentScope()
                          .getEnvironmentIdsList()
                          .contains(environmentName.get()))) {
                translateScopedPattern(scopedPattern, predicateSupportLevel)
                    .ifPresent(matchRules::add);
              }
            });
    if (matchRules.isEmpty()) {
      return Optional.empty();
    }
    dataTypeBuilder.addAllMatchRules(matchRules);
    return Optional.of(dataTypeBuilder.build());
  }

  private Optional<DataTypeMatchRule> translateScopedPattern(
      ScopedPattern scopedPattern, PredicateSupportLevel predicateSupportLevel) {
    DataTypeMatchRule.Builder dataTypeMatchRuleBuilder = DataTypeMatchRule.newBuilder();
    // environment filter is already taken care of.
    // TODO may need to support API scope in the future.
    translateLocations(scopedPattern.getLocationsList())
        .ifPresent(dataTypeMatchRuleBuilder::setAttributeFilter);
    dataTypeMatchRuleBuilder.setResult(translateAction(scopedPattern.getAction()));
    switch (scopedPattern.getPatternCase()) {
      case KEY_PATTERN:
        return Optional.of(
            dataTypeMatchRuleBuilder
                .setPathPredicate(translatePathPattern(scopedPattern.getKeyPattern()))
                .build());
      case KEY_VALUE_PATTERN:
        return Optional.of(
            dataTypeMatchRuleBuilder
                .setPathValuePredicate(
                    translatePathValuePattern(scopedPattern.getKeyValuePattern()))
                .build());
      case LEAF_KEY_VALUE_PATTERN:
        if (isLeafKeyValuePatternSupported(predicateSupportLevel)) {
          return Optional.of(
              dataTypeMatchRuleBuilder
                  .setPathValuePredicate(
                      translateLeafPathValuePattern(scopedPattern.getLeafKeyValuePattern()))
                  .build());
        } else {
          return Optional.of(
              dataTypeMatchRuleBuilder
                  .setPathValuePredicate(
                      translatePathValuePattern(scopedPattern.getLeafKeyValuePattern()))
                  .build());
        }
      case PATTERN_NOT_SET:
      default:
        log.error("Unsupported scoped pattern type: {}", scopedPattern);
        return Optional.empty();
    }
  }

  private boolean isLeafKeyValuePatternSupported(PredicateSupportLevel predicateSupportLevel) {
    return predicateSupportLevel.equals(PREDICATE_SUPPORT_LEVEL_LEAF_PATH_SEGMENT);
  }

  private PathValuePredicate translatePathValuePattern(KeyValuePattern keyValuePattern) {
    return PathValuePredicate.newBuilder()
        .setPathPredicate(translatePathPattern(keyValuePattern.getKeyPattern()))
        .setValuePredicate(translateStringPattern(keyValuePattern.getValuePattern()))
        .build();
  }

  private PathValuePredicate translateLeafPathValuePattern(KeyValuePattern keyValuePattern) {
    return PathValuePredicate.newBuilder()
        .setPathPredicate(translateLeafPathPattern(keyValuePattern.getKeyPattern()))
        .setValuePredicate(translateStringPattern(keyValuePattern.getValuePattern()))
        .build();
  }

  private StringPredicate translateStringPattern(StringPattern keyPattern) {
    return StringPredicate.newBuilder()
        .setValue(keyPattern.getValue())
        .setOperator(translateOperator(keyPattern.getOperator()))
        .build();
  }

  private PathPredicate translatePathPattern(StringPattern pathPattern) {
    return PathPredicate.newBuilder()
        .setPathSegmentPredicate(this.translateStringPattern(pathPattern))
        .build();
  }

  private PathPredicate translateLeafPathPattern(StringPattern pathPattern) {
    return PathPredicate.newBuilder()
        .setLeafPathSegmentPredicate(this.translateStringPattern(pathPattern))
        .build();
  }

  private Operator translateOperator(DataTypeRule.Operator operator) {
    switch (operator) {
      case OPERATOR_EQUALS:
        return Operator.OPERATOR_EQUALS;
      case OPERATOR_MATCHES_REGEX:
        return Operator.OPERATOR_MATCHES_REGEX;
      case OPERATOR_UNSPECIFIED:
      case UNRECOGNIZED:
      default:
        return Operator.OPERATOR_UNSPECIFIED;
    }
  }

  private Result translateAction(Action action) {
    switch (action) {
      case ACTION_MATCH:
        return Result.RESULT_MATCH;
      case ACTION_IGNORE:
        return Result.RESULT_IGNORE;
      case ACTION_UNSPECIFIED:
      case UNRECOGNIZED:
      default:
        return Result.RESULT_UNSPECIFIED;
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
        return REQUEST_HEADERS_PREFIXES_LIST;
      case LOCATION_RESPONSE_HEADER:
        return RESPONSE_HEADERS_PREFIXES_LIST;
      case LOCATION_REQUEST_COOKIE:
        return List.of(HTTP_REQUEST_COOKIE);
      case LOCATION_RESPONSE_COOKIE:
        return List.of(HTTP_RESPONSE_COOKIE);
      case LOCATION_QUERY:
        return HTTP_URL_LIST;
      case LOCATION_REQUEST_BODY:
        return REQUEST_BODY_PREFIXES_LIST;
      case LOCATION_RESPONSE_BODY:
        return RESPONSE_BODY_PREFIXES_LIST;
      case LOCATION_ANY:
        return ANY_LOCATION_PREFIXES_LIST;
      case LOCATION_PATH:
      case LOCATION_UNSPECIFIED:
      case UNRECOGNIZED:
      default:
        log.error("Received unsupported location for translation: {}", location);
        return EMPTY_PREFIXES_LIST;
    }
  }

  private Optional<DataTransformation> translateDataSuppression(DataSuppression dataSuppression) {
    switch (dataSuppression) {
      case DATA_SUPPRESSION_REDACT:
        return Optional.of(DataTransformation.DATA_TRANSFORMATION_REDACT);
      case DATA_SUPPRESSION_OBFUSCATE:
        return Optional.of(DataTransformation.DATA_TRANSFORMATION_OBFUSCATE);
      case DATA_SUPPRESSION_RAW:
      case UNRECOGNIZED:
      case DATA_SUPPRESSION_UNSPECIFIED:
      default:
        return Optional.empty();
    }
  }
}
