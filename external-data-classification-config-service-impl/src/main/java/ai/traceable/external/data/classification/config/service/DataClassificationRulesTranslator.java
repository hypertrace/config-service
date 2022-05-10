package ai.traceable.external.data.classification.config.service;

import ai.traceable.data.classification.config.service.v1.DataSetInfo.DataSuppression;
import ai.traceable.data.classification.config.service.v1.DataTypeRule;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.Action;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.KeyValuePattern;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.Location;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.ScopedPattern;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.StringPattern;
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class DataClassificationRulesTranslator {
  private static final StringPredicate KEY_PREDICATE_FOR_ENVIRONMENT_SCOPE =
      StringPredicate.newBuilder()
          .setOperator(Operator.OPERATOR_EQUALS)
          .setValue("deployment.environment")
          .build();
  private static final String SEPARATOR = ".";
  private static final String HTTP_REQUEST_HEADER = "http.request.header";
  private static final String RPC_REQUEST_METADATA = "rpc.request.metadata";
  private static final String HTTP_RESPONSE_HEADER = "http.response.header";
  private static final String RPC_RESPONSE_METADATA = "rpc.response.metadata";
  private static final String HTTP_REQUEST_QUERY_PARAM = "http.request.query.param";
  private static final String HTTP_REQUEST_BODY = "http.request.body";
  private static final String HTTP_RESPONSE_BODY = "http.response.body";
  private static final String RPC_REQUEST_BODY = "rpc.request.body";
  private static final String RPC_RESPONSE_BODY = "rpc.response.body";
  private static final String HTTP_REQUEST_COOKIE = "http.request.cookie";
  private static final String HTTP_RESPONSE_COOKIE = "http.response.cookie";
  private static final List<String> REQUEST_BODY_PREFIXES_LIST =
      List.of(HTTP_REQUEST_BODY, RPC_REQUEST_BODY);
  private static final List<String> RESPONSE_BODY_PREFIXES_LIST =
      List.of(HTTP_RESPONSE_BODY, RPC_RESPONSE_BODY);
  private static final List<String> REQUEST_HEADERS_PREFIXES_LIST =
      List.of(HTTP_REQUEST_HEADER + SEPARATOR, RPC_REQUEST_METADATA + SEPARATOR);
  private static final List<String> RESPONSE_HEADERS_PREFIXES_LIST =
      List.of(HTTP_RESPONSE_HEADER + SEPARATOR, RPC_RESPONSE_METADATA + SEPARATOR);

  // Any location really means any of the other defined locations rather than any possible location
  private static final List<String> ANY_LOCATION_PREFIXES_LIST =
      ImmutableList.copyOf(
          Iterables.concat(
              REQUEST_HEADERS_PREFIXES_LIST,
              RESPONSE_HEADERS_PREFIXES_LIST,
              List.of(HTTP_REQUEST_COOKIE),
              List.of(HTTP_RESPONSE_COOKIE),
              List.of(HTTP_REQUEST_QUERY_PARAM),
              REQUEST_BODY_PREFIXES_LIST,
              RESPONSE_BODY_PREFIXES_LIST));
  private static final List<String> EMPTY_PREFIXES_LIST = List.of("");

  List<DataType> translateDataTypes(
      Map<String, ai.traceable.data.classification.config.service.v1.DataType> dataTypesToIdMap,
      Map<String, DataSuppression> dataTypesToDataSuppressionMap) {
    List<DataType> translatedDataTypes = new ArrayList<>();
    Set<String> translatedDataTypesIds = new HashSet<>();
    for (String dataTypeId : dataTypesToIdMap.keySet()) {
      if (translatedDataTypesIds.contains(dataTypeId)) {
        continue;
      }
      translatedDataTypes.add(
          translateDataType(
              dataTypesToIdMap.get(dataTypeId), dataTypesToDataSuppressionMap.get(dataTypeId)));
      translatedDataTypesIds.add(dataTypeId);
    }
    return translatedDataTypes;
  }

  private DataType translateDataType(
      ai.traceable.data.classification.config.service.v1.DataType dataType,
      DataSuppression dataSuppression) {
    DataType.Builder dataTypeBuilder = DataType.newBuilder();
    dataTypeBuilder.setDataTypeId(dataType.getId());
    translateDataSuppression(dataSuppression).ifPresent(dataTypeBuilder::setTransformation);
    List<DataTypeMatchRule> matchRules = new ArrayList<>();
    dataType
        .getRule()
        .getScopedPatternsList()
        .forEach(scopedPattern -> matchRules.add(translateScopedPattern(scopedPattern)));
    dataTypeBuilder.addAllMatchRules(matchRules);
    return dataTypeBuilder.build();
  }

  private DataTypeMatchRule translateScopedPattern(ScopedPattern scopedPattern) {
    DataTypeMatchRule.Builder dataTypeMatchRuleBuilder = DataTypeMatchRule.newBuilder();
    // scope is mapped to span_filter
    translateScope(scopedPattern).ifPresent(dataTypeMatchRuleBuilder::setSpanFilter);
    translateLocations(scopedPattern.getLocationsList())
        .ifPresent(dataTypeMatchRuleBuilder::setAttributeFilter);
    dataTypeMatchRuleBuilder.setResult(translateAction(scopedPattern.getAction()));
    switch (scopedPattern.getPatternCase()) {
      case KEY_PATTERN:
        return dataTypeMatchRuleBuilder
            .setPathPredicate(translatePathPattern(scopedPattern.getKeyPattern()))
            .build();
      case KEY_VALUE_PATTERN:
        return dataTypeMatchRuleBuilder
            .setPathValuePredicate(translatePathValuePattern(scopedPattern.getKeyValuePattern()))
            .build();
      case PATTERN_NOT_SET:
      default:
        log.error("Unsupported scoped pattern type: {}", scopedPattern);
        return dataTypeMatchRuleBuilder.build();
    }
  }

  private PathValuePredicate translatePathValuePattern(KeyValuePattern keyValuePattern) {
    return PathValuePredicate.newBuilder()
        .setPathPredicate(translatePathPattern(keyValuePattern.getKeyPattern()))
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
    if (locations.contains(Location.LOCATION_ANY)) {
      return Optional.empty();
    }
    List<String> prefixes = new ArrayList<>();
    locations.forEach(location -> prefixes.addAll(translateLocation(location)));
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
        return List.of(HTTP_REQUEST_QUERY_PARAM);
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

  private Optional<SpanFilter> translateScope(ScopedPattern scopedPattern) {
    // TODO Api scope is not supported
    if (scopedPattern.hasEnvironmentScope()
        && !scopedPattern.getEnvironmentScope().getEnvironmentIdsList().isEmpty()) {
      SpanFilter.Builder spanFilterBuilder = SpanFilter.newBuilder();
      String regex_for_all_environment_ids =
          buildRegexForEnvironmentIds(scopedPattern.getEnvironmentScope().getEnvironmentIdsList());
      StringPredicate valuePredicate =
          StringPredicate.newBuilder()
              .setOperator(Operator.OPERATOR_MATCHES_REGEX)
              .setValue(regex_for_all_environment_ids)
              .build();
      spanFilterBuilder.addRequiredMatchingAttributes(
          AttributePredicate.newBuilder()
              .setNamePredicate(KEY_PREDICATE_FOR_ENVIRONMENT_SCOPE)
              .setValuePredicate(valuePredicate)
              .build());
      return Optional.of(spanFilterBuilder.build());
    }
    return Optional.empty();
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

  private String buildRegexForEnvironmentIds(List<String> environmentIdsList) {
    String regex =
        environmentIdsList.stream()
            .map(this::escapeSpecialCharsForRegex)
            .collect(Collectors.joining("|"));
    return "(" + regex + ")";
  }

  private String escapeSpecialCharsForRegex(String original) {
    // escapes all the special chars for regex control
    return original.replaceAll(
        "[\\<\\(\\[\\{\\\\\\^\\-\\=\\$\\!\\|\\]\\}\\)\\?\\*\\+\\.\\>]", "\\\\$0");
  }
}
