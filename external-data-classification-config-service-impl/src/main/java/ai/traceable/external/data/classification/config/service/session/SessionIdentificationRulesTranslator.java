package ai.traceable.external.data.classification.config.service.session;

import ai.traceable.external.data.classification.config.service.v1.AttributeFilter;
import ai.traceable.external.data.classification.config.service.v1.AttributePredicate;
import ai.traceable.external.data.classification.config.service.v1.DataType;
import ai.traceable.external.data.classification.config.service.v1.DataType.DataTypeMatchRule;
import ai.traceable.external.data.classification.config.service.v1.Operator;
import ai.traceable.external.data.classification.config.service.v1.PathPredicate;
import ai.traceable.external.data.classification.config.service.v1.SpanFilter;
import ai.traceable.external.data.classification.config.service.v1.StringPredicate;
import ai.traceable.sessionidentification.config.service.v1.MatchCondition;
import ai.traceable.sessionidentification.config.service.v1.ObfuscationStrategy;
import ai.traceable.sessionidentification.config.service.v1.RequestAttributeKeyLocation;
import ai.traceable.sessionidentification.config.service.v1.ResponseAttributeKeyLocation;
import ai.traceable.sessionidentification.config.service.v1.SessionIdentificationRule;
import ai.traceable.sessionidentification.config.service.v1.SessionTokenRule;
import ai.traceable.sessionidentification.config.service.v1.ValueProjection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class SessionIdentificationRulesTranslator {
  private static final List<String> URL_KEYS = List.of("http.url", "http.target");
  private static final Map<RequestAttributeKeyLocation, List<String>>
      REQUEST_ATTRIBUTE_KEY_LOCATION_LIST_MAP =
          Map.of(
              RequestAttributeKeyLocation.REQUEST_ATTRIBUTE_KEY_LOCATION_HEADER,
              List.of("http.request.header", "rpc.request.metadata"),
              RequestAttributeKeyLocation.REQUEST_ATTRIBUTE_KEY_LOCATION_COOKIE,
              List.of("http.request.header.cookie"),
              RequestAttributeKeyLocation.REQUEST_ATTRIBUTE_KEY_LOCATION_BODY,
              List.of("http.request.body", "rpc.request.body"),
              RequestAttributeKeyLocation.REQUEST_ATTRIBUTE_KEY_LOCATION_QUERY_PARAMETER,
              List.of("http.url"));

  private static final Map<ResponseAttributeKeyLocation, List<String>>
      RESPONSE_ATTRIBUTE_KEY_LOCATION_LIST_MAP =
          Map.of(
              ResponseAttributeKeyLocation.RESPONSE_ATTRIBUTE_KEY_LOCATION_HEADER,
              List.of("http.response.header", "rpc.response.metadata"),
              ResponseAttributeKeyLocation.RESPONSE_ATTRIBUTE_KEY_LOCATION_COOKIE,
              List.of("http.response.header.set-cookie"),
              ResponseAttributeKeyLocation.RESPONSE_ATTRIBUTE_KEY_LOCATION_BODY,
              List.of("http.response.body", "rpc.response.body"));

  SessionIdentificationConstants sessionIdentificationConstants;
  MatchConditionTranslator matchConditionTranslator;

  public List<DataType> translateSessionIdentificationRules(
      List<SessionIdentificationRule> sessionIdentificationRules, Optional<String> envName) {
    return sessionIdentificationRules.stream()
        .filter(rule -> matchEnvScope(rule, envName))
        .map(this::translateSessionIdentificationRule)
        .flatMap(Collection::stream)
        .collect(Collectors.toUnmodifiableList());
  }

  private boolean matchEnvScope(SessionIdentificationRule rule, Optional<String> environmentName) {
    return rule.getScope().getEnvironmentNamesList().isEmpty()
        || environmentName.isEmpty()
        || (rule.getScope().getEnvironmentNamesList().contains(environmentName.get()));
  }

  private List<DataType> translateSessionIdentificationRule(SessionIdentificationRule rule) {
    String ruleId = rule.getId();
    int ruleIndex = 0;
    List<DataType> dataTypeList = new ArrayList<>();
    for (SessionTokenRule tokenRule : rule.getTokenRulesList()) {
      if (tokenRule.getTokenValueRule().getValueObfuscationStrategy()
          == ObfuscationStrategy.OBFUSCATION_STRATEGY_HASH) {
        dataTypeList.addAll(
            translateTokenRule(
                tokenRule,
                ruleId,
                ruleIndex,
                rule.getScope().getServiceNameRegexesList(),
                rule.getScope().getUrlMatchRegexesList()));
      }
      ruleIndex++;
    }
    return dataTypeList;
  }

  private List<DataType> translateTokenRule(
      SessionTokenRule tokenRule,
      String ruleId,
      int ruleIndex,
      List<String> serviceNameRegexes,
      List<String> urlMatchRegexes) {
    AttributeFilter.Builder attributeFilterBuilder = AttributeFilter.newBuilder();
    String sessionIdKey;
    if (tokenRule.hasRequestSessionTokenDetails()) {
      sessionIdKey = sessionIdentificationConstants.buildKeyForSessionId(ruleId, ruleIndex);
      attributeFilterBuilder.addAllPrefixes(
          getRequestAttributeLocation(
              tokenRule.getRequestSessionTokenDetails().getTokenLocation()));
    } else {
      sessionIdKey = sessionIdentificationConstants.buildKeyForNewSessionId(ruleId, ruleIndex);

      attributeFilterBuilder.addAllPrefixes(
          getResponseAttributeLocation(
              tokenRule.getResponseSessionTokenDetails().getTokenLocation()));
    }
    StringPredicate.Builder stringPredicateBuilderForSessionId =
        StringPredicate.newBuilder().setValue(sessionIdKey).setOperator(Operator.OPERATOR_EQUALS);
    List<AttributePredicate> requiredMatchingAttributeList =
        new ArrayList<>(
            List.of(
                AttributePredicate.newBuilder()
                    .setNamePredicate(stringPredicateBuilderForSessionId)
                    .build()));

    // add service name regexes list to span filter
    if (!serviceNameRegexes.isEmpty()) {
      String serviceNameRegexValue = String.join("|", serviceNameRegexes);
      requiredMatchingAttributeList.add(
          AttributePredicate.newBuilder()
              .setNamePredicate(
                  StringPredicate.newBuilder()
                      .setValue("service.name")
                      .setOperator(Operator.OPERATOR_EQUALS))
              .setValuePredicate(
                  StringPredicate.newBuilder()
                      .setOperator(Operator.OPERATOR_MATCHES_REGEX)
                      .setValue(serviceNameRegexValue))
              .build());
    }

    // add url match regex list to span filter
    if (!urlMatchRegexes.isEmpty()) {
      String urlMatchRegexValue = String.join("|", urlMatchRegexes);
      List<SpanFilter> spanFilterList = new ArrayList<>();
      URL_KEYS.forEach(
          key ->
              spanFilterList.add(
                  SpanFilter.newBuilder()
                      .addAllRequiredMatchingAttributes(requiredMatchingAttributeList)
                      .addRequiredMatchingAttributes(
                          AttributePredicate.newBuilder()
                              .setNamePredicate(
                                  StringPredicate.newBuilder()
                                      .setValue(key)
                                      .setOperator(Operator.OPERATOR_EQUALS))
                              .setValuePredicate(
                                  StringPredicate.newBuilder()
                                      .setOperator(Operator.OPERATOR_MATCHES_REGEX)
                                      .setValue(urlMatchRegexValue))
                              .build())
                      .build()));
      return List.of(
          getDataTypeForRawValue(ruleId, spanFilterList, attributeFilterBuilder, tokenRule),
          getDataTypeForExtractedValue(ruleId, stringPredicateBuilderForSessionId, sessionIdKey));
    }

    return List.of(
        getDataTypeForRawValue(
            ruleId,
            List.of(
                SpanFilter.newBuilder()
                    .addAllRequiredMatchingAttributes(requiredMatchingAttributeList)
                    .build()),
            attributeFilterBuilder,
            tokenRule),
        getDataTypeForExtractedValue(ruleId, stringPredicateBuilderForSessionId, sessionIdKey));
  }

  private Optional<String> supressionPatternBuilder(List<ValueProjection> valueProjectionList) {
    if (!valueProjectionList.isEmpty() && valueProjectionList.get(0).hasRegexCaptureGroup()) {
      return Optional.of(valueProjectionList.get(0).getRegexCaptureGroup().getRegexCaptureGroup());
    } else if (valueProjectionList.size() > 1
        && valueProjectionList.get(0).hasJsonPath()
        && valueProjectionList.get(1).hasRegexCaptureGroup()) {
      return Optional.of(valueProjectionList.get(1).getRegexCaptureGroup().getRegexCaptureGroup());
    }
    return Optional.empty();
  }

  private Optional<PathPredicate> pathPredicateBuilder(SessionTokenRule tokenRule) {
    if (tokenRule.getRequestSessionTokenDetails().getTokenLocation()
            == RequestAttributeKeyLocation.REQUEST_ATTRIBUTE_KEY_LOCATION_COOKIE
        || tokenRule.getRequestSessionTokenDetails().getTokenLocation()
            == RequestAttributeKeyLocation.REQUEST_ATTRIBUTE_KEY_LOCATION_HEADER
        || tokenRule.getRequestSessionTokenDetails().getTokenLocation()
            == RequestAttributeKeyLocation.REQUEST_ATTRIBUTE_KEY_LOCATION_QUERY_PARAMETER
        || tokenRule.getResponseSessionTokenDetails().getTokenLocation()
            == ResponseAttributeKeyLocation.RESPONSE_ATTRIBUTE_KEY_LOCATION_COOKIE
        || tokenRule.getResponseSessionTokenDetails().getTokenLocation()
            == ResponseAttributeKeyLocation.RESPONSE_ATTRIBUTE_KEY_LOCATION_HEADER) {
      MatchCondition matchCondition =
          tokenRule
              .getTokenValueRule()
              .getTokenValueProjection()
              .getAttributeProjection()
              .getAttributeKeyMatchCondition();
      return Optional.of(
          PathPredicate.newBuilder()
              .setPathSegmentPredicate(matchConditionTranslator.translate(matchCondition))
              .build());
    }

    List<ValueProjection> projections =
        tokenRule
            .getTokenValueRule()
            .getTokenValueProjection()
            .getAttributeProjection()
            .getValueProjectionsInOrderList();
    if (!projections.isEmpty() && projections.get(0).hasJsonPath()) {
      return Optional.of(
          PathPredicate.newBuilder()
              .setJsonPath(projections.get(0).getJsonPath().getPath())
              .build());
    }
    return Optional.empty();
  }

  private List<String> getResponseAttributeLocation(ResponseAttributeKeyLocation location) {
    return RESPONSE_ATTRIBUTE_KEY_LOCATION_LIST_MAP.get(location);
  }

  private List<String> getRequestAttributeLocation(RequestAttributeKeyLocation location) {
    return REQUEST_ATTRIBUTE_KEY_LOCATION_LIST_MAP.get(location);
  }

  private DataType getDataTypeForRawValue(
      String ruleId,
      List<SpanFilter> spanFilterList,
      AttributeFilter.Builder attributeFilterBuilder,
      SessionTokenRule tokenRule) {
    List<ValueProjection> valueProjectionList =
        tokenRule
            .getTokenValueRule()
            .getTokenValueProjection()
            .getAttributeProjection()
            .getValueProjectionsInOrderList();
    DataTypeMatchRule.Builder matchRuleBuilder =
        DataTypeMatchRule.newBuilder()
            .setResult(DataType.Result.RESULT_MATCH)
            .setAttributeFilter(attributeFilterBuilder);
    pathPredicateBuilder(tokenRule).ifPresent(matchRuleBuilder::setPathPredicate);
    DataType.Builder dataTypeBuilderForRawValue =
        // This rule represent obfuscation of session id so we don't need to specifically set the
        // session_identifier
        DataType.newBuilder()
            .setDataTypeId(ruleId)
            .setTransformation(DataType.DataTransformation.DATA_TRANSFORMATION_OBFUSCATE)
            .addAllMatchRules(
                spanFilterList.stream()
                    .map(spanFilter -> matchRuleBuilder.setSpanFilter(spanFilter).build())
                    .collect(Collectors.toUnmodifiableList()));
    supressionPatternBuilder(valueProjectionList)
        .ifPresent(dataTypeBuilderForRawValue::setSuppressionPattern);
    return dataTypeBuilderForRawValue.build();
  }

  private DataType getDataTypeForExtractedValue(
      String ruleId, StringPredicate.Builder predicate, String sessionIdKey) {
    // This rule represent obfuscation of session id so we don't need to specifically set the
    // session_identifier
    return DataType.newBuilder()
        .setDataTypeId(ruleId)
        .setTransformation(DataType.DataTransformation.DATA_TRANSFORMATION_OBFUSCATE)
        .addMatchRules(
            DataTypeMatchRule.newBuilder()
                .setSpanFilter(
                    SpanFilter.newBuilder()
                        .addRequiredMatchingAttributes(
                            AttributePredicate.newBuilder().setNamePredicate(predicate)))
                .setAttributeFilter(AttributeFilter.newBuilder().addPrefixes(sessionIdKey))
                .setResult(DataType.Result.RESULT_MATCH)
                .setPathPredicate(PathPredicate.newBuilder().setPathSegmentPredicate(predicate)))
        .build();
  }
}
