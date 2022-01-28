package ai.traceable.sensitivedata.config.service;

import static ai.traceable.data.classification.config.service.v1.DataSetInfo.DataSuppression.DATA_SUPPRESSION_OBFUSCATE;
import static ai.traceable.data.classification.config.service.v1.DataSetInfo.DataSuppression.DATA_SUPPRESSION_REDACT;
import static java.util.function.Function.identity;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.data.classification.config.service.v1.DataSet;
import ai.traceable.data.classification.config.service.v1.DataSetInfo.DataSuppression;
import ai.traceable.data.classification.config.service.v1.DataType;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.ScopedPattern;
import ai.traceable.sensitivedata.config.service.v1.ComplexData;
import ai.traceable.sensitivedata.config.service.v1.GetPiiFilterConfigRequest;
import ai.traceable.sensitivedata.config.service.v1.GetPiiFilterConfigResponse;
import ai.traceable.sensitivedata.config.service.v1.ParamType;
import ai.traceable.sensitivedata.config.service.v1.Parameter;
import ai.traceable.sensitivedata.config.service.v1.PiiElement;
import ai.traceable.sensitivedata.config.service.v1.PiiFilterConfig;
import ai.traceable.sensitivedata.config.service.v1.PiiFilterConfigServiceGrpc;
import ai.traceable.sensitivedata.config.service.v1.RedactionRule;
import ai.traceable.sensitivedata.config.service.v1.RedactionStrategy;
import com.google.common.collect.Lists;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class PiiFilterConfigServiceImpl extends PiiFilterConfigServiceGrpc.PiiFilterConfigServiceImplBase {

  private static final String SEPARATOR = ".";
  private static final String HTTP_REQUEST_HEADER = "http.request.header";
  private static final String RPC_REQUEST_METADATA = "rpc.request.metadata";
  private static final String HTTP_RESPONSE_HEADER = "http.response.header";
  private static final String RPC_RESPONSE_METADATA = "rpc.response.metadata";
  private static final List<String> REQUEST_HEADERS_PREFIXES_LIST =
      List.of(HTTP_REQUEST_HEADER + SEPARATOR, RPC_REQUEST_METADATA + SEPARATOR);
  private static final List<String> RESPONSE_HEADERS_PREFIXES_LIST =
      List.of(HTTP_RESPONSE_HEADER + SEPARATOR, RPC_RESPONSE_METADATA + SEPARATOR);
  private static final List<String> EMPTY_PREFIXES_LIST = List.of("");

  private final ConfigServiceCoordinator configServiceCoordinator;
  private final InsightsServiceCoordinator insightsServiceCoordinator;
  private final PiiFilterConfig defaultPiiFilterConfig;
  private final UuidGenerator uuidGenerator;

  @Inject
  PiiFilterConfigServiceImpl(
      SensitiveDataServiceConfig sensitiveDataServiceConfig,
      ConfigServiceCoordinator configServiceCoordinator,
      InsightsServiceCoordinator insightsServiceCoordinator,
      UuidGenerator uuidGenerator) {
    this.configServiceCoordinator = configServiceCoordinator;
    this.insightsServiceCoordinator = insightsServiceCoordinator;
    this.defaultPiiFilterConfig = sensitiveDataServiceConfig.defaultPiiFilterConfig();
    this.uuidGenerator = uuidGenerator;
  }

  @Override
  public void getPiiFilterConfig(
      GetPiiFilterConfigRequest request,
      StreamObserver<GetPiiFilterConfigResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();

      Map<String, PiiElement> keyRegexToPiiElementMap = new LinkedHashMap<>();
      Map<String, PiiElement> valueRegexToPiiElementMap = new LinkedHashMap<>();
      Map<String, ComplexData> complexDataMap = new LinkedHashMap<>();

      List<RedactionRule> redactionRules =
          Lists.reverse(
              configServiceCoordinator.getRedactionRules(
                  requestContext, request.getIncludeConditionalRules()));

      mergeConfigFromRedactionRules(
          redactionRules, keyRegexToPiiElementMap, valueRegexToPiiElementMap, complexDataMap);

      // get pii elements from sensitive headers and add them to key regexs
      List<Parameter> sensitiveHeaderParameters =
          insightsServiceCoordinator.getSensitiveHeaderParameters(requestContext);
      RedactionStrategy redactionStrategy =
          configServiceCoordinator.getParamTypeRedactionStrategy(
              requestContext, ParamType.PARAM_TYPE_HEADER);
      List<PiiElement> piiElements =
          computePiiElements(sensitiveHeaderParameters, redactionStrategy);
      addPiiElements(keyRegexToPiiElementMap, piiElements);
      // complex data should be included always as it is also needed by default local processing PII
      // rules
      addComplexDataElements(complexDataMap, defaultPiiFilterConfig.getComplexDataList());

      // if automatic secret redaction is enabled, merge config from default config
      boolean automaticSecretRedactionStrategyEnabled =
          configServiceCoordinator.isAutomaticSecretRedactionStrategyEnabled(requestContext);
      if (automaticSecretRedactionStrategyEnabled) {
        addPiiElements(keyRegexToPiiElementMap, defaultPiiFilterConfig.getKeyRegexsList());
        addPiiElements(valueRegexToPiiElementMap, defaultPiiFilterConfig.getValueRegexsList());
      }

      Map<DataSuppression, Set<DataType>> dataTypesMap =
          getDataTypesForRedactionOrObfuscation(requestContext);
      Set<DataType> dataTypesForRedaction =
          dataTypesMap.getOrDefault(DATA_SUPPRESSION_REDACT, Collections.emptySet());
      Set<DataType> dataTypesForObfuscation =
          dataTypesMap.getOrDefault(DATA_SUPPRESSION_OBFUSCATE, Collections.emptySet());
      mergeConfigFromDataTypes(
          dataTypesForRedaction,
          RedactionStrategy.REDACTION_STRATEGY_REDACT,
          keyRegexToPiiElementMap);
      mergeConfigFromDataTypes(
          dataTypesForObfuscation,
          RedactionStrategy.REDACTION_STRATEGY_HASH,
          keyRegexToPiiElementMap);

      PiiFilterConfig resultingPiiFilterConfig =
          PiiFilterConfig.newBuilder()
              .addAllPrefixes(defaultPiiFilterConfig.getPrefixesList())
              .setRedactionStrategy(defaultPiiFilterConfig.getRedactionStrategy())
              .addAllKeyRegexs(keyRegexToPiiElementMap.values())
              .addAllValueRegexs(valueRegexToPiiElementMap.values())
              .addAllComplexData(complexDataMap.values())
              .setInvalidJsonPolicy(configServiceCoordinator.getInvalidJsonPolicy(requestContext))
              .build();

      String responseHash = uuidGenerator.generateId(resultingPiiFilterConfig);
      GetPiiFilterConfigResponse.Builder responseBuilder =
          GetPiiFilterConfigResponse.newBuilder().setHash(responseHash);
      if (!responseHash.equals(request.getHash())) {
        responseBuilder.setPiiFilterConfig(resultingPiiFilterConfig);
      }
      responseObserver.onNext(responseBuilder.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Get PII Filter Config RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  private void addPiiElements(
      Map<String, PiiElement> piiElementsMap, List<PiiElement> piiElementsToAdd) {
    for (PiiElement piiElementToAdd : piiElementsToAdd) {
      piiElementsMap.putIfAbsent(piiElementToAdd.getRegex(), piiElementToAdd);
    }
  }

  private List<PiiElement> computePiiElements(
      List<Parameter> sensitiveParameters, RedactionStrategy redactionStrategy) {
    if (redactionStrategy == RedactionStrategy.REDACTION_STRATEGY_RAW) {
      return Collections.emptyList();
    }
    Set<String> sensitiveParameterNames =
        sensitiveParameters.stream().map(Parameter::getName).collect(Collectors.toSet());
    List<PiiElement> piiElements = new ArrayList<>();
    for (String name : sensitiveParameterNames) {
      PiiElement piiElement =
          PiiElement.newBuilder()
              .setRegex(name)
              .setRedactionStrategy(redactionStrategy)
              .setFqn(true)
              .build();
      piiElements.add(piiElement);
    }
    return piiElements;
  }

  private void addComplexDataElements(
      Map<String, ComplexData> complexDataMap, List<ComplexData> complexDataElementsToAdd) {
    for (ComplexData complexData : complexDataElementsToAdd) {
      complexDataMap.putIfAbsent(complexData.getKey(), complexData);
    }
  }

  private void mergeConfigFromRedactionRules(
      List<RedactionRule> redactionRules,
      Map<String, PiiElement> keyRegexToPiiElementMap,
      Map<String, PiiElement> valueRegexToPiiElementMap,
      Map<String, ComplexData> complexDataMap) {
    for (RedactionRule redactionRule : redactionRules) {
      PiiElement piiElement =
          PiiElement.newBuilder()
              .setRegex(redactionRule.getRegex())
              .setCategory(redactionRule.getCategory())
              .setRedactionStrategy(redactionRule.getRedactionStrategy())
              .setSessionIdentifier(redactionRule.getSessionIdentifier())
              .setRuleId(redactionRule.getId())
              .addAllConditions(redactionRule.getConditionsList())
              .setFqn(redactionRule.getFqn())
              .build();
      switch (redactionRule.getMatchType()) {
        case MATCH_TYPE_HEADER:
        case MATCH_TYPE_KEY:
          keyRegexToPiiElementMap.putIfAbsent(redactionRule.getRegex(), piiElement);
          break;
        case MATCH_TYPE_VALUE:
          valueRegexToPiiElementMap.putIfAbsent(redactionRule.getRegex(), piiElement);
          break;
        case MATCH_TYPE_COMPLEX_DATA:
          keyRegexToPiiElementMap.putIfAbsent(redactionRule.getRegex(), piiElement);
          ComplexData complexData = redactionRule.getComplexData();
          complexDataMap.putIfAbsent(complexData.getKey(), complexData);
          break;
        default:
      }
    }
  }

  private Map<DataSuppression, Set<DataType>> getDataTypesForRedactionOrObfuscation(
      RequestContext requestContext) {
    if (!configServiceCoordinator.isDataClassificationEnabled(requestContext)) {
      return Collections.emptyMap();
    }
    List<DataSet> dataSets = configServiceCoordinator.getAllDataSets(requestContext);
    List<DataType> dataTypes = configServiceCoordinator.getAllDataTypes(requestContext);
    Map<String, DataType> dataTypeToIdMap =
        dataTypes.stream().collect(Collectors.toUnmodifiableMap(DataType::getId, identity()));
    Map<String, DataType> redactedDataTypesMap =
        dataSets.stream()
            .filter(
                dataSet ->
                    dataSet.getInfo().getEnabled()
                        && dataSet.getInfo().getDataSuppression().equals(DATA_SUPPRESSION_REDACT))
            .flatMap(dataSet -> dataSet.getInfo().getDataTypeIdsList().stream())
            .map(dataTypeToIdMap::get)
            .filter(Objects::nonNull)
            .collect(
                Collectors.toMap(DataType::getId, identity(), (x, y) -> y, LinkedHashMap::new));
    Set<DataType> obfuscatedDataTypes =
        dataSets.stream()
            .filter(
                dataSet ->
                    dataSet.getInfo().getEnabled()
                        && dataSet
                            .getInfo()
                            .getDataSuppression()
                            .equals(DATA_SUPPRESSION_OBFUSCATE))
            .flatMap(dataSet -> dataSet.getInfo().getDataTypeIdsList().stream())
            .filter(id -> !redactedDataTypesMap.containsKey(id))
            .map(dataTypeToIdMap::get)
            .filter(Objects::nonNull)
            .collect(Collectors.toCollection(LinkedHashSet::new));
    return Map.of(
        DATA_SUPPRESSION_REDACT,
        new LinkedHashSet<>(redactedDataTypesMap.values()),
        DATA_SUPPRESSION_OBFUSCATE,
        obfuscatedDataTypes);
  }

  private void mergeConfigFromDataTypes(
      Set<DataType> dataTypes,
      RedactionStrategy strategy,
      Map<String, PiiElement> keyRegexToPiiElementMap) {
    dataTypes.forEach(
        dataType -> {
          PiiElement.Builder piiElementBuilder =
              PiiElement.newBuilder().setRedactionStrategy(strategy).setRuleId(dataType.getId());
          dataType
              .getRule()
              .getScopedPatternList()
              .forEach(
                  scopedPattern ->
                      setRegexForScopedPattern(
                          scopedPattern, piiElementBuilder, keyRegexToPiiElementMap));
        });
  }

  private void setRegexForScopedPattern(
      ScopedPattern scopedPattern,
      PiiElement.Builder piiElementBuilder,
      Map<String, PiiElement> keyRegexToPiiElementMap) {
    List<String> prefixes = getPrefixesAndUpdateFqn(scopedPattern, piiElementBuilder);
    // We can have two PiiElement with same regex but different effects.
    // For ex. one with fqn = true and the other with fqn = false.
    for (String prefix : prefixes) {
      if (scopedPattern.hasKeyPattern()) {
        String keyRegex = prefix + scopedPattern.getKeyPattern().getValue();
        piiElementBuilder.setRegex(keyRegex);
        keyRegexToPiiElementMap.putIfAbsent(keyRegex, piiElementBuilder.build());
      }
    }
  }

  private List<String> getPrefixesAndUpdateFqn(
      ScopedPattern scopedPattern, PiiElement.Builder piiElementBuilder) {
    switch (scopedPattern.getParameterType()) {
      case PARAMETER_TYPE_REQUEST_HEADER:
        piiElementBuilder.setFqn(true);
        return REQUEST_HEADERS_PREFIXES_LIST;
      case PARAMETER_TYPE_RESPONSE_HEADER:
        piiElementBuilder.setFqn(true);
        return RESPONSE_HEADERS_PREFIXES_LIST;
      default:
        return EMPTY_PREFIXES_LIST;
    }
  }
}
