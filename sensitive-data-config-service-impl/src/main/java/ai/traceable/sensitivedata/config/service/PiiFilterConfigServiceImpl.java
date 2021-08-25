package ai.traceable.sensitivedata.config.service;

import ai.traceable.config.uuid.UuidGenerator;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class PiiFilterConfigServiceImpl extends PiiFilterConfigServiceGrpc.PiiFilterConfigServiceImplBase {

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

      PiiFilterConfig resultingPiiFilterConfig =
          PiiFilterConfig.newBuilder()
              .addAllPrefixes(defaultPiiFilterConfig.getPrefixesList())
              .setRedactionStrategy(defaultPiiFilterConfig.getRedactionStrategy())
              .addAllKeyRegexs(keyRegexToPiiElementMap.values())
              .addAllValueRegexs(valueRegexToPiiElementMap.values())
              .addAllComplexData(complexDataMap.values())
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
}
