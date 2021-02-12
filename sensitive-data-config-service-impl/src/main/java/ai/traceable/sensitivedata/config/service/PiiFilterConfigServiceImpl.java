package ai.traceable.sensitivedata.config.service;

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
import com.typesafe.config.Config;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class PiiFilterConfigServiceImpl
    extends PiiFilterConfigServiceGrpc.PiiFilterConfigServiceImplBase {

  static final String DEFAULT_PII_FILTER_CONFIG = "default.pii.filter.config";
  static final String SENSITIVE_DATA_CONFIG_SERVICE_CONFIG = "sensitive.data.config.service";
  static final String INSIGHTS_SERVICE_CONFIG = "insights.service.config";

  private final ConfigServiceCoordinator configServiceCoordinator;
  private final InsightsServiceCoordinator insightsServiceCoordinator;
  private final PiiFilterConfig defaultPiiFilterConfig;

  public PiiFilterConfigServiceImpl(Channel configChannel, Config config) {
    this(
        configChannel,
        ManagedChannelBuilder.forAddress(
                config.getConfig(INSIGHTS_SERVICE_CONFIG).getString("host"),
                config.getConfig(INSIGHTS_SERVICE_CONFIG).getInt("port"))
            .usePlaintext()
            .build(),
        config);
  }

  public PiiFilterConfigServiceImpl(Channel configChannel, Channel insightsChannel, Config config) {
    Config sensitiveDataConfigServiceConfig =
        config.getConfig(SENSITIVE_DATA_CONFIG_SERVICE_CONFIG);
    this.configServiceCoordinator =
        new ConfigServiceCoordinatorImpl(configChannel, sensitiveDataConfigServiceConfig);
    this.insightsServiceCoordinator = new InsightsServiceCoordinatorImpl(insightsChannel);
    this.defaultPiiFilterConfig =
        SensitiveDataConfigUtils.toPiiFilterConfig(
            sensitiveDataConfigServiceConfig.getConfig(DEFAULT_PII_FILTER_CONFIG));
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

      // get appropriate parts of pii filter config from redaction rules
      List<RedactionRule> redactionRules =
          configServiceCoordinator.getAllRedactionRules(requestContext);
      // reverse the list to get redaction rules from latest to earliest
      Collections.reverse(redactionRules);
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

      // if automatic secret redaction is enabled, merge config from default config
      boolean automaticSecretRedactionStrategyEnabled =
          configServiceCoordinator.isAutomaticSecretRedactionStrategyEnabled(requestContext);
      if (automaticSecretRedactionStrategyEnabled) {
        addPiiElements(keyRegexToPiiElementMap, defaultPiiFilterConfig.getKeyRegexsList());
        addPiiElements(valueRegexToPiiElementMap, defaultPiiFilterConfig.getValueRegexsList());
        addComplexDataElements(complexDataMap, defaultPiiFilterConfig.getComplexDataList());
      }

      PiiFilterConfig resultingPiiFilterConfig =
          PiiFilterConfig.newBuilder()
              .addAllPrefixes(defaultPiiFilterConfig.getPrefixesList())
              .setRedactionStrategy(defaultPiiFilterConfig.getRedactionStrategy())
              .addAllKeyRegexs(keyRegexToPiiElementMap.values())
              .addAllValueRegexs(valueRegexToPiiElementMap.values())
              .addAllComplexData(complexDataMap.values())
              .build();
      GetPiiFilterConfigResponse response =
          GetPiiFilterConfigResponse.newBuilder()
              .setPiiFilterConfig(resultingPiiFilterConfig)
              .build();
      responseObserver.onNext(response);
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
