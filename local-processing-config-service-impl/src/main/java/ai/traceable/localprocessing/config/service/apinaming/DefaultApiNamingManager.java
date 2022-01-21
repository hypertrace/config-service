package ai.traceable.localprocessing.config.service.apinaming;

import ai.traceable.anomaly.config.service.v1.trainer.ApiNamingTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.CustomRulesListConfig;
import ai.traceable.anomaly.config.service.v1.trainer.GetAllScopedTrainingConfigsRequest;
import ai.traceable.anomaly.config.service.v1.trainer.GetAllScopedTrainingConfigsResponse;
import ai.traceable.anomaly.config.service.v1.trainer.GetTrainingConfigsFilter;
import ai.traceable.anomaly.config.service.v1.trainer.ScopedTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.ThresholdRegexConfig;
import ai.traceable.anomaly.config.service.v1.trainer.TrainerConfigServiceGrpc.TrainerConfigServiceBlockingStub;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingConfigType;
import ai.traceable.anomaly.config.service.v1.trainer.TrieModelTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.UrlFilterConfig;
import ai.traceable.localprocessing.config.service.client.EntityDataServiceClient;
import ai.traceable.localprocessing.config.service.config.ApiNamingConfig;
import ai.traceable.localprocessing.config.service.utils.UuidGenerator;
import ai.traceable.localprocessing.config.service.v1.GetApiNamingModelRequest;
import ai.traceable.localprocessing.config.service.v1.HttpApiNamingCustomRule;
import ai.traceable.localprocessing.config.service.v1.HttpServiceResponse;
import ai.traceable.localprocessing.config.service.v1.ServiceRequest;
import ai.traceable.localprocessing.config.service.v1.WildcardConfig;
import ai.traceable.localprocessing.config.service.v1.WildcardType;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.constants.v1.CommonAttribute;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.ByTypeAndIdentifyingAttributes;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.Value;
import org.hypertrace.entity.service.constants.EntityConstants;
import org.hypertrace.entity.v1.entitytype.EntityType;

@Slf4j
class DefaultApiNamingManager implements ApiNamingManager {

  private final TrainerConfigServiceBlockingStub configServiceBlockingStub;
  private final UuidGenerator uuidGenerator;
  private final EntityDataServiceClient entityDataServiceClient;
  private final ApiNamingConfig apiNamingConfig;
  private static final String ENVIRONMENT_IDENTIFYING_ATTRIBUTE = "ENVIRONMENT";

  @Inject
  public DefaultApiNamingManager(
      TrainerConfigServiceBlockingStub configServiceBlockingStub,
      EntityDataServiceClient entityDataServiceClient,
      ApiNamingConfig apiNamingConfig,
      UuidGenerator uuidGenerator) {
    this.configServiceBlockingStub = configServiceBlockingStub;
    this.entityDataServiceClient = entityDataServiceClient;
    this.apiNamingConfig = apiNamingConfig;
    this.uuidGenerator = uuidGenerator;
  }

  public List<String> getFallbackWildcardRegexes() {
    return apiNamingConfig.getFallbackRegexes();
  }

  public List<HttpServiceResponse> getHttpServiceResponseList(
      RequestContext requestContext, GetApiNamingModelRequest request) {
    Map<String, ServiceRequest> serviceIdServiceRequestMap = new HashMap<>();

    for (ServiceRequest serviceRequest : request.getServiceRequestsList()) {
      Entity entity = null;
      try {
        entity =
            entityDataServiceClient.getByTypeAndIdentifyingProperties(
                requestContext,
                buildGetEntityByTypeAndIdentifyingAttributesRequest(
                    serviceRequest.getServiceName(), getEnvironment(request)));
      } catch (Exception e) {
        log.error(
            "Could not fetch entity for tenant id:{} and service request : {}",
            requestContext.getTenantId(),
            serviceRequest);
        continue;
      }

      if (!isValidEntity(entity)) {
        log.error(
            "Entity not found for tenant id:{} and service request : {}",
            requestContext.getTenantId(),
            serviceRequest);
        continue;
      }
      serviceIdServiceRequestMap.put(entity.getEntityId(), serviceRequest);
    }

    List<HttpServiceResponse> httpServiceResponses = new ArrayList<>();
    List<ScopedTrainingConfig> scopedTrainingConfigs = getScopedTrainingConfigsList();
    serviceIdServiceRequestMap.forEach(
        (serviceId, serviceRequest) ->
            getTrainingConfigListForService(scopedTrainingConfigs, requestContext, serviceId)
                .ifPresent(
                    trainingConfigs ->
                        httpServiceResponses.add(
                            HttpServiceResponse.newBuilder()
                                .setServiceName(serviceRequest.getServiceName())
                                .setHttpConfig(
                                    buildApiNamingConfig(
                                        trainingConfigs, serviceRequest.getConfigHash()))
                                .build())));
    return Collections.unmodifiableList(httpServiceResponses);
  }

  private boolean isValidEntity(Entity entity) {
    return entity != null && !entity.getEntityId().isEmpty();
  }

  private Optional<String> getEnvironment(GetApiNamingModelRequest request) {
    if (request.hasEnvironment()) {
      return Optional.of(request.getEnvironment());
    }
    return Optional.empty();
  }

  private ByTypeAndIdentifyingAttributes buildGetEntityByTypeAndIdentifyingAttributesRequest(
      String serviceName, Optional<String> environment) {
    ByTypeAndIdentifyingAttributes.Builder byTypeAndIdentifyingAttributesBuilder =
        ByTypeAndIdentifyingAttributes.newBuilder()
            .setEntityType(EntityType.SERVICE.name())
            .putIdentifyingAttributes(
                EntityConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_FQN),
                AttributeValue.newBuilder()
                    .setValue(Value.newBuilder().setString(serviceName).build())
                    .build());
    if (environment.isPresent()) {
      byTypeAndIdentifyingAttributesBuilder.putIdentifyingAttributes(
          ENVIRONMENT_IDENTIFYING_ATTRIBUTE,
          AttributeValue.newBuilder()
              .setValue(Value.newBuilder().setString(serviceName).build())
              .build());
    }
    return byTypeAndIdentifyingAttributesBuilder.build();
  }

  private List<ScopedTrainingConfig> getScopedTrainingConfigsList() {
    GetAllScopedTrainingConfigsResponse response =
        configServiceBlockingStub.getAllScopedTrainingConfigs(
            GetAllScopedTrainingConfigsRequest.newBuilder()
                .setFilter(
                    GetTrainingConfigsFilter.newBuilder()
                        .addTrainingConfigTypes(TrainingConfigType.TRAINING_CONFIG_TYPE_API_NAMING))
                .build());

    return response.getScopedTrainingConfigsList();
  }

  private Optional<List<TrainingConfig>> getTrainingConfigListForService(
      List<ScopedTrainingConfig> scopedTrainingConfigs,
      RequestContext requestContext,
      String serviceId) {
    try {
      return scopedTrainingConfigs.stream()
          .filter(scopedTrainingConfig -> scopedTrainingConfig.getConfigScope().hasServiceScope())
          .filter(
              scopedTrainingConfig ->
                  scopedTrainingConfig.getConfigScope().getServiceScope().getId().equals(serviceId))
          .findAny()
          .map(ScopedTrainingConfig::getTrainingConfigsList)
          .or(() -> getTrainingConfigListForTenant(scopedTrainingConfigs, requestContext));
    } catch (Exception exception) {
      log.error(
          "Get Training Config List failed for Request context {}, service {} with exception",
          requestContext,
          serviceId,
          exception);
      return Optional.empty();
    }
  }

  private Optional<List<TrainingConfig>> getTrainingConfigListForTenant(
      List<ScopedTrainingConfig> scopedTrainingConfigs, RequestContext requestContext) {
    try {
      return scopedTrainingConfigs.stream()
          .filter(scopedTrainingConfig -> scopedTrainingConfig.getConfigScope().hasCustomerScope())
          .findAny()
          .map(ScopedTrainingConfig::getTrainingConfigsList);
    } catch (Exception exception) {
      log.error(
          "Get Training Config List failed for Request context {} with exception",
          requestContext,
          exception);
      return Optional.empty();
    }
  }

  private ai.traceable.localprocessing.config.service.v1.HttpApiNamingConfig buildApiNamingConfig(
      List<TrainingConfig> trainingConfigs, String configHash) {
    ai.traceable.localprocessing.config.service.v1.HttpApiNamingConfig.Builder
        apiNamingConfigBuilder =
            ai.traceable.localprocessing.config.service.v1.HttpApiNamingConfig.newBuilder();
    Optional<TrieModelTrainingConfig> trieModelTrainingConfigs =
        getTrieModelTrainingConfig(trainingConfigs);

    trieModelTrainingConfigs.ifPresent(
        trieModelTrainingConfig ->
            apiNamingConfigBuilder
                .addAllExtensions(trieModelTrainingConfig.getExtensions().getValuesList())
                .addAllSegmentWhitelistRegexes(
                    trieModelTrainingConfig.getAllowRegexList().getValuesList())
                .addAllWildcardConfigs(convertWildcardConfigs(trieModelTrainingConfig)));
    getUrlFilterConfig(trainingConfigs)
        .ifPresent(
            urlFilterConfig ->
                apiNamingConfigBuilder.addAllUrlRejectRegexes(getUrlRejectRegex(urlFilterConfig)));
    getCustomRulesListConfig(trainingConfigs)
        .ifPresent(
            customRulesListConfig ->
                apiNamingConfigBuilder.addAllApiNamingCustomRules(
                    convertCustomRules(customRulesListConfig)));

    String hash = uuidGenerator.generateId(apiNamingConfigBuilder.build());
    if (!hash.equals(configHash)) {
      return apiNamingConfigBuilder.setHash(hash).build();
    }
    return ai.traceable.localprocessing.config.service.v1.HttpApiNamingConfig.newBuilder()
        .setHash(hash)
        .build();
  }

  private List<WildcardConfig> convertWildcardConfigs(
      TrieModelTrainingConfig trieModelTrainingConfig) {
    return List.of(
        buildWildcardConfig(WildcardType.WILDCARD_TYPE_ID, trieModelTrainingConfig.getIds()),
        buildWildcardConfig(
            WildcardType.WILDCARD_TYPE_LOW_CARDINALITY,
            trieModelTrainingConfig.getLowCardinality()),
        buildWildcardConfig(
            WildcardType.WILDCARD_TYPE_MEDIUM_CARDINALITY,
            trieModelTrainingConfig.getMediumCardinality()),
        buildWildcardConfig(
            WildcardType.WILDCARD_TYPE_HIGH_CARDINALITY,
            trieModelTrainingConfig.getHighCardinality()));
  }

  private WildcardConfig buildWildcardConfig(
      WildcardType wildcardType, ThresholdRegexConfig thresholdRegexConfig) {
    return WildcardConfig.newBuilder()
        .setWildcardType(wildcardType)
        .setPriority(getWildcardPriority(wildcardType))
        .addAllIdentificationRegexes(thresholdRegexConfig.getRegexList().getValuesList())
        .build();
  }

  private int getWildcardPriority(WildcardType wildcardType) {
    switch (wildcardType) {
      case WILDCARD_TYPE_ID:
        return 4;
      case WILDCARD_TYPE_LOW_CARDINALITY:
        return 3;
      case WILDCARD_TYPE_HIGH_CARDINALITY:
        return 2;
      case WILDCARD_TYPE_MEDIUM_CARDINALITY:
        return 1;
      default:
        log.error("Unrecognized wildcard type: {}", wildcardType);
        return 0;
    }
  }

  private List<HttpApiNamingCustomRule> convertCustomRules(
      CustomRulesListConfig customRulesListConfig) {
    // TODO: setting priority once available from training config service APIs
    return customRulesListConfig.getCustomRulesConfigList().stream()
        .map(
            customRuleConfig ->
                HttpApiNamingCustomRule.newBuilder()
                    .setRegexPattern(customRuleConfig.getRegex())
                    .setUrlPattern(customRuleConfig.getUrlPattern())
                    .build())
        .collect(Collectors.toUnmodifiableList());
  }

  private List<String> getUrlRejectRegex(UrlFilterConfig urlFilterConfig) {
    return urlFilterConfig.getUrlRejectRegexPatterns().getValuesList();
  }

  private Optional<CustomRulesListConfig> getCustomRulesListConfig(
      List<TrainingConfig> trainingConfigs) {
    return trainingConfigs.stream()
        .filter(TrainingConfig::hasApiNamingTrainingConfig)
        .map(TrainingConfig::getApiNamingTrainingConfig)
        .filter(ApiNamingTrainingConfig::hasCustomRulesListConfig)
        .map(ApiNamingTrainingConfig::getCustomRulesListConfig)
        .findAny();
  }

  private Optional<TrieModelTrainingConfig> getTrieModelTrainingConfig(
      List<TrainingConfig> trainingConfigs) {
    return trainingConfigs.stream()
        .filter(TrainingConfig::hasApiNamingTrainingConfig)
        .map(TrainingConfig::getApiNamingTrainingConfig)
        .filter(ApiNamingTrainingConfig::hasTrieModelTrainingConfig)
        .map(ApiNamingTrainingConfig::getTrieModelTrainingConfig)
        .findAny();
  }

  private Optional<UrlFilterConfig> getUrlFilterConfig(List<TrainingConfig> trainingConfigs) {
    return trainingConfigs.stream()
        .filter(TrainingConfig::hasApiNamingTrainingConfig)
        .map(TrainingConfig::getApiNamingTrainingConfig)
        .filter(ApiNamingTrainingConfig::hasUrlFilterConfig)
        .map(ApiNamingTrainingConfig::getUrlFilterConfig)
        .findAny();
  }
}
