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
import ai.traceable.localprocessing.config.service.v1.FullTrie;
import ai.traceable.localprocessing.config.service.v1.GetApiNamingModelRequest;
import ai.traceable.localprocessing.config.service.v1.HttpApiNamingConfig;
import ai.traceable.localprocessing.config.service.v1.HttpApiNamingCustomRule;
import ai.traceable.localprocessing.config.service.v1.HttpServiceResponse;
import ai.traceable.localprocessing.config.service.v1.Node;
import ai.traceable.localprocessing.config.service.v1.ServiceRequest;
import ai.traceable.localprocessing.config.service.v1.Trie;
import ai.traceable.localprocessing.config.service.v1.WildcardConfig;
import ai.traceable.localprocessing.config.service.v1.WildcardType;
import ai.traceable.platform.apientity.Segment;
import ai.traceable.platform.apientity.TrieNodeType;
import ai.traceable.platform.apientity.Wildcard;
import ai.traceable.platform.apientity.http.model.TrieModel;
import ai.traceable.platform.apientity.http.model.TrieNodeConfig;
import ai.traceable.platform.model.store.ModelPersistentStore;
import ai.traceable.platform.model.store.ServiceScope;
import com.google.inject.Inject;
import com.google.protobuf.ProtocolStringList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
  private final ModelPersistentStore<TrieModel> modelStore;

  @Inject
  public DefaultApiNamingManager(
      TrainerConfigServiceBlockingStub configServiceBlockingStub,
      EntityDataServiceClient entityDataServiceClient,
      ApiNamingConfig apiNamingConfig,
      ModelPersistentStore<TrieModel> modelStore,
      UuidGenerator uuidGenerator) {
    this.configServiceBlockingStub = configServiceBlockingStub;
    this.entityDataServiceClient = entityDataServiceClient;
    this.apiNamingConfig = apiNamingConfig;
    this.modelStore = modelStore;
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
                    trainingConfigs -> {
                      try {
                        HttpApiNamingConfig httpApiNamingConfig =
                            buildHttpApiNamingConfig(
                                trainingConfigs, serviceRequest.getConfigHash());
                        httpServiceResponses.add(
                            HttpServiceResponse.newBuilder()
                                .setServiceName(serviceRequest.getServiceName())
                                .setTrie(getTrie(requestContext, httpApiNamingConfig, serviceId))
                                .setHttpConfig(httpApiNamingConfig)
                                .build());
                      } catch (Exception e) {
                        log.error("Could not retrieve trie for request:{}", request, e);
                      }
                    }));
    return Collections.unmodifiableList(httpServiceResponses);
  }

  private Trie getTrie(
      RequestContext requestContext, HttpApiNamingConfig httpApiNamingConfig, String serviceId)
      throws IOException {
    TrieModel trieModel =
        modelStore
            .loadModel(new ServiceScope(requestContext.getTenantId().get(), serviceId))
            .getModel();
    Set<List<Segment>> nonEmbryonicPaths =
        trieModel.getNonEmbryonicPaths(buildTrieNodeConfig(httpApiNamingConfig));
    return Trie.newBuilder().setFullTrie(buildFullTrie(nonEmbryonicPaths)).build();
  }

  private FullTrie buildFullTrie(Set<List<Segment>> paths) {
    ArrayList<Node> roots = new ArrayList<>();
    for (List<Segment> segments : paths) {
      roots = insertIntoTrie(roots, segments, 0);
    }
    return FullTrie.newBuilder().addAllRoots(roots).build();
  }

  private ArrayList<Node> insertIntoTrie(ArrayList<Node> roots, List<Segment> segments, int index) {
    if (segments.size() == index) {
      return new ArrayList<>();
    }

    ai.traceable.localprocessing.config.service.v1.Value segmentValue =
        convertSegmentToValue(segments.get(index));
    Optional<Node> maybeNode = getNode(roots, segmentValue);
    if (maybeNode.isPresent()) {
      Node currentNode = maybeNode.get();
      Node newNode =
          Node.newBuilder(currentNode)
              .clearChildren()
              .addAllChildren(
                  insertIntoTrie(
                      new ArrayList<>(currentNode.getChildrenList()), segments, index + 1))
              .build();
      roots.remove(currentNode);
      roots.add(newNode);
    } else {
      roots.add(
          Node.newBuilder()
              .setValue(segmentValue)
              .addAllChildren(insertIntoTrie(new ArrayList<>(), segments, index + 1))
              .build());
    }
    return roots;
  }

  private Optional<Node> getNode(
      List<Node> nodes, ai.traceable.localprocessing.config.service.v1.Value segmentValue) {
    for (Node node : nodes) {
      if (segmentValue.equals(node.getValue())) {
        return Optional.of(node);
      }
    }
    return Optional.empty();
  }

  private ai.traceable.localprocessing.config.service.v1.Value convertSegmentToValue(
      Segment segment) {
    if (segment.getName() == null) {
      log.error("Segment name is null: {}", segment);
    }

    if (isWildcard(segment)) {
      return ai.traceable.localprocessing.config.service.v1.Value.newBuilder()
          .setWildcard(convertWildcard((Wildcard) segment.getName()))
          .build();
    }
    return ai.traceable.localprocessing.config.service.v1.Value.newBuilder()
        .setName(segment.getName().toString())
        .build();
  }

  private boolean isWildcard(Segment segment) {
    return Wildcard.class.isAssignableFrom(segment.getName().getClass());
  }

  private ai.traceable.localprocessing.config.service.v1.Wildcard convertWildcard(
      Wildcard wildcard) {
    ai.traceable.localprocessing.config.service.v1.Wildcard.Builder wildcardBuilder =
        ai.traceable.localprocessing.config.service.v1.Wildcard.newBuilder()
            .setWildcardType(convertWildcardType(wildcard.getWildcardType()));
    if (!wildcard.getExtension().isEmpty()) {
      wildcardBuilder = wildcardBuilder.setExtension(wildcard.getExtension());
    }
    return wildcardBuilder.build();
  }

  private WildcardType convertWildcardType(TrieNodeType wildcardType) {
    switch (wildcardType) {
      case ID:
        return WildcardType.WILDCARD_TYPE_ID;
      case LOW_CARDINALITY:
        return WildcardType.WILDCARD_TYPE_LOW_CARDINALITY;
      case HIGH_CARDINALITY:
        return WildcardType.WILDCARD_TYPE_HIGH_CARDINALITY;
      case MEDIUM_CARDINALITY:
        return WildcardType.WILDCARD_TYPE_MEDIUM_CARDINALITY;
      case WHITELIST:
        return WildcardType.WILDCARD_TYPE_UNSPECIFIED;
      default:
        log.error("Unknown wildcard type:{}", wildcardType);
        return WildcardType.WILDCARD_TYPE_UNSPECIFIED;
    }
  }

  private TrieNodeConfig buildTrieNodeConfig(HttpApiNamingConfig httpApiNamingConfig) {
    return new TrieNodeConfig(
        httpApiNamingConfig.getSegmentWhitelistRegexesList(),
        getWildcardConfigList(
            filterWildcardConfigs(
                httpApiNamingConfig.getWildcardConfigsList(), WildcardType.WILDCARD_TYPE_ID)),
        getWildcardConfigList(
            filterWildcardConfigs(
                httpApiNamingConfig.getWildcardConfigsList(),
                WildcardType.WILDCARD_TYPE_LOW_CARDINALITY)),
        getWildcardConfigList(
            filterWildcardConfigs(
                httpApiNamingConfig.getWildcardConfigsList(),
                WildcardType.WILDCARD_TYPE_HIGH_CARDINALITY)),
        new HashSet<>(httpApiNamingConfig.getExtensionsList()),
        apiNamingConfig.getEmbryonicThreshold());
  }

  private Optional<ProtocolStringList> filterWildcardConfigs(
      List<WildcardConfig> wildcardConfigs, WildcardType wildcardType) {
    return wildcardConfigs.stream()
        .filter(wildcardConfig -> wildcardConfig.getWildcardType().equals(wildcardType))
        .map(WildcardConfig::getIdentificationRegexesList)
        .findAny();
  }

  private List<String> getWildcardConfigList(
      Optional<ProtocolStringList> protocolStringListOptional) {
    if (protocolStringListOptional.isEmpty()) {
      return Collections.emptyList();
    }
    return protocolStringListOptional.get();
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

  private HttpApiNamingConfig buildHttpApiNamingConfig(
      List<TrainingConfig> trainingConfigs, String configHash) {
    HttpApiNamingConfig.Builder httpApiNamingConfigBuilder = HttpApiNamingConfig.newBuilder();
    Optional<TrieModelTrainingConfig> trieModelTrainingConfigs =
        getTrieModelTrainingConfig(trainingConfigs);

    trieModelTrainingConfigs.ifPresent(
        trieModelTrainingConfig ->
            httpApiNamingConfigBuilder
                .addAllExtensions(trieModelTrainingConfig.getExtensions().getValuesList())
                .addAllSegmentWhitelistRegexes(
                    trieModelTrainingConfig.getAllowRegexList().getValuesList())
                .addAllWildcardConfigs(convertWildcardConfigs(trieModelTrainingConfig)));
    getUrlFilterConfig(trainingConfigs)
        .ifPresent(
            urlFilterConfig ->
                httpApiNamingConfigBuilder.addAllUrlRejectRegexes(
                    getUrlRejectRegex(urlFilterConfig)));
    getCustomRulesListConfig(trainingConfigs)
        .ifPresent(
            customRulesListConfig ->
                httpApiNamingConfigBuilder.addAllApiNamingCustomRules(
                    convertCustomRules(customRulesListConfig)));

    String hash = uuidGenerator.generateId(httpApiNamingConfigBuilder.build());
    if (!hash.equals(configHash)) {
      return httpApiNamingConfigBuilder.setHash(hash).build();
    }
    return HttpApiNamingConfig.newBuilder().setHash(hash).build();
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
