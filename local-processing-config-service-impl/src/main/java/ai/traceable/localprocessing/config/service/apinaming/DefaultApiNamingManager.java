package ai.traceable.localprocessing.config.service.apinaming;

import static java.util.function.Predicate.not;

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
import ai.traceable.localprocessing.config.service.client.EntityDataServiceClient;
import ai.traceable.localprocessing.config.service.config.ApiNamingConfig;
import ai.traceable.localprocessing.config.service.utils.UuidGenerator;
import ai.traceable.localprocessing.config.service.v1.DiffTrie;
import ai.traceable.localprocessing.config.service.v1.FullTrie;
import ai.traceable.localprocessing.config.service.v1.GetApiNamingModelRequest;
import ai.traceable.localprocessing.config.service.v1.HttpApiNamingConfig;
import ai.traceable.localprocessing.config.service.v1.HttpApiNamingCustomRule;
import ai.traceable.localprocessing.config.service.v1.HttpServiceResponse;
import ai.traceable.localprocessing.config.service.v1.Node;
import ai.traceable.localprocessing.config.service.v1.ServiceRequest;
import ai.traceable.localprocessing.config.service.v1.Trie;
import ai.traceable.localprocessing.config.service.v1.TrieDiffLog;
import ai.traceable.localprocessing.config.service.v1.TrieNodePath;
import ai.traceable.localprocessing.config.service.v1.WildcardConfig;
import ai.traceable.localprocessing.config.service.v1.WildcardType;
import ai.traceable.platform.apientity.Addition;
import ai.traceable.platform.apientity.Deletion;
import ai.traceable.platform.apientity.Segment;
import ai.traceable.platform.apientity.TrieNodeType;
import ai.traceable.platform.apientity.Wildcard;
import ai.traceable.platform.apientity.http.difflog.TrieDiffLogModel;
import ai.traceable.platform.apientity.http.model.TrieModel;
import ai.traceable.platform.apientity.http.model.TrieNodeConfig;
import ai.traceable.platform.model.PersistedModel;
import ai.traceable.platform.model.filter.ModelFilter;
import ai.traceable.platform.model.store.ModelPersistentStore;
import ai.traceable.platform.model.store.ModelScope;
import ai.traceable.platform.model.store.ServiceScope;
import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.common.collect.Streams;
import com.google.inject.Inject;
import com.google.protobuf.ProtocolStringList;
import com.typesafe.config.Config;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slf4j
class DefaultApiNamingManager implements ApiNamingManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultApiNamingManager.class);

  private final TrainerConfigServiceBlockingStub configServiceBlockingStub;
  private final UuidGenerator uuidGenerator;
  private final EntityDataServiceClient entityDataServiceClient;
  private final ApiNamingConfig apiNamingConfig;
  private static final String ENVIRONMENT_IDENTIFYING_ATTRIBUTE = "ENVIRONMENT";
  private static final String FULL_TRIE_RELOAD_DEFAULT_CONFIG_KEY = "default";
  private static final String FULL_TRIE_RELOAD_TIMESTAMP_CONFIG_KEY = "timestamp";
  private static final String FULL_TRIE_RELOAD_DISABLED_CONFIG_KEY = "disabled";
  private final ModelPersistentStore<TrieDiffLogModel> trieDiffLogModelStore;
  private final ModelPersistentStore<TrieModel> trieModelStore;

  @Inject
  public DefaultApiNamingManager(
      TrainerConfigServiceBlockingStub configServiceBlockingStub,
      EntityDataServiceClient entityDataServiceClient,
      ApiNamingConfig apiNamingConfig,
      ModelPersistentStore<TrieModel> trieModelStore,
      ModelPersistentStore<TrieDiffLogModel> trieDiffLogModelModelStore,
      UuidGenerator uuidGenerator) {
    this.configServiceBlockingStub = configServiceBlockingStub;
    this.entityDataServiceClient = entityDataServiceClient;
    this.apiNamingConfig = apiNamingConfig;
    this.trieModelStore = trieModelStore;
    this.trieDiffLogModelStore = trieDiffLogModelModelStore;
    this.uuidGenerator = uuidGenerator;
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

    return buildHttpResponses(request, requestContext, serviceIdServiceRequestMap);
  }

  private List<HttpServiceResponse> buildHttpResponses(
      GetApiNamingModelRequest request,
      RequestContext requestContext,
      Map<String, ServiceRequest> serviceIdServiceRequestMap) {
    List<HttpServiceResponse> httpServiceResponses = new ArrayList<>();
    List<ScopedTrainingConfig> scopedTrainingConfigs = getScopedTrainingConfigsList();
    serviceIdServiceRequestMap.forEach(
        (serviceId, serviceRequest) ->
            getTrainingConfigListForService(scopedTrainingConfigs, requestContext, serviceId)
                .ifPresent(
                    trainingConfigs -> {
                      try {
                        HttpApiNamingConfigInfo httpApiNamingConfigInfo =
                            buildApiNamingConfig(trainingConfigs, serviceRequest.getConfigHash());
                        httpServiceResponses.add(
                            HttpServiceResponse.newBuilder()
                                .setServiceName(serviceRequest.getServiceName())
                                .setTrie(
                                    getTrie(
                                        requestContext,
                                        httpApiNamingConfigInfo,
                                        serviceId,
                                        serviceRequest.hasTrieToken()
                                            ? serviceRequest.getTrieToken()
                                            : "0"))
                                .setHttpConfig(httpApiNamingConfigInfo.getHttpApiNamingConfig())
                                .build());
                      } catch (Exception e) {
                        log.error("Could not retrieve trie for request:{}", request, e);
                      }
                    }));
    return Collections.unmodifiableList(httpServiceResponses);
  }

  /**
   * Sends full trie based on the following conditions
   *
   * <ul>
   *   <li>Agent timestamp is 0 i.e agent makes request for the first time
   *   <li>Agent timestamp is 1hr(configurable) behind trie model timestamp i.e. agent timestamp is
   *       older than the retention period of diff log models
   *   <li>No diff logs and agent timestamp < trie model timestamp
   * </ul>
   */
  private Trie getTrie(
      RequestContext requestContext,
      HttpApiNamingConfigInfo httpApiNamingConfigInfo,
      String serviceId,
      String trieToken)
      throws IOException, ExecutionException, RetryException {
    long agentTimestamp = getAgentTimestamp(trieToken, serviceId, requestContext);
    String tenantId = requestContext.getTenantId().get();
    ServiceScope serviceScope = new ServiceScope(tenantId, serviceId);
    List<PersistedModel<TrieDiffLogModel>> trieDiffLogModels = Collections.emptyList();
    List<TrieDiffLog> trieDiffLogs = Collections.emptyList();
    long trieModelTimestamp = trieModelStore.getModelMetadata(serviceScope).getModificationTime();
    boolean reloadFullTrie = shouldReloadFullTrie(tenantId, serviceId, agentTimestamp);

    if (!reloadFullTrie
        && agentTimestamp != 0
        && trieModelTimestamp - agentTimestamp <= apiNamingConfig.getDiffLogsRetentionPeriod()) {
      trieDiffLogModels = loadDiffLogModels(serviceScope, buildModelFilter(agentTimestamp));
      trieDiffLogs = getAllTrieDiffLogs(trieDiffLogModels);
    }

    if (!trieDiffLogs.isEmpty()) {
      return Trie.newBuilder()
          .setDiffTrie(getDiffTrie(trieDiffLogs))
          .setToken(String.valueOf(getLatestDiffLogTimestamp(trieDiffLogModels)))
          .build();
    } else {
      if (agentTimestamp < trieModelTimestamp) {
        return Trie.newBuilder()
            .setFullTrie(getFullTrie(serviceScope, httpApiNamingConfigInfo))
            .setToken(String.valueOf(trieModelTimestamp))
            .build();
      } else {
        return Trie.newBuilder().setToken(trieToken).build();
      }
    }
  }

  private List<TrieDiffLog> getAllTrieDiffLogs(
      List<PersistedModel<TrieDiffLogModel>> persistedModels) {
    return persistedModels.stream()
        .map(PersistedModel::getModel)
        .map(trieDiffLogModel -> getTrieDiffLogs(trieDiffLogModel.getTrieDiffLog()))
        .flatMap(List::stream)
        .collect(Collectors.toUnmodifiableList());
  }

  private DiffTrie getDiffTrie(List<TrieDiffLog> allTrieDiffLogs) {
    return DiffTrie.newBuilder().addAllTrieDiffLogs(allTrieDiffLogs).build();
  }

  private boolean shouldReloadFullTrie(String tenantId, String serviceId, long agentTimestamp) {
    Config fullTrieReloadConfig = apiNamingConfig.getFullTrieReloadConfig();
    FullTrieReloadConfigInfo fullTrieReloadConfigInfo;
    if (fullTrieReloadConfig.hasPath(tenantId)) {
      Config tenantSpecificConfig = fullTrieReloadConfig.getConfig(tenantId);
      if (tenantSpecificConfig.hasPath(serviceId)) {
        fullTrieReloadConfigInfo =
            getFullTrieReloadConfigInfo(tenantSpecificConfig.getConfig(serviceId));
      } else {
        fullTrieReloadConfigInfo =
            getFullTrieReloadConfigInfo(
                tenantSpecificConfig.getConfig(FULL_TRIE_RELOAD_DEFAULT_CONFIG_KEY));
      }
    } else {
      fullTrieReloadConfigInfo =
          getFullTrieReloadConfigInfo(
              fullTrieReloadConfig.getConfig(FULL_TRIE_RELOAD_DEFAULT_CONFIG_KEY));
    }
    if (fullTrieReloadConfigInfo.isDisabled()) {
      return false;
    }
    return agentTimestamp < fullTrieReloadConfigInfo.getTimestamp();
  }

  private FullTrieReloadConfigInfo getFullTrieReloadConfigInfo(Config config) {
    return new FullTrieReloadConfigInfo(
        config.getBoolean(FULL_TRIE_RELOAD_DISABLED_CONFIG_KEY),
        Instant.parse(config.getString(FULL_TRIE_RELOAD_TIMESTAMP_CONFIG_KEY)).toEpochMilli());
  }

  private long getLatestDiffLogTimestamp(List<PersistedModel<TrieDiffLogModel>> persistedModels) {
    return persistedModels.stream()
        .map(persistedModel -> persistedModel.getMetadata().getModificationTime())
        .max(Long::compare)
        .orElse(0L);
  }

  private FullTrie getFullTrie(
      ServiceScope serviceScope, HttpApiNamingConfigInfo httpApiNamingConfigInfo)
      throws IOException {
    TrieModel trieModel = trieModelStore.loadModel(serviceScope).getModel();
    Set<List<Segment>> nonEmbryonicPaths =
        trieModel.getNonEmbryonicPaths(buildTrieNodeConfig(httpApiNamingConfigInfo));

    return buildFullTrie(nonEmbryonicPaths);
  }

  private long getAgentTimestamp(
      String trieToken, String serviceId, RequestContext requestContext) {
    try {
      return Long.parseLong(trieToken);
    } catch (NumberFormatException e) {
      log.error(
          "Could not parse trieToken:{}, for serviceId:{}, and requestContext:{}",
          trieToken,
          serviceId,
          requestContext);
      return 0;
    }
  }

  private List<PersistedModel<TrieDiffLogModel>> loadDiffLogModels(
      ModelScope scope, ModelFilter filter) throws ExecutionException, RetryException {
    String diffLogDirPath =
        this.getAbsoluteDiffLogDirPath(Path.of(apiNamingConfig.getBaseDirectory()), scope);
    return this.retry(
        () ->
            this.trieDiffLogModelStore.loadModelsInDir(diffLogDirPath, filter).values().stream()
                .collect(Collectors.toUnmodifiableList()));
  }

  private <T> T retry(Callable<T> callable) throws ExecutionException, RetryException {
    Retryer retryer =
        RetryerBuilder.newBuilder()
            .retryIfExceptionOfType(IOException.class)
            .withWaitStrategy(WaitStrategies.fixedWait(100L, TimeUnit.MILLISECONDS))
            .withStopStrategy(StopStrategies.stopAfterAttempt(2))
            .build();

    try {
      return (T) retryer.call(callable);
    } catch (ExecutionException | RetryException ex) {
      LOGGER.error("Error in loading model after retrying", ex);
      throw ex;
    }
  }

  private String getAbsoluteDiffLogDirPath(Path baseDir, ModelScope scope) {
    String modelScopeSubPath = scope.getSubPath();
    String modelScopeAbsoluteDirPath =
        baseDir.resolve(modelScopeSubPath).toAbsolutePath().toString();
    return Path.of(modelScopeAbsoluteDirPath).toAbsolutePath().toString();
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

  private ModelFilter buildModelFilter(long startTimestamp) {
    return ModelFilter.builder().modifiedStartTimestamp(startTimestamp).build();
  }

  private List<TrieDiffLog> getTrieDiffLogs(
      ai.traceable.platform.apientity.TrieDiffLog trieDiffLog) {
    List<TrieDiffLog> pathAdditionTrieDiffLogs =
        convertPathAdditions(trieDiffLog.getPathAdditions());
    List<TrieDiffLog> nodeDeletionTrieDiffLogs =
        convertNodeDeletions(trieDiffLog.getNodeDeletions());
    return Streams.concat(pathAdditionTrieDiffLogs.stream(), nodeDeletionTrieDiffLogs.stream())
        .collect(Collectors.toUnmodifiableList());
  }

  private List<TrieDiffLog> convertPathAdditions(List<Addition> additions) {
    return additions.stream()
        .map(this::convertAdditionToValues)
        .filter(not(List::isEmpty))
        .map(
            values ->
                TrieDiffLog.newBuilder()
                    .setPathAddition(TrieNodePath.newBuilder().addAllValues(values).build())
                    .build())
        .collect(Collectors.toUnmodifiableList());
  }

  private List<TrieDiffLog> convertNodeDeletions(List<Deletion> deletions) {
    return deletions.stream()
        .map(this::convertDeletionToValues)
        .filter(not(List::isEmpty))
        .map(
            values ->
                TrieDiffLog.newBuilder()
                    .setNodeRemoval(TrieNodePath.newBuilder().addAllValues(values).build())
                    .build())
        .collect(Collectors.toUnmodifiableList());
  }

  private List<ai.traceable.localprocessing.config.service.v1.Value> convertAdditionToValues(
      Addition addition) {
    return addition.getSegments().stream()
        .map(this::convertSegmentToValue)
        .collect(Collectors.toUnmodifiableList());
  }

  private List<ai.traceable.localprocessing.config.service.v1.Value> convertDeletionToValues(
      Deletion deletion) {
    return deletion.getSegments().stream()
        .map(this::convertSegmentToValue)
        .collect(Collectors.toUnmodifiableList());
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

  private TrieNodeConfig buildTrieNodeConfig(HttpApiNamingConfigInfo httpApiNamingConfigInfo) {
    HttpApiNamingConfig httpApiNamingConfig = httpApiNamingConfigInfo.getHttpApiNamingConfig();
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
        httpApiNamingConfigInfo.getEmbryonicThreshold());
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

  private HttpApiNamingConfigInfo buildApiNamingConfig(
      List<TrainingConfig> trainingConfigs, String configHash) {
    HttpApiNamingConfig.Builder httpApiNamingConfigBuilder = HttpApiNamingConfig.newBuilder();
    Optional<TrieModelTrainingConfig> maybeTrieModelTrainingConfig =
        getTrieModelTrainingConfig(trainingConfigs);

    int embryonicThreshold =
        maybeTrieModelTrainingConfig
            .map(TrieModelTrainingConfig::getEmbryonicThreshold)
            .orElseGet(apiNamingConfig::getDefaultEmbryonicThreshold);

    maybeTrieModelTrainingConfig.ifPresent(
        trieModelTrainingConfig ->
            httpApiNamingConfigBuilder
                .addAllExtensions(trieModelTrainingConfig.getExtensions().getValuesList())
                .addAllSegmentWhitelistRegexes(
                    trieModelTrainingConfig.getAllowRegexList().getValuesList())
                .addAllWildcardConfigs(convertWildcardConfigs(trieModelTrainingConfig)));

    getCustomRulesListConfig(trainingConfigs)
        .ifPresent(
            customRulesListConfig ->
                httpApiNamingConfigBuilder.addAllApiNamingCustomRules(
                    convertCustomRules(customRulesListConfig)));

    httpApiNamingConfigBuilder.addAllFallbackWildcardRegexes(apiNamingConfig.getFallbackRegexes());
    String hash = uuidGenerator.generateId(httpApiNamingConfigBuilder.build());
    if (!hash.equals(configHash)) {
      return new HttpApiNamingConfigInfo(
          httpApiNamingConfigBuilder.setHash(hash).build(), embryonicThreshold);
    }
    return new HttpApiNamingConfigInfo(
        HttpApiNamingConfig.newBuilder().setHash(hash).build(), embryonicThreshold);
  }

  private List<WildcardConfig> convertWildcardConfigs(
      TrieModelTrainingConfig trieModelTrainingConfig) {
    // Ordering of wildcard configs decides the priority
    // ID > LOW_CARDINALITY > HIGH_CARDINALITY > MEDIUM_CARDINALITY
    return List.of(
        buildWildcardConfig(WildcardType.WILDCARD_TYPE_ID, trieModelTrainingConfig.getIds()),
        buildWildcardConfig(
            WildcardType.WILDCARD_TYPE_LOW_CARDINALITY,
            trieModelTrainingConfig.getLowCardinality()),
        buildWildcardConfig(
            WildcardType.WILDCARD_TYPE_HIGH_CARDINALITY,
            trieModelTrainingConfig.getHighCardinality()),
        buildWildcardConfig(
            WildcardType.WILDCARD_TYPE_MEDIUM_CARDINALITY,
            trieModelTrainingConfig.getMediumCardinality()));
  }

  private WildcardConfig buildWildcardConfig(
      WildcardType wildcardType, ThresholdRegexConfig thresholdRegexConfig) {
    return WildcardConfig.newBuilder()
        .setWildcardType(wildcardType)
        .addAllIdentificationRegexes(thresholdRegexConfig.getRegexList().getValuesList())
        .build();
  }

  private List<HttpApiNamingCustomRule> convertCustomRules(
      CustomRulesListConfig customRulesListConfig) {
    // TODO: Order the rules, once the priority is available from training config service APIs
    return customRulesListConfig.getCustomRulesConfigList().stream()
        .map(
            customRuleConfig ->
                HttpApiNamingCustomRule.newBuilder()
                    .setRegexPattern(customRuleConfig.getRegex())
                    .setUrlPattern(customRuleConfig.getUrlPattern())
                    .build())
        .collect(Collectors.toUnmodifiableList());
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

  @lombok.Value
  private static class HttpApiNamingConfigInfo {
    HttpApiNamingConfig httpApiNamingConfig;
    int embryonicThreshold;
  }

  @lombok.Value
  private static class FullTrieReloadConfigInfo {
    boolean disabled;
    long timestamp;
  }
}
