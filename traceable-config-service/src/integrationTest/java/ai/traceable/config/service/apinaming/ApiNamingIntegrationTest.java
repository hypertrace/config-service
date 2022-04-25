package ai.traceable.config.service.apinaming;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.anomaly.config.service.v1.StringList;
import ai.traceable.anomaly.config.service.v1.trainer.ThresholdRegexConfig;
import ai.traceable.anomaly.config.service.v1.trainer.TrieModelTrainingConfig;
import ai.traceable.config.service.TraceableConfigServiceIntegrationTestBase;
import ai.traceable.localprocessing.config.service.v1.DiffTrie;
import ai.traceable.localprocessing.config.service.v1.FullTrie;
import ai.traceable.localprocessing.config.service.v1.GetApiNamingModelRequest;
import ai.traceable.localprocessing.config.service.v1.GetApiNamingModelResponse;
import ai.traceable.localprocessing.config.service.v1.HttpApiNamingConfig;
import ai.traceable.localprocessing.config.service.v1.LocalProcessingConfigServiceGrpc;
import ai.traceable.localprocessing.config.service.v1.Node;
import ai.traceable.localprocessing.config.service.v1.ServiceRequest;
import ai.traceable.localprocessing.config.service.v1.TrieDiffLog;
import ai.traceable.localprocessing.config.service.v1.TrieNodePath;
import ai.traceable.localprocessing.config.service.v1.WildcardType;
import ai.traceable.platform.apientity.http.difflog.TrieDiffLogModel;
import ai.traceable.platform.apientity.http.model.TrieModel;
import ai.traceable.platform.apientity.http.model.TrieModelTrainerConfig;
import ai.traceable.platform.model.store.DateScope;
import ai.traceable.platform.model.store.FileSystemModelStore;
import ai.traceable.platform.model.store.ServiceScope;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.constants.v1.CommonAttribute;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.Value;
import org.hypertrace.entity.service.constants.EntityConstants;
import org.hypertrace.entity.type.service.v1.AttributeKind;
import org.hypertrace.entity.type.service.v1.AttributeType;
import org.hypertrace.entity.type.service.v1.EntityType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ApiNamingIntegrationTest extends TraceableConfigServiceIntegrationTestBase {
  private static final String TENANT_ID = "tenant1";
  private static GrpcChannelRegistry channelRegistry;
  private static FileSystemModelStore trieModelFileSystemModelStore;
  private static FileSystemModelStore trieDiffLogModelFileSystemModelStore;
  private static final String BASE_DIR = "/tmp/models";
  private static final String DIFF_LOGS_BASE_DIR = "/tmp/difflogs";
  private static final DateTimeFormatter DATE_TIME_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH-mm-ss");

  private static LocalProcessingConfigServiceGrpc.LocalProcessingConfigServiceBlockingStub
      localProcessingConfigStub;
  private static Entity createdEntity;
  private static EntityServiceClient entityServiceClient;

  @BeforeAll
  static void init() {
    channelRegistry = new GrpcChannelRegistry();
    localProcessingConfigStub =
        LocalProcessingConfigServiceGrpc.newBlockingStub(managedChannelForExternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
    entityServiceClient =
        new EntityServiceClient(new EntityServiceConfig(buildConfig()), channelRegistry);
    setUpEntityTypes();
    setUpEntities();
    trieModelFileSystemModelStore = new FileSystemModelStore(TrieModel.class);
    trieModelFileSystemModelStore.init(buildFileSystemConfig(BASE_DIR));
    trieDiffLogModelFileSystemModelStore = new FileSystemModelStore(TrieDiffLogModel.class);
    trieDiffLogModelFileSystemModelStore.init(buildFileSystemConfig(DIFF_LOGS_BASE_DIR));
  }

  private static void setUpEntityTypes() {
    entityServiceClient.upsertEntityType(
        RequestContext.forTenantId(TENANT_ID),
        EntityType.newBuilder()
            .setName("SERVICE")
            .setTenantId(TENANT_ID)
            .addAttributeType(
                AttributeType.newBuilder()
                    .setName(EntityConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_FQN))
                    .setIdentifyingAttribute(true)
                    .setValueKind(AttributeKind.TYPE_STRING)
                    .build())
            .build());
  }

  private static void setUpEntities() {
    Entity entity =
        Entity.newBuilder()
            .setTenantId(TENANT_ID)
            .setEntityType("SERVICE")
            .setEntityId("id")
            .setEntityName("serviceName")
            .putIdentifyingAttributes(
                EntityConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_FQN),
                AttributeValue.newBuilder()
                    .setValue(Value.newBuilder().setString("serviceName").build())
                    .build())
            .build();
    createdEntity = entityServiceClient.upsertEntity(RequestContext.forTenantId(TENANT_ID), entity);
  }

  @Test
  void testHttpApiNamingResponse() {
    TrieModel trieModel = new TrieModel();
    TrieModelTrainerConfig trieModelTrainerConfig = buildTrieModelTrainerConfig();
    for (int i = 0; i < 10; i++) {
      trieModel.insert(trieModelTrainerConfig, "GET/sports/cricket");
    }
    trieModelFileSystemModelStore.storeModel(
        new ServiceScope(TENANT_ID, createdEntity.getEntityId()), trieModel);
    GetApiNamingModelResponse getApiNamingModelResponse = getApiNamingModel("1.2.3");
    testHttpApiNamingConfig(
        getApiNamingModelResponse
            .getHttpApiNamingResponse()
            .getHttpServiceResponses(0)
            .getHttpConfig());
    testFullTrie(
        getApiNamingModelResponse
            .getHttpApiNamingResponse()
            .getHttpServiceResponses(0)
            .getTrie()
            .getFullTrie());

    TrieModel trieModel1 = new TrieModel();
    for (int i = 0; i < 10; i++) {
      trieModel1.insert(trieModelTrainerConfig, "GET/sports/tennis");
    }
    TrieDiffLogModel trieDiffLogModel1 = new TrieDiffLogModel();
    trieDiffLogModel1.computeDiffLog(
        trieModel, trieModel1, buildTrieModelTrainerConfig().getTrieNodeConfig());
    DateScope dateScope1 =
        new DateScope(
            System.currentTimeMillis() - 10,
            new ServiceScope(TENANT_ID, createdEntity.getEntityId()),
            DATE_TIME_FORMATTER);
    trieDiffLogModelFileSystemModelStore.storeModel(dateScope1, trieDiffLogModel1);
    getApiNamingModelResponse = getApiNamingModel("1.2.3");
    testDiffTrie(
        getApiNamingModelResponse
            .getHttpApiNamingResponse()
            .getHttpServiceResponses(0)
            .getTrie()
            .getDiffTrie());

    // Forced full trie in-case the version is changed
    getApiNamingModelResponse = getApiNamingModel("1.0.0");
    testFullTrie(
        getApiNamingModelResponse
            .getHttpApiNamingResponse()
            .getHttpServiceResponses(0)
            .getTrie()
            .getFullTrie());

    deleteDirectory(new File(BASE_DIR));
    deleteDirectory(new File(DIFF_LOGS_BASE_DIR));
  }

  private GetApiNamingModelResponse getApiNamingModel(String version) {
    GetApiNamingModelRequest request =
        GetApiNamingModelRequest.newBuilder()
            .addServiceRequests(
                ServiceRequest.newBuilder()
                    .setServiceName("serviceName")
                    .setTrieToken("t=" + (System.currentTimeMillis() - 1000) + ";v=" + version)
                    .build())
            .build();
    return RequestContext.forTenantId(TENANT_ID)
        .call(() -> localProcessingConfigStub.getApiNamingModel(request));
  }

  private static void testDiffTrie(DiffTrie diffTrie) {
    assertTrue(
        diffTrie
            .getTrieDiffLogsList()
            .contains(
                TrieDiffLog.newBuilder()
                    .setNodeRemoval(
                        TrieNodePath.newBuilder()
                            .addAllValues(
                                List.of(
                                    ai.traceable.localprocessing.config.service.v1.Value
                                        .newBuilder()
                                        .setName("3")
                                        .build(),
                                    ai.traceable.localprocessing.config.service.v1.Value
                                        .newBuilder()
                                        .setName("GET")
                                        .build(),
                                    ai.traceable.localprocessing.config.service.v1.Value
                                        .newBuilder()
                                        .setName("sports")
                                        .build(),
                                    ai.traceable.localprocessing.config.service.v1.Value
                                        .newBuilder()
                                        .setName("cricket")
                                        .build()))
                            .build())
                    .build()));
    assertTrue(
        diffTrie
            .getTrieDiffLogsList()
            .contains(
                TrieDiffLog.newBuilder()
                    .setPathAddition(
                        TrieNodePath.newBuilder()
                            .addAllValues(
                                List.of(
                                    ai.traceable.localprocessing.config.service.v1.Value
                                        .newBuilder()
                                        .setName("3")
                                        .build(),
                                    ai.traceable.localprocessing.config.service.v1.Value
                                        .newBuilder()
                                        .setName("GET")
                                        .build(),
                                    ai.traceable.localprocessing.config.service.v1.Value
                                        .newBuilder()
                                        .setName("sports")
                                        .build(),
                                    ai.traceable.localprocessing.config.service.v1.Value
                                        .newBuilder()
                                        .setName("tennis")
                                        .build()))
                            .build())
                    .build()));
  }

  private static void testFullTrie(FullTrie fullTrie) {
    Node node = fullTrie.getRoots(0);
    assertEquals("3", node.getValue().getName());
    node = node.getChildren(0);
    assertEquals("GET", node.getValue().getName());
    node = node.getChildren(0);
    assertEquals("sports", node.getValue().getName());
    node = node.getChildren(0);
    assertEquals("cricket", node.getValue().getName());
  }

  private static void testHttpApiNamingConfig(HttpApiNamingConfig httpApiNamingConfig) {
    assertEquals(List.of("v\\d+"), httpApiNamingConfig.getSegmentWhitelistRegexesList());
    assertEquals(
        List.of(
            "(\\{){0,1}[0-9a-fA-F]{8}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{12}(\\}){0,1}",
            "\\d+"),
        httpApiNamingConfig.getWildcardConfigsList().stream()
            .filter(
                wildcardConfig ->
                    wildcardConfig.getWildcardType().equals(WildcardType.WILDCARD_TYPE_ID))
            .findAny()
            .get()
            .getIdentificationRegexesList());

    assertEquals(
        List.of(),
        httpApiNamingConfig.getWildcardConfigsList().stream()
            .filter(
                wildcardConfig ->
                    wildcardConfig
                        .getWildcardType()
                        .equals(WildcardType.WILDCARD_TYPE_LOW_CARDINALITY))
            .findAny()
            .get()
            .getIdentificationRegexesList());
    assertEquals(
        List.of("[a-zA-Z]*?([-_+]?[a-zA-Z]+)+[-_+]?"),
        httpApiNamingConfig.getWildcardConfigsList().stream()
            .filter(
                wildcardConfig ->
                    wildcardConfig
                        .getWildcardType()
                        .equals(WildcardType.WILDCARD_TYPE_HIGH_CARDINALITY))
            .findAny()
            .get()
            .getIdentificationRegexesList());
    assertEquals(List.of("json", "xml"), httpApiNamingConfig.getExtensionsList());
  }

  private static Config buildConfig() {
    Map<String, Object> configMap = new HashMap<>();
    Map<String, Object> edsConfigMap = new HashMap<>();
    edsConfigMap.put("host", "localhost");
    edsConfigMap.put("port", 60061);
    configMap.put("entity.service.config", edsConfigMap);
    return ConfigFactory.parseMap(configMap);
  }

  private static Config buildFileSystemConfig(String baseDir) {
    Map<String, Object> fsConfig = new HashMap<>();
    fsConfig.put("deep.store.fs.scheme", "file");
    fsConfig.put("directory", baseDir);
    return ConfigFactory.parseMap(fsConfig);
  }

  private static TrieModelTrainerConfig buildTrieModelTrainerConfig() {
    return new TrieModelTrainerConfig(
        TrieModelTrainingConfig.newBuilder()
            .setIds(
                ThresholdRegexConfig.newBuilder()
                    .setThreshold(1)
                    .setRegexList(StringList.newBuilder().addValues("a").build())
                    .build())
            .setLowCardinality(
                ThresholdRegexConfig.newBuilder()
                    .setThreshold(1)
                    .setRegexList(StringList.newBuilder().addValues("b").build())
                    .build())
            .setHighCardinality(
                ThresholdRegexConfig.newBuilder()
                    .setThreshold(1)
                    .setRegexList(StringList.newBuilder().addValues("c").build())
                    .build())
            .setMediumCardinality(
                ThresholdRegexConfig.newBuilder()
                    .setThreshold(1)
                    .setRegexList(StringList.newBuilder().addValues("d").build())
                    .build())
            .setExtensions(StringList.newBuilder().addValues("e").build())
            .setAllowRegexList(StringList.newBuilder().addValues("f").build())
            .setEmbryonicThreshold(10)
            .build(),
        ConfigFactory.parseMap(Map.of()));
  }

  private void deleteDirectory(File file) {
    File[] contents = file.listFiles();
    if (contents != null) {
      for (File f : contents) {
        deleteDirectory(f);
      }
    }
    file.delete();
  }
}
