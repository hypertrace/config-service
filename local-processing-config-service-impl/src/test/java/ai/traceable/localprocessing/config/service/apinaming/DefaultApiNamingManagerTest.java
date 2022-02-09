package ai.traceable.localprocessing.config.service.apinaming;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.anomaly.config.service.v1.trainer.TrainerConfigServiceGrpc.TrainerConfigServiceBlockingStub;
import ai.traceable.localprocessing.config.service.client.EntityDataServiceClient;
import ai.traceable.localprocessing.config.service.config.ApiNamingConfig;
import ai.traceable.localprocessing.config.service.utils.UuidGenerator;
import ai.traceable.localprocessing.config.service.v1.DiffTrie;
import ai.traceable.localprocessing.config.service.v1.FullTrie;
import ai.traceable.localprocessing.config.service.v1.GetApiNamingModelRequest;
import ai.traceable.localprocessing.config.service.v1.HttpApiNamingConfig;
import ai.traceable.localprocessing.config.service.v1.HttpServiceResponse;
import ai.traceable.localprocessing.config.service.v1.Node;
import ai.traceable.localprocessing.config.service.v1.ServiceRequest;
import ai.traceable.localprocessing.config.service.v1.Trie;
import ai.traceable.localprocessing.config.service.v1.TrieDiffLog;
import ai.traceable.platform.apientity.http.difflog.TrieDiffLogModel;
import ai.traceable.platform.apientity.http.model.TrieModel;
import ai.traceable.platform.deepstore.FileMetadata;
import ai.traceable.platform.model.PersistedModel;
import ai.traceable.platform.model.store.ModelPersistentStore;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.data.service.v1.Entity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DefaultApiNamingManagerTest {
  private static final String BASE_PATH = "/var/tmp";
  private static final String LOGS_DIRECTORY_NAME = "logs";

  private ApiNamingManager apiNamingManager;
  private EntityDataServiceClient entityDataServiceClient;
  private TrieDiffLogModel trieDiffLogModel;
  private TrieModel trieModel;
  private FileMetadata fileMetadata;

  @BeforeEach
  void setup() throws IOException {
    TrainerConfigServiceBlockingStub configServiceBlockingStub =
        mock(TrainerConfigServiceBlockingStub.class);
    entityDataServiceClient = mock(EntityDataServiceClient.class);
    UuidGenerator uuidGenerator = new UuidGenerator();
    ApiNamingConfig apiNamingConfig = mock(ApiNamingConfig.class);
    PersistedModel trieDiffLogPersistedModel = mock(PersistedModel.class);
    trieDiffLogModel = mock(TrieDiffLogModel.class);

    ModelPersistentStore trieModelStore = mock(ModelPersistentStore.class);
    ModelPersistentStore trieDiffLogModelStore = mock(ModelPersistentStore.class);
    PersistedModel persistedModel = mock(PersistedModel.class);
    fileMetadata = mock(FileMetadata.class);
    trieModel = mock(TrieModel.class);

    apiNamingManager =
        new DefaultApiNamingManager(
            configServiceBlockingStub,
            entityDataServiceClient,
            apiNamingConfig,
            trieModelStore,
            trieDiffLogModelStore,
            uuidGenerator);
    when(configServiceBlockingStub.getAllScopedTrainingConfigs(any()))
        .thenReturn(ApiNamingManagerTestUtils.buildGetAllScopedTrainingConfigsResponse());
    when(apiNamingConfig.getFallbackRegexes())
        .thenReturn(
            List.of(
                "(\\{){0,1}[0-9a-fA-F]{8}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{12}(\\}){0,1}",
                "\\d+"));

    when(trieDiffLogPersistedModel.getModel()).thenReturn(trieDiffLogModel);
    when(trieDiffLogPersistedModel.getMetadata()).thenReturn(fileMetadata);
    when(apiNamingConfig.getDefaultEmbryonicThreshold()).thenReturn(100);
    when(apiNamingConfig.getDiffLogsRetentionPeriod()).thenReturn(5L);
    when(apiNamingConfig.getBaseDirectory()).thenReturn("logs");
    when(trieDiffLogModelStore.loadModelsInDir(any(), any()))
        .thenReturn(Map.of("model", trieDiffLogPersistedModel));
    when(trieModelStore.loadModel(any())).thenReturn(persistedModel);
    when(trieModelStore.getModelMetadata(any())).thenReturn(fileMetadata);
    when(persistedModel.getModel()).thenReturn(trieModel);
  }

  @Test
  void testGetServiceResponseList() {
    when(trieModel.getNonEmbryonicPaths(ApiNamingManagerTestUtils.buildTrieNodeConfig()))
        .thenReturn(new HashSet<>());
    when(trieDiffLogModel.getTrieDiffLog())
        .thenReturn(ai.traceable.platform.apientity.TrieDiffLog.newBuilder().build());
    when(fileMetadata.getModificationTime()).thenReturn(1L);
    HttpApiNamingConfig httpApiNamingConfig = ApiNamingManagerTestUtils.buildApiNamingConfig();

    ServiceRequest serviceRequest1 =
        ServiceRequest.newBuilder().setServiceName("serviceName1").setConfigHash("").build();
    ServiceRequest serviceRequest2 =
        ServiceRequest.newBuilder()
            .setServiceName("serviceName2")
            .setConfigHash(httpApiNamingConfig.getHash())
            .build();
    when(entityDataServiceClient.getByTypeAndIdentifyingProperties(
            any(),
            eq(
                ApiNamingManagerTestUtils.buildGetEntityByTypeAndIdentifyingAttributesRequest(
                    "serviceName1"))))
        .thenReturn(Entity.newBuilder().setEntityId("serviceId1").build());
    when(entityDataServiceClient.getByTypeAndIdentifyingProperties(
            any(),
            eq(
                ApiNamingManagerTestUtils.buildGetEntityByTypeAndIdentifyingAttributesRequest(
                    "serviceName2"))))
        .thenReturn(Entity.newBuilder().setEntityId("serviceId2").build());

    List<HttpServiceResponse> actualServiceResponseList =
        apiNamingManager.getHttpServiceResponseList(
            RequestContext.forTenantId("tenantId"),
            GetApiNamingModelRequest.newBuilder()
                .setEnvironment("environment")
                .addAllServiceRequests(List.of(serviceRequest1, serviceRequest2))
                .build());
    assertEquals(2, actualServiceResponseList.size());
    assertTrue(
        actualServiceResponseList.contains(
            HttpServiceResponse.newBuilder()
                .setServiceName("serviceName1")
                .setTrie(
                    Trie.newBuilder()
                        .setToken("1")
                        .setFullTrie(FullTrie.newBuilder().build())
                        .build())
                .setHttpConfig(httpApiNamingConfig)
                .build()));
    assertTrue(
        actualServiceResponseList.contains(
            HttpServiceResponse.newBuilder()
                .setServiceName("serviceName2")
                .setTrie(
                    Trie.newBuilder()
                        .setToken("1")
                        .setFullTrie(FullTrie.newBuilder().build())
                        .build())
                .setHttpConfig(
                    HttpApiNamingConfig.newBuilder().setHash(httpApiNamingConfig.getHash()).build())
                .build()));
  }

  @Test
  void testTrieConstruction() {
    when(trieModel.getNonEmbryonicPaths(any()))
        .thenReturn(ApiNamingManagerTestUtils.buildNonEmbryonicPaths());
    when(trieDiffLogModel.getTrieDiffLog())
        .thenReturn(ai.traceable.platform.apientity.TrieDiffLog.newBuilder().build());
    when(fileMetadata.getModificationTime()).thenReturn(2L);
    ai.traceable.localprocessing.config.service.v1.HttpApiNamingConfig httpApiNamingConfig =
        ApiNamingManagerTestUtils.buildApiNamingConfig();
    FullTrie expectedFullTrie = ApiNamingManagerTestUtils.buildExpectedFullTrie();
    ServiceRequest serviceRequest =
        ServiceRequest.newBuilder()
            .setServiceName("serviceName1")
            .setConfigHash(httpApiNamingConfig.getHash())
            .build();
    when(entityDataServiceClient.getByTypeAndIdentifyingProperties(
            any(),
            eq(
                ApiNamingManagerTestUtils.buildGetEntityByTypeAndIdentifyingAttributesRequest(
                    "serviceName1"))))
        .thenReturn(Entity.newBuilder().setEntityId("serviceId1").build());

    List<HttpServiceResponse> actualServiceResponseList =
        apiNamingManager.getHttpServiceResponseList(
            RequestContext.forTenantId("tenantId"),
            GetApiNamingModelRequest.newBuilder()
                .setEnvironment("environment")
                .addAllServiceRequests(List.of(serviceRequest))
                .build());

    assertEquals(1, actualServiceResponseList.size());
    List<Node> actualNodes =
        actualServiceResponseList
            .get(0)
            .getTrie()
            .getFullTrie()
            .getRootsList()
            .get(0)
            .getChildrenList();
    assertEquals("2", actualServiceResponseList.get(0).getTrie().getToken());
    assertTrue(actualNodes.contains(expectedFullTrie.getRoots(0).getChildren(0)));
    assertTrue(actualNodes.contains(expectedFullTrie.getRoots(0).getChildren(1)));
  }

  @Test
  void testTrieDiffLogConstruction() {
    when(trieDiffLogModel.getTrieDiffLog())
        .thenReturn(ApiNamingManagerTestUtils.buildTrieDiffLog());
    when(fileMetadata.getModificationTime()).thenReturn(3L);
    DiffTrie expectedDiffTrie = ApiNamingManagerTestUtils.buildExpectedDiffTrie();

    ServiceRequest serviceRequest1 =
        ServiceRequest.newBuilder()
            .setServiceName("serviceName1")
            .setConfigHash("")
            .setTrieToken("1")
            .build();
    when(entityDataServiceClient.getByTypeAndIdentifyingProperties(
            any(),
            eq(
                ApiNamingManagerTestUtils.buildGetEntityByTypeAndIdentifyingAttributesRequest(
                    "serviceName1"))))
        .thenReturn(Entity.newBuilder().setEntityId("serviceId1").build());

    List<HttpServiceResponse> actualServiceResponseList =
        apiNamingManager.getHttpServiceResponseList(
            RequestContext.forTenantId("tenantId"),
            GetApiNamingModelRequest.newBuilder()
                .setEnvironment("environment")
                .addAllServiceRequests(List.of(serviceRequest1))
                .build());

    assertEquals(1, actualServiceResponseList.size());
    assertEquals("3", actualServiceResponseList.get(0).getTrie().getToken());
    List<TrieDiffLog> actualTrieDiffLogsList =
        actualServiceResponseList.get(0).getTrie().getDiffTrie().getTrieDiffLogsList();
    assertTrue(actualTrieDiffLogsList.contains(expectedDiffTrie.getTrieDiffLogs(0)));
    assertTrue(actualTrieDiffLogsList.contains(expectedDiffTrie.getTrieDiffLogs(1)));
    assertTrue(actualTrieDiffLogsList.contains(expectedDiffTrie.getTrieDiffLogs(2)));
  }
}
