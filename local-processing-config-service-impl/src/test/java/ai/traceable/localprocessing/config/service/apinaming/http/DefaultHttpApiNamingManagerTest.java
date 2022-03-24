package ai.traceable.localprocessing.config.service.apinaming.http;

import static ai.traceable.localprocessing.config.service.apinaming.http.ApiNamingManagerTestUtils.buildFullTrieReloadConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.anomaly.config.service.v1.trainer.TrainerConfigServiceGrpc.TrainerConfigServiceBlockingStub;
import ai.traceable.localprocessing.config.service.apinaming.http.namingconfig.DefaultHttpApiNamingConfigManager;
import ai.traceable.localprocessing.config.service.apinaming.http.namingconfig.HttpApiNamingCachedConfigManager;
import ai.traceable.localprocessing.config.service.apinaming.http.namingconfig.HttpCustomApiNamingRulesManager;
import ai.traceable.localprocessing.config.service.apinaming.http.trie.DefaultHttpApiNamingTrieManager;
import ai.traceable.localprocessing.config.service.apinaming.http.trie.FullTrieManager;
import ai.traceable.localprocessing.config.service.apinaming.http.trie.TrieDiffLogManager;
import ai.traceable.localprocessing.config.service.apinaming.http.utils.SegmentConverter;
import ai.traceable.localprocessing.config.service.config.http.HttpApiNamingConfig;
import ai.traceable.localprocessing.config.service.utils.UuidGenerator;
import ai.traceable.localprocessing.config.service.v1.DiffTrie;
import ai.traceable.localprocessing.config.service.v1.FullTrie;
import ai.traceable.localprocessing.config.service.v1.GetApiNamingModelRequest;
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
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.config.utils.SpanFilterMatcher;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.span.processing.config.service.v1.SpanProcessingConfigServiceGrpc;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DefaultHttpApiNamingManagerTest {

  private HttpApiNamingManager httpApiNamingManager;
  private TrieDiffLogModel trieDiffLogModel;
  private TrieModel trieModel;
  private FileMetadata fileMetadata;
  private HttpApiNamingConfig httpApiNamingConfig;
  private EntityFetcher entityFetcher;

  @BeforeEach
  void setup() throws IOException {
    TrainerConfigServiceBlockingStub trainerConfigServiceBlockingStub =
        mock(TrainerConfigServiceBlockingStub.class);
    SpanProcessingConfigServiceGrpc.SpanProcessingConfigServiceBlockingStub
        spanProcessingConfigServiceBlockingStub =
            mock(SpanProcessingConfigServiceGrpc.SpanProcessingConfigServiceBlockingStub.class);
    UuidGenerator uuidGenerator = new UuidGenerator();
    httpApiNamingConfig = mock(HttpApiNamingConfig.class);

    PersistedModel trieDiffLogPersistedModel = mock(PersistedModel.class);
    trieDiffLogModel = mock(TrieDiffLogModel.class);

    ModelPersistentStore trieModelStore = mock(ModelPersistentStore.class);
    ModelPersistentStore trieDiffLogModelStore = mock(ModelPersistentStore.class);
    PersistedModel persistedModel = mock(PersistedModel.class);
    fileMetadata = mock(FileMetadata.class);
    trieModel = mock(TrieModel.class);
    entityFetcher = mock(EntityFetcher.class);
    SegmentConverter segmentConverter = new SegmentConverter();
    SpanFilterMatcher spanFilterMatcher = new SpanFilterMatcher();

    httpApiNamingManager =
        new DefaultHttpApiNamingManager(
            new DefaultHttpApiNamingConfigManager(httpApiNamingConfig, uuidGenerator),
            new DefaultHttpApiNamingTrieManager(
                httpApiNamingConfig,
                new FullTrieManager(
                    ConfigFactory.parseMap(Map.of()), trieModelStore, segmentConverter),
                new TrieDiffLogManager(
                    ConfigFactory.parseMap(Map.of()),
                    trieDiffLogModelStore,
                    httpApiNamingConfig,
                    segmentConverter)),
            new HttpApiNamingCachedConfigManager(
                ConfigFactory.parseMap(Map.of()), trainerConfigServiceBlockingStub),
            new HttpCustomApiNamingRulesManager(
                ConfigFactory.parseMap(Map.of()),
                spanProcessingConfigServiceBlockingStub,
                spanFilterMatcher),
            new LocalApiNamingConfigManager(httpApiNamingConfig),
            entityFetcher);
    when(trainerConfigServiceBlockingStub.getAllScopedTrainingConfigs(any()))
        .thenReturn(ApiNamingManagerTestUtils.buildGetAllScopedTrainingConfigsResponse());
    when(httpApiNamingConfig.getFallbackRegexes())
        .thenReturn(
            List.of(
                "(\\{){0,1}[0-9a-fA-F]{8}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{12}(\\}){0,1}",
                "\\d+"));

    when(trieDiffLogPersistedModel.getModel()).thenReturn(trieDiffLogModel);
    when(trieDiffLogPersistedModel.getMetadata()).thenReturn(fileMetadata);
    when(httpApiNamingConfig.getDefaultEmbryonicThreshold()).thenReturn(100);
    when(httpApiNamingConfig.getDiffLogsRetentionPeriod()).thenReturn(5L);
    when(httpApiNamingConfig.getBaseDirectory()).thenReturn("logs");
    when(trieDiffLogModelStore.loadModelsInDir(any(), any()))
        .thenReturn(Map.of("model", trieDiffLogPersistedModel));
    when(trieModelStore.loadModel(any())).thenReturn(persistedModel);
    when(trieModelStore.getModelMetadata(any())).thenReturn(fileMetadata);
    when(persistedModel.getModel()).thenReturn(trieModel);
    when(spanProcessingConfigServiceBlockingStub.getAllApiNamingRules(any()))
        .thenReturn(ApiNamingManagerTestUtils.buildGetAllApiNamingRuleResponse());
  }

  @Test
  void testGetServiceResponseList() {
    when(trieModel.getNonEmbryonicPaths(ApiNamingManagerTestUtils.buildTrieNodeConfig()))
        .thenReturn(new HashSet<>());
    when(httpApiNamingConfig.getFullTrieReloadConfig())
        .thenReturn(buildFullTrieReloadConfig(false, "2022-03-09T13:36:33Z"));
    when(trieDiffLogModel.getTrieDiffLog())
        .thenReturn(ai.traceable.platform.apientity.TrieDiffLog.newBuilder().build());
    when(fileMetadata.getModificationTime()).thenReturn(1L);
    ai.traceable.localprocessing.config.service.v1.HttpApiNamingConfig httpApiNamingConfig =
        ApiNamingManagerTestUtils.buildApiNamingConfig();

    ServiceRequest serviceRequest1 =
        ServiceRequest.newBuilder().setServiceName("serviceName1").setConfigHash("").build();
    ServiceRequest serviceRequest2 =
        ServiceRequest.newBuilder()
            .setServiceName("serviceName2")
            .setConfigHash(httpApiNamingConfig.getHash())
            .build();

    when(entityFetcher.getEntity(any(), eq("serviceName1"), eq(Optional.of("environment"))))
        .thenReturn(Optional.of(Entity.newBuilder().setEntityId("serviceId1").build()));
    when(entityFetcher.getEntity(any(), eq("serviceName2"), eq(Optional.of("environment"))))
        .thenReturn(Optional.of(Entity.newBuilder().setEntityId("serviceId2").build()));

    List<HttpServiceResponse> actualServiceResponseList =
        httpApiNamingManager.getHttpServiceResponseList(
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
                    ai.traceable.localprocessing.config.service.v1.HttpApiNamingConfig.newBuilder()
                        .setHash(httpApiNamingConfig.getHash())
                        .build())
                .build()));
  }

  @Test
  void testTrieConstruction() {
    when(httpApiNamingConfig.getFullTrieReloadConfig())
        .thenReturn(buildFullTrieReloadConfig(false, "2022-03-09T13:36:33Z"));
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
    when(entityFetcher.getEntity(any(), eq("serviceName1"), eq(Optional.of("environment"))))
        .thenReturn(Optional.of(Entity.newBuilder().setEntityId("serviceId1").build()));

    List<HttpServiceResponse> actualServiceResponseList =
        httpApiNamingManager.getHttpServiceResponseList(
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
    when(httpApiNamingConfig.getFullTrieReloadConfig())
        .thenReturn(buildFullTrieReloadConfig(false, "1970-01-01T00:00:00.000Z"));
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
    when(entityFetcher.getEntity(any(), eq("serviceName1"), eq(Optional.of("environment"))))
        .thenReturn(Optional.of(Entity.newBuilder().setEntityId("serviceId1").build()));

    List<HttpServiceResponse> actualServiceResponseList =
        httpApiNamingManager.getHttpServiceResponseList(
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

  @Test
  void testLocalApiNamingConfig() {
    when(trieDiffLogModel.getTrieDiffLog())
        .thenReturn(ApiNamingManagerTestUtils.buildTrieDiffLog());
    when(fileMetadata.getModificationTime()).thenReturn(3L);
    when(trieModel.getNonEmbryonicPaths(any()))
        .thenReturn(ApiNamingManagerTestUtils.buildNonEmbryonicPaths());
    FullTrie expectedFullTrie = ApiNamingManagerTestUtils.buildExpectedFullTrie();

    ServiceRequest serviceRequest1 =
        ServiceRequest.newBuilder()
            .setServiceName("serviceName1")
            .setConfigHash("")
            .setTrieToken("1")
            .build();
    when(entityFetcher.getEntity(any(), eq("serviceName1"), eq(Optional.of("environment"))))
        .thenReturn(Optional.of(Entity.newBuilder().setEntityId("serviceId1").build()));

    when(httpApiNamingConfig.getFullTrieReloadConfig())
        .thenReturn(buildFullTrieReloadConfig(true, "2022-03-09T13:36:33Z"));
    List<HttpServiceResponse> actualServiceResponseList =
        httpApiNamingManager.getHttpServiceResponseList(
            RequestContext.forTenantId("tenantId"),
            GetApiNamingModelRequest.newBuilder()
                .setEnvironment("environment")
                .addAllServiceRequests(List.of(serviceRequest1))
                .build());
    // disabled local api naming config
    assertEquals(0, actualServiceResponseList.size());

    when(httpApiNamingConfig.getFullTrieReloadConfig())
        .thenReturn(buildFullTrieReloadConfig(false, "2022-03-09T13:36:33Z"));
    actualServiceResponseList =
        httpApiNamingManager.getHttpServiceResponseList(
            RequestContext.forTenantId("tenantId"),
            GetApiNamingModelRequest.newBuilder()
                .setEnvironment("environment")
                .addAllServiceRequests(List.of(serviceRequest1))
                .build());

    assertEquals(1, actualServiceResponseList.size());
    assertEquals("3", actualServiceResponseList.get(0).getTrie().getToken());
    List<TrieDiffLog> actualTrieDiffLogsList =
        actualServiceResponseList.get(0).getTrie().getDiffTrie().getTrieDiffLogsList();
    assertEquals(0, actualTrieDiffLogsList.size());
    List<Node> actualNodes =
        actualServiceResponseList
            .get(0)
            .getTrie()
            .getFullTrie()
            .getRootsList()
            .get(0)
            .getChildrenList();
    assertTrue(actualNodes.contains(expectedFullTrie.getRoots(0).getChildren(0)));
    assertTrue(actualNodes.contains(expectedFullTrie.getRoots(0).getChildren(1)));
  }
}
