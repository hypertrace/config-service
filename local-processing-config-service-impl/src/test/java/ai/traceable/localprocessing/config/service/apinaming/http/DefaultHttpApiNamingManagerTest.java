package ai.traceable.localprocessing.config.service.apinaming.http;

import static ai.traceable.localprocessing.config.service.apinaming.http.ApiNamingManagerTestUtils.buildFullTrieReloadConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
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
import ai.traceable.localprocessing.config.service.v1.ApiNamingPattern;
import ai.traceable.localprocessing.config.service.v1.ApiNamingPatterns;
import ai.traceable.localprocessing.config.service.v1.DiffLog;
import ai.traceable.localprocessing.config.service.v1.DiffPattern;
import ai.traceable.localprocessing.config.service.v1.FullPattern;
import ai.traceable.localprocessing.config.service.v1.GetApiNamingModelRequest;
import ai.traceable.localprocessing.config.service.v1.HttpServiceResponse;
import ai.traceable.localprocessing.config.service.v1.ServiceRequest;
import ai.traceable.platform.apientity.http.difflog.TrieDiffLogModel;
import ai.traceable.platform.apientity.http.model.TrieModel;
import ai.traceable.platform.deepstore.FileMetadata;
import ai.traceable.platform.model.PersistedModel;
import ai.traceable.platform.model.store.ModelPersistentStore;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import org.hypertrace.config.utils.SpanFilterMatcher;
import org.hypertrace.core.grpcutils.context.RequestContext;
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
    when(httpApiNamingConfig.getDefaultMaxNumberOfTriePaths()).thenReturn(10000);
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
  void testGetServiceResponseList() throws ExecutionException {
    when(trieModel.getNonEmbryonicWildcardPaths(ApiNamingManagerTestUtils.builtTrieNodeConfig, 10))
        .thenReturn(new ArrayList<>());
    when(httpApiNamingConfig.getFullTrieReloadConfig())
        .thenReturn(buildFullTrieReloadConfig(false, "2.3.0"));
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

    when(entityFetcher.getServiceIds(
            any(), eq(List.of(serviceRequest1, serviceRequest2)), eq(Optional.of("environment"))))
        .thenReturn(
            Map.of(
                serviceRequest1,
                Optional.of("serviceId1"),
                serviceRequest2,
                Optional.of("serviceId2")));

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
                .setApiNamingPatterns(
                    ApiNamingPatterns.newBuilder()
                        .setToken("t=1;v=2.3.0")
                        .setFullPattern(FullPattern.newBuilder().build())
                        .build())
                .setHttpConfig(httpApiNamingConfig)
                .build()));
    assertTrue(
        actualServiceResponseList.contains(
            HttpServiceResponse.newBuilder()
                .setServiceName("serviceName2")
                .setApiNamingPatterns(
                    ApiNamingPatterns.newBuilder()
                        .setToken("t=1;v=2.3.0")
                        .setFullPattern(FullPattern.newBuilder().build())
                        .build())
                .setHttpConfig(
                    ai.traceable.localprocessing.config.service.v1.HttpApiNamingConfig.newBuilder()
                        .setHash(httpApiNamingConfig.getHash())
                        .build())
                .build()));
  }

  @Test
  void testTrieConstruction() throws ExecutionException {
    when(httpApiNamingConfig.getFullTrieReloadConfig())
        .thenReturn(buildFullTrieReloadConfig(false, "0.0.0"));
    when(trieModel.getNonEmbryonicWildcardPaths(any(), anyInt()))
        .thenReturn(ApiNamingManagerTestUtils.builtNonEmbryonicPaths);
    when(trieDiffLogModel.getTrieDiffLog())
        .thenReturn(ai.traceable.platform.apientity.TrieDiffLog.newBuilder().build());
    when(fileMetadata.getModificationTime()).thenReturn(2L);
    ai.traceable.localprocessing.config.service.v1.HttpApiNamingConfig httpApiNamingConfig =
        ApiNamingManagerTestUtils.buildApiNamingConfig();
    FullPattern expectedFullPattern = ApiNamingManagerTestUtils.builtExpectedFullPattern;
    ServiceRequest serviceRequest =
        ServiceRequest.newBuilder()
            .setServiceName("serviceName1")
            .setConfigHash(httpApiNamingConfig.getHash())
            .build();
    when(entityFetcher.getServiceIds(
            any(), eq(List.of(serviceRequest)), eq(Optional.of("environment"))))
        .thenReturn(Map.of(serviceRequest, Optional.of("serviceId1")));

    List<HttpServiceResponse> actualServiceResponseList =
        httpApiNamingManager.getHttpServiceResponseList(
            RequestContext.forTenantId("tenantId"),
            GetApiNamingModelRequest.newBuilder()
                .setEnvironment("environment")
                .addAllServiceRequests(List.of(serviceRequest))
                .build());

    assertEquals(1, actualServiceResponseList.size());
    List<ApiNamingPattern> actualPatterns =
        actualServiceResponseList
            .get(0)
            .getApiNamingPatterns()
            .getFullPattern()
            .getApiNamingPatternsList();
    assertEquals("t=2;v=0.0.0", actualServiceResponseList.get(0).getApiNamingPatterns().getToken());
    assertTrue(actualPatterns.contains(expectedFullPattern.getApiNamingPatterns(0)));
    assertTrue(actualPatterns.contains(expectedFullPattern.getApiNamingPatterns(1)));
  }

  @Test
  void testTrieDiffLogConstruction() throws ExecutionException {
    when(httpApiNamingConfig.getFullTrieReloadConfig())
        .thenReturn(buildFullTrieReloadConfig(false, "0.0.0"));
    when(trieDiffLogModel.getTrieDiffLog()).thenReturn(ApiNamingManagerTestUtils.builtTrieDiffLog);
    when(fileMetadata.getModificationTime()).thenReturn(3L);
    DiffPattern expectedDiffPattern = ApiNamingManagerTestUtils.builtExpectedDiffTrie;

    ServiceRequest serviceRequest1 =
        ServiceRequest.newBuilder()
            .setServiceName("serviceName1")
            .setConfigHash("")
            .setToken("t=1;v=0.0.0")
            .build();
    when(entityFetcher.getServiceIds(
            any(), eq(List.of(serviceRequest1)), eq(Optional.of("environment"))))
        .thenReturn(Map.of(serviceRequest1, Optional.of("serviceId1")));

    List<HttpServiceResponse> actualServiceResponseList =
        httpApiNamingManager.getHttpServiceResponseList(
            RequestContext.forTenantId("tenantId"),
            GetApiNamingModelRequest.newBuilder()
                .setEnvironment("environment")
                .addAllServiceRequests(List.of(serviceRequest1))
                .build());

    assertEquals(1, actualServiceResponseList.size());
    assertEquals("t=3;v=0.0.0", actualServiceResponseList.get(0).getApiNamingPatterns().getToken());
    List<DiffLog> actualDiffLogsList =
        actualServiceResponseList.get(0).getApiNamingPatterns().getDiffPattern().getDiffLogsList();
    assertTrue(actualDiffLogsList.contains(expectedDiffPattern.getDiffLogs(0)));
    assertTrue(actualDiffLogsList.contains(expectedDiffPattern.getDiffLogs(1)));
    assertTrue(actualDiffLogsList.contains(expectedDiffPattern.getDiffLogs(2)));
  }

  @Test
  void testLocalApiNamingConfig() throws ExecutionException {
    when(trieDiffLogModel.getTrieDiffLog()).thenReturn(ApiNamingManagerTestUtils.builtTrieDiffLog);
    when(fileMetadata.getModificationTime()).thenReturn(3L);
    when(trieModel.getNonEmbryonicWildcardPaths(any(), anyInt()))
        .thenReturn(ApiNamingManagerTestUtils.builtNonEmbryonicPaths);
    FullPattern expectedFullPattern = ApiNamingManagerTestUtils.builtExpectedFullPattern;

    ServiceRequest serviceRequest1 =
        ServiceRequest.newBuilder()
            .setServiceName("serviceName1")
            .setConfigHash("")
            .setToken("t=1;v=0.0.0")
            .build();

    when(entityFetcher.getServiceIds(
            any(), eq(List.of(serviceRequest1)), eq(Optional.of("environment"))))
        .thenReturn(Map.of(serviceRequest1, Optional.of("serviceId1")));
    when(httpApiNamingConfig.getFullTrieReloadConfig())
        .thenReturn(buildFullTrieReloadConfig(true, "1.0.0"));
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
        .thenReturn(buildFullTrieReloadConfig(false, "1.0.0"));
    actualServiceResponseList =
        httpApiNamingManager.getHttpServiceResponseList(
            RequestContext.forTenantId("tenantId"),
            GetApiNamingModelRequest.newBuilder()
                .setEnvironment("environment")
                .addAllServiceRequests(List.of(serviceRequest1))
                .build());

    assertEquals(1, actualServiceResponseList.size());
    assertEquals("t=3;v=1.0.0", actualServiceResponseList.get(0).getApiNamingPatterns().getToken());
    List<DiffLog> actualTrieDiffLogsList =
        actualServiceResponseList.get(0).getApiNamingPatterns().getDiffPattern().getDiffLogsList();
    assertEquals(0, actualTrieDiffLogsList.size());
    List<ApiNamingPattern> actualPatterns =
        actualServiceResponseList
            .get(0)
            .getApiNamingPatterns()
            .getFullPattern()
            .getApiNamingPatternsList();
    assertTrue(actualPatterns.contains(expectedFullPattern.getApiNamingPatterns(0)));
    assertTrue(actualPatterns.contains(expectedFullPattern.getApiNamingPatterns(1)));
  }
}
