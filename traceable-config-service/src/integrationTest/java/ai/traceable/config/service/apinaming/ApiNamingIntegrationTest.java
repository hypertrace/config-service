package ai.traceable.config.service.apinaming;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.anomaly.config.service.v1.StringList;
import ai.traceable.anomaly.config.service.v1.trainer.ApiNamingTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.ThresholdRegexConfig;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.TrieModelTrainingConfig;
import ai.traceable.config.service.TraceableConfigServiceIntegrationTestBase;
import ai.traceable.localprocessing.config.service.v1.ApiNamingPattern;
import ai.traceable.localprocessing.config.service.v1.DiffLog;
import ai.traceable.localprocessing.config.service.v1.GetApiNamingModelRequest;
import ai.traceable.localprocessing.config.service.v1.GetApiNamingModelResponse;
import ai.traceable.localprocessing.config.service.v1.LocalProcessingConfigServiceGrpc;
import ai.traceable.localprocessing.config.service.v1.Segment;
import ai.traceable.localprocessing.config.service.v1.ServiceRequest;
import ai.traceable.localprocessing.config.service.v1.Wildcard;
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
      trieModel.insert(trieModelTrainerConfig, "GET/sports/hockey");
      trieModel.insert(trieModelTrainerConfig, "GET/sports/tennis");
    }
    trieModel.train(buildTrieModelTrainerConfig());

    trieModelFileSystemModelStore.storeModel(
        new ServiceScope(TENANT_ID, createdEntity.getEntityId()), trieModel);
    GetApiNamingModelResponse getApiNamingModelResponse = getApiNamingModel("1.2.3");

    // Since there would be only one path GET/sports/*
    assertEquals(
        1,
        getApiNamingModelResponse
            .getHttpApiNamingResponse()
            .getHttpServiceResponses(0)
            .getApiNamingPatterns()
            .getFullPattern()
            .getApiNamingPatternsCount());
    assertEquals(
        ApiNamingPattern.newBuilder()
            .addSegments(Segment.newBuilder().setName("GET").build())
            .addSegments(Segment.newBuilder().setName("sports").build())
            .addSegments(
                Segment.newBuilder()
                    .setWildcard(
                        Wildcard.newBuilder()
                            .setIdentificationRegex("")
                            .setReplacementPattern("*")
                            .build())
                    .build())
            .build(),
        getApiNamingModelResponse
            .getHttpApiNamingResponse()
            .getHttpServiceResponses(0)
            .getApiNamingPatterns()
            .getFullPattern()
            .getApiNamingPatterns(0));

    TrieModel trieModel1 = new TrieModel();
    for (int i = 0; i < 10; i++) {
      trieModel1.insert(trieModelTrainerConfig, "GET/fruits/apple");
      trieModel1.insert(trieModelTrainerConfig, "GET/fruits/mango");
      trieModel1.insert(trieModelTrainerConfig, "GET/fruits/peach");
    }
    trieModel1.train(buildTrieModelTrainerConfig());

    TrieDiffLogModel trieDiffLogModel = new TrieDiffLogModel();
    trieDiffLogModel.computeDiffLog(
        trieModel.getNonEmbryonicWildcardPaths(buildTrieModelTrainerConfig().getTrieNodeConfig()),
        trieModel1.getNonEmbryonicWildcardPaths(buildTrieModelTrainerConfig().getTrieNodeConfig()));
    long timestamp = System.currentTimeMillis() - 10;
    DateScope dateScope =
        new DateScope(
            timestamp,
            new ServiceScope(TENANT_ID, createdEntity.getEntityId()),
            DATE_TIME_FORMATTER);
    trieDiffLogModelFileSystemModelStore.storeModel(dateScope, trieDiffLogModel);

    GetApiNamingModelResponse getApiNamingModelResponse1 = getApiNamingModel("1.2.3");
    assertEquals(
        2,
        getApiNamingModelResponse1
            .getHttpApiNamingResponse()
            .getHttpServiceResponses(0)
            .getApiNamingPatterns()
            .getDiffPattern()
            .getDiffLogsCount());

    assertEquals(
        DiffLog.newBuilder()
            .setApiNamingPatternAddition(
                ApiNamingPattern.newBuilder()
                    .addSegments(Segment.newBuilder().setName("GET").build())
                    .addSegments(Segment.newBuilder().setName("fruits").build())
                    .addSegments(
                        Segment.newBuilder()
                            .setWildcard(
                                Wildcard.newBuilder()
                                    .setIdentificationRegex("")
                                    .setReplacementPattern("*")
                                    .build())
                            .build()))
            .build(),
        getApiNamingModelResponse1
            .getHttpApiNamingResponse()
            .getHttpServiceResponses(0)
            .getApiNamingPatterns()
            .getDiffPattern()
            .getDiffLogs(0));

    assertEquals(
        DiffLog.newBuilder()
            .setApiNamingPatternDeletion(
                ApiNamingPattern.newBuilder()
                    .addSegments(Segment.newBuilder().setName("GET").build())
                    .addSegments(Segment.newBuilder().setName("sports").build())
                    .addSegments(
                        Segment.newBuilder()
                            .setWildcard(
                                Wildcard.newBuilder()
                                    .setIdentificationRegex("")
                                    .setReplacementPattern("*")
                                    .build())
                            .build()))
            .build(),
        getApiNamingModelResponse1
            .getHttpApiNamingResponse()
            .getHttpServiceResponses(0)
            .getApiNamingPatterns()
            .getDiffPattern()
            .getDiffLogs(1));

    // Forced full trie in-case the version is changed
    var getApiNamingModelResponse2 = getApiNamingModel("0.1.2");
    assertEquals(
        0,
        getApiNamingModelResponse2
            .getHttpApiNamingResponse()
            .getHttpServiceResponses(0)
            .getApiNamingPatterns()
            .getDiffPattern()
            .getDiffLogsCount());
    assertEquals(
        1,
        getApiNamingModelResponse
            .getHttpApiNamingResponse()
            .getHttpServiceResponses(0)
            .getApiNamingPatterns()
            .getFullPattern()
            .getApiNamingPatternsCount());

    deleteDirectory(new File(BASE_DIR));
    deleteDirectory(new File(DIFF_LOGS_BASE_DIR));
  }

  private GetApiNamingModelResponse getApiNamingModel(String version) {
    GetApiNamingModelRequest request =
        GetApiNamingModelRequest.newBuilder()
            .addServiceRequests(
                ServiceRequest.newBuilder()
                    .setServiceName("serviceName")
                    .setToken("t=" + (System.currentTimeMillis() - 10000) + ";v=" + version)
                    .build())
            .build();
    return RequestContext.forTenantId(TENANT_ID)
        .call(() -> localProcessingConfigStub.getApiNamingModel(request));
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
        TrainingConfig.newBuilder()
            .setDisabled(false)
            .setApiNamingTrainingConfig(
                ApiNamingTrainingConfig.newBuilder()
                    .setTrieModelTrainingConfig(
                        TrieModelTrainingConfig.newBuilder()
                            .setIds(
                                ThresholdRegexConfig.newBuilder()
                                    .setThreshold(10)
                                    .setRegexList(StringList.newBuilder().addValues("a*").build())
                                    .build())
                            .setLowCardinality(
                                ThresholdRegexConfig.newBuilder()
                                    .setThreshold(10)
                                    .setRegexList(StringList.newBuilder().addValues("b*").build())
                                    .build())
                            .setHighCardinality(
                                ThresholdRegexConfig.newBuilder()
                                    .setThreshold(10)
                                    .setRegexList(StringList.newBuilder().addValues("c*").build())
                                    .build())
                            .setMediumCardinality(
                                ThresholdRegexConfig.newBuilder()
                                    .setThreshold(3)
                                    .setRegexList(StringList.newBuilder().addValues("d*").build())
                                    .build())
                            .setExtensions(StringList.newBuilder().addValues("e").build())
                            .setAllowRegexList(StringList.newBuilder().addValues("f").build())
                            .setEmbryonicThreshold(10)
                            .build())
                    .build())
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
