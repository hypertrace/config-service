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
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
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
  void testHttpApiNamingResponse() throws InterruptedException {
    TrieModel trieModel = new TrieModel();
    TrieModelTrainerConfig trieModelTrainerConfig = buildTrieModelTrainerConfig();
    for (int i = 0; i < 10; i++) {
      // Medium cardinality threshold is 4
      trieModel.insert(trieModelTrainerConfig, "GET/sports/cricket");
      trieModel.insert(trieModelTrainerConfig, "GET/sports/hockey");
      trieModel.insert(trieModelTrainerConfig, "GET/sports/tennis");
      trieModel.insert(trieModelTrainerConfig, "GET/sports/badminton");
    }
    trieModel.train(buildTrieModelTrainerConfig());

    trieModelFileSystemModelStore.storeModel(
        new ServiceScope(TENANT_ID, createdEntity.getEntityId()), trieModel);
    GetApiNamingModelResponse getApiNamingModelResponse =
        getApiNamingModel(System.currentTimeMillis() - 1000, "1.2.3");

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
                            .setIdentificationRegex(".*")
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
      // Medium cardinality threshold is 4
      trieModel1.insert(trieModelTrainerConfig, "GET/fruits/apple");
      trieModel1.insert(trieModelTrainerConfig, "GET/fruits/mango");
      trieModel1.insert(trieModelTrainerConfig, "GET/fruits/peach");
      trieModel1.insert(trieModelTrainerConfig, "GET/fruits/banana");
    }
    trieModel1.train(buildTrieModelTrainerConfig());

    TrieDiffLogModel trieDiffLogModel = new TrieDiffLogModel();
    trieDiffLogModel.computeDiffLog(
        trieModel.getNonEmbryonicWildcardPaths(
            buildTrieModelTrainerConfig().getTrieNodeConfig(),
            buildTrieModelTrainerConfig().getMaxNumberOfTriePaths()),
        trieModel1.getNonEmbryonicWildcardPaths(
            buildTrieModelTrainerConfig().getTrieNodeConfig(),
            buildTrieModelTrainerConfig().getMaxNumberOfTriePaths()));
    long timestamp = System.currentTimeMillis() - 2500;
    DateScope dateScope =
        new DateScope(
            timestamp,
            new ServiceScope(TENANT_ID, createdEntity.getEntityId()),
            DATE_TIME_FORMATTER);
    trieDiffLogModelFileSystemModelStore.storeModel(dateScope, trieDiffLogModel);

    GetApiNamingModelResponse getApiNamingModelResponse1 =
        getApiNamingModel(System.currentTimeMillis() - 3000, "1.2.3");
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
                                    .setIdentificationRegex(".*")
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
                                    .setIdentificationRegex(".*")
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
    GetApiNamingModelResponse getApiNamingModelResponse2 =
        getApiNamingModel(System.currentTimeMillis() - 3000, "0.1.2");
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

    // Testing if max number of trie path nodes is honoured
    TrieModel trieModel2 = new TrieModel();
    for (int i = 0; i < 10; i++) {
      // Medium cardinality threshold is 4
      trieModel2.insert(trieModelTrainerConfig, "GET/sports/cricket");
      trieModel2.insert(trieModelTrainerConfig, "GET/sports/hockey");
      trieModel2.insert(trieModelTrainerConfig, "GET/sports/tennis");
      trieModel2.insert(trieModelTrainerConfig, "GET/sports/badminton");
    }

    TimeUnit.MICROSECONDS.sleep(250);
    for (int i = 0; i < 10; i++) {
      // Medium cardinality threshold is 4
      trieModel2.insert(trieModelTrainerConfig, "GET/city/London");
      trieModel2.insert(trieModelTrainerConfig, "GET/city/Tokyo");
      trieModel2.insert(trieModelTrainerConfig, "GET/city/Delhi");
      trieModel2.insert(trieModelTrainerConfig, "GET/city/Paris");
      trieModel2.insert(trieModelTrainerConfig, "GET/city/Madrid");
    }

    TimeUnit.MICROSECONDS.sleep(250);
    for (int i = 0; i < 10; i++) {
      // Medium cardinality threshold is 4
      trieModel2.insert(trieModelTrainerConfig, "GET/fruits/apple");
      trieModel2.insert(trieModelTrainerConfig, "GET/fruits/mango");
      trieModel2.insert(trieModelTrainerConfig, "GET/fruits/peach");
      trieModel2.insert(trieModelTrainerConfig, "GET/fruits/banana");
      trieModel2.insert(trieModelTrainerConfig, "GET/fruits/pineapple");
    }

    TimeUnit.MICROSECONDS.sleep(250);
    trieModel2.train(buildTrieModelTrainerConfig());

    List<List<ai.traceable.platform.apientity.Segment>> curr_paths =
        trieModel2.getNonEmbryonicWildcardPaths(
            buildTrieModelTrainerConfig().getTrieNodeConfig(),
            buildTrieModelTrainerConfig().getMaxNumberOfTriePaths());
    List<List<ai.traceable.platform.apientity.Segment>> previous_paths =
        trieModel.getNonEmbryonicWildcardPaths(
            buildTrieModelTrainerConfig().getTrieNodeConfig(),
            buildTrieModelTrainerConfig().getMaxNumberOfTriePaths());
    TrieDiffLogModel trieDiffLogModel1 = new TrieDiffLogModel();
    trieDiffLogModel1.computeDiffLog(previous_paths, curr_paths);

    // Added delay to not consider the previous diffLog
    TimeUnit.SECONDS.sleep(1);

    dateScope =
        new DateScope(
            System.currentTimeMillis(),
            new ServiceScope(TENANT_ID, createdEntity.getEntityId()),
            DATE_TIME_FORMATTER);
    trieDiffLogModelFileSystemModelStore.storeModel(dateScope, trieDiffLogModel1);

    GetApiNamingModelResponse getApiNamingModelResponse3 =
        getApiNamingModel(System.currentTimeMillis() - 1000, "1.2.3");

    List<DiffLog> diffLogs =
        getApiNamingModelResponse3
            .getHttpApiNamingResponse()
            .getHttpServiceResponses(0)
            .getApiNamingPatterns()
            .getDiffPattern()
            .getDiffLogsList();

    assertEquals(3, diffLogs.size());
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
                                    .setIdentificationRegex(".*")
                                    .setReplacementPattern("*")
                                    .build())
                            .build()))
            .build(),
        diffLogs.get(0));
    assertEquals(
        DiffLog.newBuilder()
            .setApiNamingPatternAddition(
                ApiNamingPattern.newBuilder()
                    .addSegments(Segment.newBuilder().setName("GET").build())
                    .addSegments(Segment.newBuilder().setName("city").build())
                    .addSegments(
                        Segment.newBuilder()
                            .setWildcard(
                                Wildcard.newBuilder()
                                    .setIdentificationRegex(".*")
                                    .setReplacementPattern("*")
                                    .build())
                            .build()))
            .build(),
        diffLogs.get(1));
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
                                    .setIdentificationRegex(".*")
                                    .setReplacementPattern("*")
                                    .build())
                            .build()))
            .build(),
        diffLogs.get(2));
    deleteDirectory(new File(BASE_DIR));
    deleteDirectory(new File(DIFF_LOGS_BASE_DIR));
  }

  private GetApiNamingModelResponse getApiNamingModel(long timestamp, String version) {
    GetApiNamingModelRequest request =
        GetApiNamingModelRequest.newBuilder()
            .addServiceRequests(
                ServiceRequest.newBuilder()
                    .setServiceName("serviceName")
                    .setToken("t=" + timestamp + ";v=" + version)
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
                                    .setThreshold(4)
                                    .setRegexList(StringList.newBuilder().addValues("d*").build())
                                    .build())
                            .setExtensions(StringList.newBuilder().addValues("e").build())
                            .setAllowRegexList(StringList.newBuilder().addValues("f").build())
                            .setEmbryonicThreshold(10)
                            .setMaxNumberOfTriePaths(2)
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
