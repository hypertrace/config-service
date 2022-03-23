package ai.traceable.config.service.anomaly;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.anomaly.config.service.v1.AnomalyApiScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.AnomalyParamScope;
import ai.traceable.anomaly.config.service.v1.AnomalyServiceScope;
import ai.traceable.anomaly.config.service.v1.StringList;
import ai.traceable.anomaly.config.service.v1.trainer.ApiNamingTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.ContentSizeTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.GetAllScopedTrainingConfigsRequest;
import ai.traceable.anomaly.config.service.v1.trainer.GetAllTrainingActionsRequest;
import ai.traceable.anomaly.config.service.v1.trainer.GetScopedTrainingConfigRequest;
import ai.traceable.anomaly.config.service.v1.trainer.LackOfEncryptionTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.MetadataTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.MinOccurrenceConfig;
import ai.traceable.anomaly.config.service.v1.trainer.PauseAction;
import ai.traceable.anomaly.config.service.v1.trainer.ResetAction;
import ai.traceable.anomaly.config.service.v1.trainer.ResumeAction;
import ai.traceable.anomaly.config.service.v1.trainer.ScopedTrainingActionConfig;
import ai.traceable.anomaly.config.service.v1.trainer.ScopedTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.TrainerConfigServiceGrpc;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingAction;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingAction.ActionCase;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingActionConfig;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingConfig.TrainingConfigCase;
import ai.traceable.anomaly.config.service.v1.trainer.UpdateScopedTrainingConfigRequest;
import ai.traceable.anomaly.config.service.v1.trainer.UpsertTrainingActionRequest;
import ai.traceable.anomaly.config.service.v1.trainer.UrlFilterConfig;
import ai.traceable.anomaly.config.service.v1.trainer.VulnerabilityTrainingConfig;
import ai.traceable.config.service.TraceableConfigServiceIntegrationTestBase;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TrainerConfigServiceIntegrationTest extends TraceableConfigServiceIntegrationTestBase {
  private static TrainerConfigServiceGrpc.TrainerConfigServiceBlockingStub configServiceStub;
  private final AnomalyServiceScope serviceScope =
      AnomalyServiceScope.newBuilder().setId("service").build();
  private final AnomalyApiScope apiScope =
      AnomalyApiScope.newBuilder().setId("api").setServiceScope(serviceScope).build();
  private final AnomalyConfigScope apiConfigScope =
      AnomalyConfigScope.newBuilder().setApiScope(apiScope).build();
  private final AnomalyConfigScope customerConfigScope =
      AnomalyConfigScope.newBuilder()
          .setCustomerScope(AnomalyCustomerScope.getDefaultInstance())
          .build();
  private final AnomalyConfigScope serviceConfigScope =
      AnomalyConfigScope.newBuilder().setServiceScope(serviceScope).build();

  @BeforeAll
  static void init() {
    configServiceStub =
        TrainerConfigServiceGrpc.newBlockingStub(managedChannelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Test
  void testDefaultGetAllScopedTrainingConfig() {
    List<ScopedTrainingConfig> scopedTrainingConfigs = fetchAllTrainerConfigs(TENANT_ID);
    assertEquals(1, scopedTrainingConfigs.size());
    testDefaultApiNamingConfig(scopedTrainingConfigs.get(0));
    ScopedTrainingConfig updateConfig =
        ScopedTrainingConfig.newBuilder()
            .setConfigScope(serviceConfigScope)
            .addTrainingConfigs(
                TrainingConfig.newBuilder()
                    .setApiNamingTrainingConfig(
                        ApiNamingTrainingConfig.newBuilder()
                            .setUrlFilterConfig(
                                UrlFilterConfig.newBuilder()
                                    .setUrlRejectRegexPatterns(
                                        StringList.newBuilder()
                                            .addAllValues(List.of("regex-1"))
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();
    updateTrainerConfig(updateConfig);
    scopedTrainingConfigs = fetchAllTrainerConfigs(TENANT_ID);
    assertEquals(2, scopedTrainingConfigs.size());
    testDefaultApiNamingConfig(scopedTrainingConfigs.get(1));
  }

  @Test
  void testDefaultGetScopedTrainingConfig() {
    ScopedTrainingConfig scopedTrainingConfig = fetchTrainerConfig(serviceConfigScope);
    testDefaultApiNamingConfig(scopedTrainingConfig);
  }

  void testDefaultApiNamingConfig(ScopedTrainingConfig scopedTrainingConfig) {
    assertEquals(
        List.of(
            ".*\\.css$",
            ".*\\.jpg$",
            ".*\\.svg$",
            ".*\\.js$",
            ".*\\.pdf$",
            ".*\\.jpeg$",
            ".*\\.gif$",
            ".*\\.png$",
            ".*\\.bmp$",
            ".*\\.tif$",
            ".*\\.tiff$",
            ".*\\.mp3$",
            ".*\\.wma$",
            ".*\\.wav$",
            ".*\\.ogg$",
            ".*\\.mp4$",
            ".*\\.avi$",
            ".*\\.mkv$",
            ".*\\.woff$",
            ".*\\.woff2$",
            ".*\\.webp$",
            ".*\\.html$"),
        scopedTrainingConfig
            .getTrainingConfigs(0)
            .getApiNamingTrainingConfig()
            .getUrlFilterConfig()
            .getUrlRejectRegexPatterns()
            .getValuesList());
    assertEquals(
        100,
        scopedTrainingConfig
            .getTrainingConfigs(1)
            .getApiNamingTrainingConfig()
            .getRejectFilterConfig()
            .getSegmentFilterConfig()
            .getUrlPartsThreshold());

    assertEquals(
        100,
        scopedTrainingConfig
            .getTrainingConfigs(1)
            .getApiNamingTrainingConfig()
            .getRejectFilterConfig()
            .getSegmentFilterConfig()
            .getSegmentLengthThreshold());

    assertEquals(
        List.of(
            "bot",
            "crawler",
            "baiduspider",
            "80legs",
            "ia_archiver",
            "voyager",
            "curl",
            "wget",
            "yahoo",
            "slurp",
            "mediapartners-google",
            "whiteHat security"),
        scopedTrainingConfig
            .getTrainingConfigs(1)
            .getApiNamingTrainingConfig()
            .getRejectFilterConfig()
            .getUserAgentFilterConfig()
            .getBotAgentList()
            .getValuesList());

    assertEquals(
        2,
        scopedTrainingConfig
            .getTrainingConfigs(1)
            .getApiNamingTrainingConfig()
            .getRejectFilterConfig()
            .getStatusCodeFilterConfig()
            .getRangeFilterConfigsList()
            .size());

    assertEquals(
        List.of(302, 307),
        scopedTrainingConfig
            .getTrainingConfigs(1)
            .getApiNamingTrainingConfig()
            .getRejectFilterConfig()
            .getStatusCodeFilterConfig()
            .getRangeFilterConfigs(0)
            .getExclusions()
            .getValuesList());

    assertEquals(
        300,
        scopedTrainingConfig
            .getTrainingConfigs(1)
            .getApiNamingTrainingConfig()
            .getRejectFilterConfig()
            .getStatusCodeFilterConfig()
            .getRangeFilterConfigs(0)
            .getLow());

    assertEquals(
        599,
        scopedTrainingConfig
            .getTrainingConfigs(1)
            .getApiNamingTrainingConfig()
            .getRejectFilterConfig()
            .getStatusCodeFilterConfig()
            .getRangeFilterConfigs(0)
            .getHigh());

    assertEquals(
        0,
        scopedTrainingConfig
            .getTrainingConfigs(1)
            .getApiNamingTrainingConfig()
            .getRejectFilterConfig()
            .getStatusCodeFilterConfig()
            .getRangeFilterConfigs(1)
            .getLow());

    assertEquals(
        0,
        scopedTrainingConfig
            .getTrainingConfigs(1)
            .getApiNamingTrainingConfig()
            .getRejectFilterConfig()
            .getStatusCodeFilterConfig()
            .getRangeFilterConfigs(1)
            .getHigh());

    assertEquals(
        List.of(
            ".*\\.css$",
            ".*\\.jpg$",
            ".*\\.svg$",
            ".*\\.js$",
            ".*\\.pdf$",
            ".*\\.jpeg$",
            ".*\\.gif$",
            ".*\\.png$",
            ".*\\.bmp$",
            ".*\\.tif$",
            ".*\\.tiff$",
            ".*\\.mp3$",
            ".*\\.wma$",
            ".*\\.wav$",
            ".*\\.ogg$",
            ".*\\.mp4$",
            ".*\\.avi$",
            ".*\\.mkv$",
            ".*\\.woff$",
            ".*\\.woff2$",
            ".*\\.webp$",
            ".*\\.html$"),
        scopedTrainingConfig
            .getTrainingConfigs(1)
            .getApiNamingTrainingConfig()
            .getRejectFilterConfig()
            .getUrlPathFilterConfig()
            .getUrlPathRegexPatterns()
            .getValuesList());
    assertEquals(
        List.of("v\\d+"),
        scopedTrainingConfig
            .getTrainingConfigs(2)
            .getApiNamingTrainingConfig()
            .getTrieModelTrainingConfig()
            .getAllowRegexList()
            .getValuesList());
    assertEquals(
        List.of(
            "(\\{){0,1}[0-9a-fA-F]{8}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{12}(\\}){0,1}",
            "\\d+"),
        scopedTrainingConfig
            .getTrainingConfigs(2)
            .getApiNamingTrainingConfig()
            .getTrieModelTrainingConfig()
            .getIds()
            .getRegexList()
            .getValuesList());
    assertEquals(
        List.of(),
        scopedTrainingConfig
            .getTrainingConfigs(2)
            .getApiNamingTrainingConfig()
            .getTrieModelTrainingConfig()
            .getLowCardinality()
            .getRegexList()
            .getValuesList());
    assertEquals(
        List.of("[a-zA-Z]*?([-_+]?[a-zA-Z]+)+[-_+]?"),
        scopedTrainingConfig
            .getTrainingConfigs(2)
            .getApiNamingTrainingConfig()
            .getTrieModelTrainingConfig()
            .getHighCardinality()
            .getRegexList()
            .getValuesList());
    assertEquals(
        List.of("json", "xml"),
        scopedTrainingConfig
            .getTrainingConfigs(2)
            .getApiNamingTrainingConfig()
            .getTrieModelTrainingConfig()
            .getExtensions()
            .getValuesList());

    assertEquals(
        1,
        scopedTrainingConfig
            .getTrainingConfigs(2)
            .getApiNamingTrainingConfig()
            .getTrieModelTrainingConfig()
            .getIds()
            .getThreshold());
    assertEquals(
        10,
        scopedTrainingConfig
            .getTrainingConfigs(2)
            .getApiNamingTrainingConfig()
            .getTrieModelTrainingConfig()
            .getLowCardinality()
            .getThreshold());
    assertEquals(
        25,
        scopedTrainingConfig
            .getTrainingConfigs(2)
            .getApiNamingTrainingConfig()
            .getTrieModelTrainingConfig()
            .getMediumCardinality()
            .getThreshold());
    assertEquals(
        45,
        scopedTrainingConfig
            .getTrainingConfigs(2)
            .getApiNamingTrainingConfig()
            .getTrieModelTrainingConfig()
            .getHighCardinality()
            .getThreshold());
    assertEquals(
        100,
        scopedTrainingConfig
            .getTrainingConfigs(2)
            .getApiNamingTrainingConfig()
            .getTrieModelTrainingConfig()
            .getEmbryonicThreshold());
  }

  @Test
  void testGetAndUpdateScopedTrainingConfig() {
    ScopedTrainingConfig scopedTrainingConfig;
    assertThrows(
        RuntimeException.class,
        () ->
            fetchTrainerConfig(
                AnomalyConfigScope.newBuilder()
                    .setParamScope(AnomalyParamScope.getDefaultInstance())
                    .build()));

    assertThrows(
        RuntimeException.class,
        () ->
            updateTrainerConfig(
                ScopedTrainingConfig.newBuilder()
                    .setConfigScope(
                        AnomalyConfigScope.newBuilder()
                            .setParamScope(AnomalyParamScope.getDefaultInstance())
                            .build())
                    .build()));

    scopedTrainingConfig =
        ScopedTrainingConfig.newBuilder()
            .setConfigScope(customerConfigScope)
            .addTrainingConfigs(
                TrainingConfig.newBuilder()
                    .setVulnerabilityTrainingConfig(
                        VulnerabilityTrainingConfig.newBuilder()
                            .setLackOfEncryption(
                                LackOfEncryptionTrainingConfig.newBuilder()
                                    .setHttpsCallsConfig(
                                        MinOccurrenceConfig.newBuilder()
                                            .setMinTotalOccurrences(50)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();

    updateTrainerConfig(scopedTrainingConfig);
    assertEquals(
        50,
        fetchTrainerConfig(customerConfigScope)
            .getTrainingConfigsList()
            .get(0)
            .getVulnerabilityTrainingConfig()
            .getLackOfEncryption()
            .getHttpsCallsConfig()
            .getMinTotalOccurrences());
    assertEquals(
        50,
        fetchTrainerConfig(serviceConfigScope)
            .getTrainingConfigsList()
            .get(0)
            .getVulnerabilityTrainingConfig()
            .getLackOfEncryption()
            .getHttpsCallsConfig()
            .getMinTotalOccurrences());
    assertEquals(
        50,
        fetchTrainerConfig(apiConfigScope)
            .getTrainingConfigsList()
            .get(0)
            .getVulnerabilityTrainingConfig()
            .getLackOfEncryption()
            .getHttpsCallsConfig()
            .getMinTotalOccurrences());

    scopedTrainingConfig =
        ScopedTrainingConfig.newBuilder()
            .setConfigScope(serviceConfigScope)
            .addTrainingConfigs(
                TrainingConfig.newBuilder()
                    .setVulnerabilityTrainingConfig(
                        VulnerabilityTrainingConfig.newBuilder()
                            .setLackOfEncryption(
                                LackOfEncryptionTrainingConfig.newBuilder()
                                    .setHttpsCallsConfig(
                                        MinOccurrenceConfig.newBuilder()
                                            .setMinTotalOccurrences(60)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();

    updateTrainerConfig(scopedTrainingConfig);
    // customer config remains unchanged
    assertEquals(
        50,
        fetchTrainerConfig(customerConfigScope)
            .getTrainingConfigsList()
            .get(0)
            .getVulnerabilityTrainingConfig()
            .getLackOfEncryption()
            .getHttpsCallsConfig()
            .getMinTotalOccurrences());
    assertEquals(
        60,
        fetchTrainerConfig(serviceConfigScope)
            .getTrainingConfigsList()
            .get(0)
            .getVulnerabilityTrainingConfig()
            .getLackOfEncryption()
            .getHttpsCallsConfig()
            .getMinTotalOccurrences());
    assertEquals(
        60,
        fetchTrainerConfig(apiConfigScope)
            .getTrainingConfigsList()
            .get(0)
            .getVulnerabilityTrainingConfig()
            .getLackOfEncryption()
            .getHttpsCallsConfig()
            .getMinTotalOccurrences());

    scopedTrainingConfig =
        ScopedTrainingConfig.newBuilder()
            .setConfigScope(apiConfigScope)
            .addTrainingConfigs(
                TrainingConfig.newBuilder()
                    .setVulnerabilityTrainingConfig(
                        VulnerabilityTrainingConfig.newBuilder()
                            .setLackOfEncryption(
                                LackOfEncryptionTrainingConfig.newBuilder()
                                    .setHttpsCallsConfig(
                                        MinOccurrenceConfig.newBuilder()
                                            .setMinTotalOccurrences(70)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();

    updateTrainerConfig(scopedTrainingConfig);
    // customer config remains unchanged
    assertEquals(
        50,
        fetchTrainerConfig(customerConfigScope)
            .getTrainingConfigsList()
            .get(0)
            .getVulnerabilityTrainingConfig()
            .getLackOfEncryption()
            .getHttpsCallsConfig()
            .getMinTotalOccurrences());
    // service config remains unchanged
    assertEquals(
        60,
        fetchTrainerConfig(serviceConfigScope)
            .getTrainingConfigsList()
            .get(0)
            .getVulnerabilityTrainingConfig()
            .getLackOfEncryption()
            .getHttpsCallsConfig()
            .getMinTotalOccurrences());
    assertEquals(
        70,
        fetchTrainerConfig(apiConfigScope)
            .getTrainingConfigsList()
            .get(0)
            .getVulnerabilityTrainingConfig()
            .getLackOfEncryption()
            .getHttpsCallsConfig()
            .getMinTotalOccurrences());
  }

  @Test
  void testPartialConfigUpdate() {
    ScopedTrainingConfig scopedTrainingConfig =
        ScopedTrainingConfig.newBuilder()
            .setConfigScope(customerConfigScope)
            .addTrainingConfigs(
                TrainingConfig.newBuilder()
                    .setVulnerabilityTrainingConfig(
                        VulnerabilityTrainingConfig.newBuilder()
                            .setLackOfEncryption(
                                LackOfEncryptionTrainingConfig.newBuilder()
                                    .setHttpsCallsConfig(
                                        MinOccurrenceConfig.newBuilder()
                                            .setMinTotalOccurrences(50)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();

    updateTrainerConfig(scopedTrainingConfig);
    assertEquals(
        50,
        fetchTrainerConfig(customerConfigScope)
            .getTrainingConfigsList()
            .get(0)
            .getVulnerabilityTrainingConfig()
            .getLackOfEncryption()
            .getHttpsCallsConfig()
            .getMinTotalOccurrences());

    scopedTrainingConfig =
        ScopedTrainingConfig.newBuilder()
            .setConfigScope(customerConfigScope)
            .addTrainingConfigs(
                TrainingConfig.newBuilder()
                    .setMetadataTrainingConfig(
                        MetadataTrainingConfig.newBuilder()
                            .setContentSize(
                                ContentSizeTrainingConfig.newBuilder()
                                    .setRequestRangeSize(1000)
                                    .build())
                            .build())
                    .build())
            .build();
    updateTrainerConfig(scopedTrainingConfig);
    List<TrainingConfig> trainingConfigs =
        fetchTrainerConfig(customerConfigScope).getTrainingConfigsList();
    assertEquals(5, trainingConfigs.size());
    for (TrainingConfig trainingConfig : trainingConfigs) {
      if (trainingConfig.getTrainingConfigCase() == TrainingConfigCase.METADATA_TRAINING_CONFIG) {
        assertEquals(
            1000,
            trainingConfig.getMetadataTrainingConfig().getContentSize().getRequestRangeSize());
      } else if (trainingConfig.getTrainingConfigCase()
          == TrainingConfigCase.VULNERABILITY_TRAINING_CONFIG) {
        assertEquals(
            50,
            trainingConfig
                .getVulnerabilityTrainingConfig()
                .getLackOfEncryption()
                .getHttpsCallsConfig()
                .getMinTotalOccurrences());
      }
    }
  }

  @Test
  void testGetAllScopedTrainingConfigs() {
    ScopedTrainingConfig scopedTrainingConfig;

    scopedTrainingConfig =
        ScopedTrainingConfig.newBuilder()
            .setConfigScope(customerConfigScope)
            .addTrainingConfigs(
                TrainingConfig.newBuilder()
                    .setVulnerabilityTrainingConfig(
                        VulnerabilityTrainingConfig.newBuilder()
                            .setLackOfEncryption(
                                LackOfEncryptionTrainingConfig.newBuilder()
                                    .setHttpsCallsConfig(
                                        MinOccurrenceConfig.newBuilder()
                                            .setMinTotalOccurrences(50)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();

    updateTrainerConfig(scopedTrainingConfig);

    List<ScopedTrainingConfig> scopedTrainingConfigs = fetchAllTrainerConfigs(TENANT_ID);

    assertEquals(1, scopedTrainingConfigs.size());
    assertEquals(
        50,
        getScopedTrainingConfig(scopedTrainingConfigs, customerConfigScope)
            .getTrainingConfigsList()
            .get(0)
            .getVulnerabilityTrainingConfig()
            .getLackOfEncryption()
            .getHttpsCallsConfig()
            .getMinTotalOccurrences());

    scopedTrainingConfig =
        ScopedTrainingConfig.newBuilder()
            .setConfigScope(serviceConfigScope)
            .addTrainingConfigs(
                TrainingConfig.newBuilder()
                    .setVulnerabilityTrainingConfig(
                        VulnerabilityTrainingConfig.newBuilder()
                            .setLackOfEncryption(
                                LackOfEncryptionTrainingConfig.newBuilder()
                                    .setHttpsCallsConfig(
                                        MinOccurrenceConfig.newBuilder()
                                            .setMinTotalOccurrences(60)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();

    updateTrainerConfig(scopedTrainingConfig);

    scopedTrainingConfigs = fetchAllTrainerConfigs(TENANT_ID);
    assertEquals(2, scopedTrainingConfigs.size());
    assertEquals(
        50,
        getScopedTrainingConfig(scopedTrainingConfigs, customerConfigScope)
            .getTrainingConfigsList()
            .get(0)
            .getVulnerabilityTrainingConfig()
            .getLackOfEncryption()
            .getHttpsCallsConfig()
            .getMinTotalOccurrences());
    assertEquals(
        60,
        getScopedTrainingConfig(scopedTrainingConfigs, serviceConfigScope)
            .getTrainingConfigsList()
            .get(0)
            .getVulnerabilityTrainingConfig()
            .getLackOfEncryption()
            .getHttpsCallsConfig()
            .getMinTotalOccurrences());

    scopedTrainingConfig =
        ScopedTrainingConfig.newBuilder()
            .setConfigScope(apiConfigScope)
            .addTrainingConfigs(
                TrainingConfig.newBuilder()
                    .setVulnerabilityTrainingConfig(
                        VulnerabilityTrainingConfig.newBuilder()
                            .setLackOfEncryption(
                                LackOfEncryptionTrainingConfig.newBuilder()
                                    .setHttpsCallsConfig(
                                        MinOccurrenceConfig.newBuilder()
                                            .setMinTotalOccurrences(70)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();

    updateTrainerConfig(scopedTrainingConfig);

    scopedTrainingConfigs = fetchAllTrainerConfigs(TENANT_ID);
    assertEquals(3, scopedTrainingConfigs.size());
    assertEquals(
        50,
        getScopedTrainingConfig(scopedTrainingConfigs, customerConfigScope)
            .getTrainingConfigsList()
            .get(0)
            .getVulnerabilityTrainingConfig()
            .getLackOfEncryption()
            .getHttpsCallsConfig()
            .getMinTotalOccurrences());
    assertEquals(
        60,
        getScopedTrainingConfig(scopedTrainingConfigs, serviceConfigScope)
            .getTrainingConfigsList()
            .get(0)
            .getVulnerabilityTrainingConfig()
            .getLackOfEncryption()
            .getHttpsCallsConfig()
            .getMinTotalOccurrences());
    assertEquals(
        70,
        getScopedTrainingConfig(scopedTrainingConfigs, apiConfigScope)
            .getTrainingConfigsList()
            .get(0)
            .getVulnerabilityTrainingConfig()
            .getLackOfEncryption()
            .getHttpsCallsConfig()
            .getMinTotalOccurrences());
  }

  @Test
  void testUpsertAndGetAllTrainingActions() {
    // 1. upsert PAUSE training action at tenant level
    TrainingAction trainingAction =
        TrainingAction.newBuilder().setPauseAction(PauseAction.newBuilder().build()).build();

    long upsertPauseRequestTime1 = System.currentTimeMillis();
    upsertTrainingAction(customerConfigScope, trainingAction);
    // get all trainer actions for tenant and validate
    List<ScopedTrainingActionConfig> scopedTrainingActionConfigList =
        getAllTrainerActions(TENANT_ID);
    assertEquals(1, scopedTrainingActionConfigList.size());
    Map<AnomalyConfigScope, ScopedTrainingActionConfig> scopedActionConfigMap =
        getScopedTrainingActionConfigMap(scopedTrainingActionConfigList);
    assertEquals(1, scopedActionConfigMap.size());
    // customer scope will have 1 action: PAUSE
    List<TrainingActionConfig> trainingActionConfigList =
        scopedActionConfigMap.get(customerConfigScope).getTrainingActionConfigList();
    Map<TrainingAction.ActionCase, TrainingActionConfig> actionConfigMap =
        getActionConfigMap(trainingActionConfigList);
    assertEquals(1, actionConfigMap.size());
    TrainingActionConfig trainingActionConfig = actionConfigMap.get(ActionCase.PAUSE_ACTION);
    assertTrue(trainingActionConfig.getTimestamp() >= upsertPauseRequestTime1);
    long tenantPauseActionTime = trainingActionConfig.getTimestamp();

    // 2. upsert RESUME training action at service level
    trainingAction =
        TrainingAction.newBuilder().setResumeAction(ResumeAction.newBuilder().build()).build();
    long upsertResumeRequestTime2 = System.currentTimeMillis();
    upsertTrainingAction(serviceConfigScope, trainingAction);
    // get all trainer actions for tenant and validate
    scopedTrainingActionConfigList = getAllTrainerActions(TENANT_ID);
    assertEquals(2, scopedTrainingActionConfigList.size());
    scopedActionConfigMap = getScopedTrainingActionConfigMap(scopedTrainingActionConfigList);
    // customer scope will have 1 action: PAUSE
    trainingActionConfigList =
        scopedActionConfigMap.get(customerConfigScope).getTrainingActionConfigList();
    actionConfigMap = getActionConfigMap(trainingActionConfigList);
    assertEquals(1, actionConfigMap.size());
    assertEquals(
        tenantPauseActionTime, actionConfigMap.get(ActionCase.PAUSE_ACTION).getTimestamp());
    // service scope will have 2 actions: PAUSE and RESUME
    trainingActionConfigList =
        scopedActionConfigMap.get(serviceConfigScope).getTrainingActionConfigList();
    actionConfigMap = getActionConfigMap(trainingActionConfigList);
    assertEquals(2, actionConfigMap.size());
    assertEquals(
        tenantPauseActionTime, actionConfigMap.get(ActionCase.PAUSE_ACTION).getTimestamp());
    assertTrue(
        actionConfigMap.get(ActionCase.RESUME_ACTION).getTimestamp() >= upsertResumeRequestTime2);
    long serviceResumeActionTime = actionConfigMap.get(ActionCase.RESUME_ACTION).getTimestamp();

    // 3. upsert PAUSE training action at api level
    trainingAction =
        TrainingAction.newBuilder().setPauseAction(PauseAction.newBuilder().build()).build();
    long upsertPauseRequestTime2 = System.currentTimeMillis();
    upsertTrainingAction(apiConfigScope, trainingAction);
    // get all trainer actions for tenant and validate
    scopedTrainingActionConfigList = getAllTrainerActions(TENANT_ID);
    assertEquals(3, scopedTrainingActionConfigList.size());
    scopedActionConfigMap = getScopedTrainingActionConfigMap(scopedTrainingActionConfigList);
    // customer scope will have 1 action: PAUSE
    trainingActionConfigList =
        scopedActionConfigMap.get(customerConfigScope).getTrainingActionConfigList();
    actionConfigMap = getActionConfigMap(trainingActionConfigList);
    assertEquals(1, actionConfigMap.size());
    assertEquals(
        tenantPauseActionTime, actionConfigMap.get(ActionCase.PAUSE_ACTION).getTimestamp());
    // service scope will have 2 actions: PAUSE and RESUME
    trainingActionConfigList =
        scopedActionConfigMap.get(serviceConfigScope).getTrainingActionConfigList();
    actionConfigMap = getActionConfigMap(trainingActionConfigList);
    assertEquals(2, actionConfigMap.size());
    assertEquals(
        tenantPauseActionTime, actionConfigMap.get(ActionCase.PAUSE_ACTION).getTimestamp());
    assertEquals(
        serviceResumeActionTime, actionConfigMap.get(ActionCase.RESUME_ACTION).getTimestamp());
    // api scope will have 2 actions: PAUSE and RESUME. But the PAUSE time will be updated
    trainingActionConfigList =
        scopedActionConfigMap.get(apiConfigScope).getTrainingActionConfigList();
    actionConfigMap = getActionConfigMap(trainingActionConfigList);
    assertEquals(2, actionConfigMap.size());
    long apiPauseActionTime = actionConfigMap.get(ActionCase.PAUSE_ACTION).getTimestamp();
    assertTrue(apiPauseActionTime >= tenantPauseActionTime);
    assertTrue(apiPauseActionTime >= upsertPauseRequestTime2);
    assertEquals(
        serviceResumeActionTime, actionConfigMap.get(ActionCase.RESUME_ACTION).getTimestamp());

    // 4. upsert another RESET training action at tenant level
    trainingAction =
        TrainingAction.newBuilder().setResetAction(ResetAction.newBuilder().build()).build();
    long upsertResetRequestTime = System.currentTimeMillis();
    upsertTrainingAction(customerConfigScope, trainingAction);
    // get all trainer actions for tenant and validate
    scopedTrainingActionConfigList = getAllTrainerActions(TENANT_ID);
    assertEquals(3, scopedTrainingActionConfigList.size());
    scopedActionConfigMap = getScopedTrainingActionConfigMap(scopedTrainingActionConfigList);
    // customer scope will now have 2 actions: PAUSE and RESET
    trainingActionConfigList =
        scopedActionConfigMap.get(customerConfigScope).getTrainingActionConfigList();
    actionConfigMap = getActionConfigMap(trainingActionConfigList);
    assertEquals(2, actionConfigMap.size());
    assertEquals(
        tenantPauseActionTime, actionConfigMap.get(ActionCase.PAUSE_ACTION).getTimestamp());
    long tenantResetActionTime = actionConfigMap.get(ActionCase.RESET_ACTION).getTimestamp();
    assertTrue(tenantResetActionTime >= upsertResetRequestTime);
    // service scope will have 3 actions: PAUSE, RESUME and RESET
    trainingActionConfigList =
        scopedActionConfigMap.get(serviceConfigScope).getTrainingActionConfigList();
    actionConfigMap = getActionConfigMap(trainingActionConfigList);
    assertEquals(3, actionConfigMap.size());
    assertEquals(
        tenantPauseActionTime, actionConfigMap.get(ActionCase.PAUSE_ACTION).getTimestamp());
    assertEquals(
        serviceResumeActionTime, actionConfigMap.get(ActionCase.RESUME_ACTION).getTimestamp());
    assertEquals(
        tenantResetActionTime, actionConfigMap.get(ActionCase.RESET_ACTION).getTimestamp());
    // api scope will have 3 actions: PAUSE, RESUME and RESET. The PAUSE time will be updated
    trainingActionConfigList =
        scopedActionConfigMap.get(apiConfigScope).getTrainingActionConfigList();
    actionConfigMap = getActionConfigMap(trainingActionConfigList);
    assertEquals(3, actionConfigMap.size());
    assertEquals(apiPauseActionTime, actionConfigMap.get(ActionCase.PAUSE_ACTION).getTimestamp());
    assertEquals(
        serviceResumeActionTime, actionConfigMap.get(ActionCase.RESUME_ACTION).getTimestamp());
    assertEquals(
        tenantResetActionTime, actionConfigMap.get(ActionCase.RESET_ACTION).getTimestamp());
  }

  private ScopedTrainingConfig fetchTrainerConfig(AnomalyConfigScope configScope) {
    return fetchTrainerConfig(configScope, TENANT_ID);
  }

  private ScopedTrainingConfig fetchTrainerConfig(AnomalyConfigScope configScope, String tenantId) {
    return RequestContext.forTenantId(tenantId)
        .call(
            () ->
                configServiceStub
                    .getScopedTrainingConfig(
                        GetScopedTrainingConfigRequest.newBuilder()
                            .setConfigScope(configScope)
                            .build())
                    .getScopedTrainingConfig());
  }

  private List<ScopedTrainingConfig> fetchAllTrainerConfigs(String tenantId) {
    return RequestContext.forTenantId(tenantId)
        .call(
            () ->
                configServiceStub
                    .getAllScopedTrainingConfigs(
                        GetAllScopedTrainingConfigsRequest.newBuilder().build())
                    .getScopedTrainingConfigsList());
  }

  private ScopedTrainingConfig updateTrainerConfig(ScopedTrainingConfig scopedTrainingConfig) {
    return RequestContext.forTenantId(TENANT_ID)
        .call(
            () ->
                configServiceStub
                    .updateScopedTrainingConfig(
                        UpdateScopedTrainingConfigRequest.newBuilder()
                            .setScopedTrainingConfig(scopedTrainingConfig)
                            .build())
                    .getScopedTrainingConfig());
  }

  private ScopedTrainingConfig getScopedTrainingConfig(
      List<ScopedTrainingConfig> trainingConfigs, AnomalyConfigScope configScope) {
    for (ScopedTrainingConfig trainingConfig : trainingConfigs) {
      if (trainingConfig.getConfigScope().equals(configScope)) {
        return trainingConfig;
      }
    }
    return null;
  }

  private ScopedTrainingActionConfig upsertTrainingAction(
      AnomalyConfigScope anomalyConfigScope, TrainingAction trainingAction) {
    return RequestContext.forTenantId(TENANT_ID)
        .call(
            () ->
                configServiceStub
                    .upsertTrainingAction(
                        UpsertTrainingActionRequest.newBuilder()
                            .setConfigScope(anomalyConfigScope)
                            .setTrainingAction(trainingAction)
                            .build())
                    .getScopedTrainingActionConfig());
  }

  private List<ScopedTrainingActionConfig> getAllTrainerActions(String tenantId) {
    return RequestContext.forTenantId(tenantId)
        .call(
            () ->
                configServiceStub
                    .getAllTrainingActions(GetAllTrainingActionsRequest.newBuilder().build())
                    .getScopedTrainingActionConfigsList());
  }

  private Map<AnomalyConfigScope, ScopedTrainingActionConfig> getScopedTrainingActionConfigMap(
      List<ScopedTrainingActionConfig> scopedActionConfigs) {
    return scopedActionConfigs.stream()
        .collect(Collectors.toMap(ScopedTrainingActionConfig::getConfigScope, Function.identity()));
  }

  private Map<TrainingAction.ActionCase, TrainingActionConfig> getActionConfigMap(
      List<TrainingActionConfig> trainingActionConfigList) {
    return trainingActionConfigList.stream()
        .collect(
            Collectors.toMap(
                trainingActionConfig -> trainingActionConfig.getTrainingAction().getActionCase(),
                Function.identity()));
  }
}
