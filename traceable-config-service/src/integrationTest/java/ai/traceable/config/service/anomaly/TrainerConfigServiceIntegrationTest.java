package ai.traceable.config.service.anomaly;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.anomaly.config.service.v1.AnomalyApiScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.AnomalyParamScope;
import ai.traceable.anomaly.config.service.v1.AnomalyServiceScope;
import ai.traceable.anomaly.config.service.v1.StringList;
import ai.traceable.anomaly.config.service.v1.trainer.AccessorsTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.ApiNamingTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.ContentSizeTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.EnumerationsTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.GetAllScopedTrainingConfigsRequest;
import ai.traceable.anomaly.config.service.v1.trainer.GetAllTrainingActionsRequest;
import ai.traceable.anomaly.config.service.v1.trainer.GetScopedTrainingConfigRequest;
import ai.traceable.anomaly.config.service.v1.trainer.JwtParamsTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.LackOfEncryptionTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.MetadataTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.MinOccurrenceConfig;
import ai.traceable.anomaly.config.service.v1.trainer.PauseAction;
import ai.traceable.anomaly.config.service.v1.trainer.ResetAction;
import ai.traceable.anomaly.config.service.v1.trainer.ResumeAction;
import ai.traceable.anomaly.config.service.v1.trainer.ScopedTrainingActionConfig;
import ai.traceable.anomaly.config.service.v1.trainer.ScopedTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.ThresholdCountConfig;
import ai.traceable.anomaly.config.service.v1.trainer.ThresholdFamilyConfig;
import ai.traceable.anomaly.config.service.v1.trainer.ThresholdsFamilyTrainingConfig;
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
        TrainerConfigServiceGrpc.newBlockingStub(channelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Test
  void testDefaultGetAllScopedTrainingConfig() {
    List<ScopedTrainingConfig> scopedTrainingConfigs = fetchAllTrainerConfigs(TENANT_ID);
    assertEquals(1, scopedTrainingConfigs.size());
    testDefaultApiNamingConfig(scopedTrainingConfigs.get(0));
    testDefaultMetadataConfig(scopedTrainingConfigs.get(0));
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
    testDefaultMetadataConfig(scopedTrainingConfigs.get(1));
  }

  @Test
  void testDefaultGetScopedTrainingConfig() {
    ScopedTrainingConfig scopedTrainingConfig = fetchTrainerConfig(serviceConfigScope);
    testDefaultApiNamingConfig(scopedTrainingConfig);
    testDefaultMetadataConfig(scopedTrainingConfig);
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
        0,
        scopedTrainingConfig
            .getTrainingConfigs(1)
            .getApiNamingTrainingConfig()
            .getRejectFilterConfig()
            .getUserAgentFilterConfig()
            .getBotAgentList()
            .getValuesList()
            .size());

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
        10,
        scopedTrainingConfig
            .getTrainingConfigs(2)
            .getApiNamingTrainingConfig()
            .getTrieModelTrainingConfig()
            .getEmbryonicThreshold());
  }

  void testDefaultMetadataConfig(ScopedTrainingConfig scopedTrainingConfig) {
    List<TrainingConfig> metadataTrainingConfigs =
        scopedTrainingConfig.getTrainingConfigsList().stream()
            .filter(TrainingConfig::hasMetadataTrainingConfig)
            .collect(Collectors.toUnmodifiableList());
    assertFalse(metadataTrainingConfigs.get(0).getDisabled());
    ThresholdFamilyConfig digitLengthThresholdFamilyConfig =
        metadataTrainingConfigs
            .get(0)
            .getMetadataTrainingConfig()
            .getDigitLength()
            .getThresholdFamilyConfig();
    ThresholdCountConfig digitLengthDiverseIpDiverseUserFamilyConfig =
        digitLengthThresholdFamilyConfig.getDiverseIpDiverseUserFamilyConfig();
    ThresholdCountConfig digitLengthDiverseIpLimitedUserFamilyConfig =
        digitLengthThresholdFamilyConfig.getDiverseIpLimitedUserFamilyConfig();
    ThresholdCountConfig digitLengthLimitedIpFamilyConfig =
        digitLengthThresholdFamilyConfig.getLimitedIpFamilyConfig();
    assertEquals(20, digitLengthDiverseIpDiverseUserFamilyConfig.getRequiredCallsCount());
    assertEquals(10, digitLengthDiverseIpDiverseUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(10, digitLengthDiverseIpDiverseUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(20, digitLengthDiverseIpLimitedUserFamilyConfig.getRequiredCallsCount());
    assertEquals(0, digitLengthDiverseIpLimitedUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(10, digitLengthDiverseIpLimitedUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(20, digitLengthLimitedIpFamilyConfig.getRequiredCallsCount());
    assertEquals(0, digitLengthLimitedIpFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(0, digitLengthLimitedIpFamilyConfig.getRequiredUniqueIpsCount());

    assertFalse(metadataTrainingConfigs.get(1).getDisabled());
    ThresholdFamilyConfig specialCharsThresholdFamilyConfig =
        metadataTrainingConfigs
            .get(1)
            .getMetadataTrainingConfig()
            .getSpecialChars()
            .getThresholdFamilyConfig();
    ThresholdCountConfig specialCharsDiverseIpDiverseUserFamilyConfig =
        specialCharsThresholdFamilyConfig.getDiverseIpDiverseUserFamilyConfig();
    ThresholdCountConfig specialCharsDiverseIpLimitedUserFamilyConfig =
        specialCharsThresholdFamilyConfig.getDiverseIpLimitedUserFamilyConfig();
    ThresholdCountConfig specialCharsLimitedIpFamilyConfig =
        specialCharsThresholdFamilyConfig.getLimitedIpFamilyConfig();
    assertEquals(20, specialCharsDiverseIpDiverseUserFamilyConfig.getRequiredCallsCount());
    assertEquals(10, specialCharsDiverseIpDiverseUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(10, specialCharsDiverseIpDiverseUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(20, specialCharsDiverseIpLimitedUserFamilyConfig.getRequiredCallsCount());
    assertEquals(0, specialCharsDiverseIpLimitedUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(10, specialCharsDiverseIpLimitedUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(20, specialCharsLimitedIpFamilyConfig.getRequiredCallsCount());
    assertEquals(0, specialCharsLimitedIpFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(0, specialCharsLimitedIpFamilyConfig.getRequiredUniqueIpsCount());

    assertFalse(metadataTrainingConfigs.get(2).getDisabled());
    ThresholdFamilyConfig htmlTagsThresholdFamilyConfig =
        metadataTrainingConfigs
            .get(2)
            .getMetadataTrainingConfig()
            .getHtmlTags()
            .getThresholdFamilyConfig();
    ThresholdCountConfig htmlTagsDiverseIpDiverseUserFamilyConfig =
        htmlTagsThresholdFamilyConfig.getDiverseIpDiverseUserFamilyConfig();
    ThresholdCountConfig htmlTagsDiverseIpLimitedUserFamilyConfig =
        htmlTagsThresholdFamilyConfig.getDiverseIpLimitedUserFamilyConfig();
    ThresholdCountConfig htmlTagsLimitedIpFamilyConfig =
        htmlTagsThresholdFamilyConfig.getLimitedIpFamilyConfig();
    assertEquals(20, htmlTagsDiverseIpDiverseUserFamilyConfig.getRequiredCallsCount());
    assertEquals(10, htmlTagsDiverseIpDiverseUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(10, htmlTagsDiverseIpDiverseUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(20, htmlTagsDiverseIpLimitedUserFamilyConfig.getRequiredCallsCount());
    assertEquals(0, htmlTagsDiverseIpLimitedUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(10, htmlTagsDiverseIpLimitedUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(20, htmlTagsLimitedIpFamilyConfig.getRequiredCallsCount());
    assertEquals(0, htmlTagsLimitedIpFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(0, htmlTagsLimitedIpFamilyConfig.getRequiredUniqueIpsCount());

    assertFalse(metadataTrainingConfigs.get(3).getDisabled());
    ThresholdFamilyConfig htmlTagAttributesThresholdFamilyConfig =
        metadataTrainingConfigs
            .get(3)
            .getMetadataTrainingConfig()
            .getHtmlTagAttributes()
            .getThresholdFamilyConfig();
    ThresholdCountConfig htmlTagAttributesDiverseIpDiverseUserFamilyConfig =
        htmlTagAttributesThresholdFamilyConfig.getDiverseIpDiverseUserFamilyConfig();
    ThresholdCountConfig htmlTagAttributesDiverseIpLimitedUserFamilyConfig =
        htmlTagAttributesThresholdFamilyConfig.getDiverseIpLimitedUserFamilyConfig();
    ThresholdCountConfig htmlTagAttributesLimitedIpFamilyConfig =
        htmlTagAttributesThresholdFamilyConfig.getLimitedIpFamilyConfig();
    assertEquals(20, htmlTagAttributesDiverseIpDiverseUserFamilyConfig.getRequiredCallsCount());
    assertEquals(
        10, htmlTagAttributesDiverseIpDiverseUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(10, htmlTagAttributesDiverseIpDiverseUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(20, htmlTagAttributesDiverseIpLimitedUserFamilyConfig.getRequiredCallsCount());
    assertEquals(
        0, htmlTagAttributesDiverseIpLimitedUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(10, htmlTagAttributesDiverseIpLimitedUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(20, htmlTagAttributesLimitedIpFamilyConfig.getRequiredCallsCount());
    assertEquals(0, htmlTagAttributesLimitedIpFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(0, htmlTagAttributesLimitedIpFamilyConfig.getRequiredUniqueIpsCount());

    assertFalse(metadataTrainingConfigs.get(4).getDisabled());
    ThresholdFamilyConfig httpStatusThresholdFamilyConfig =
        metadataTrainingConfigs
            .get(4)
            .getMetadataTrainingConfig()
            .getHttpStatus()
            .getThresholdFamilyConfig();
    ThresholdCountConfig httpStatusDiverseIpDiverseUserFamilyConfig =
        httpStatusThresholdFamilyConfig.getDiverseIpDiverseUserFamilyConfig();
    ThresholdCountConfig httpStatusDiverseIpLimitedUserFamilyConfig =
        httpStatusThresholdFamilyConfig.getDiverseIpLimitedUserFamilyConfig();
    ThresholdCountConfig httpStatusLimitedIpFamilyConfig =
        httpStatusThresholdFamilyConfig.getLimitedIpFamilyConfig();
    assertEquals(20, httpStatusDiverseIpDiverseUserFamilyConfig.getRequiredCallsCount());
    assertEquals(10, httpStatusDiverseIpDiverseUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(10, httpStatusDiverseIpDiverseUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(20, httpStatusDiverseIpLimitedUserFamilyConfig.getRequiredCallsCount());
    assertEquals(0, httpStatusDiverseIpLimitedUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(10, httpStatusDiverseIpLimitedUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(20, httpStatusLimitedIpFamilyConfig.getRequiredCallsCount());
    assertEquals(0, httpStatusLimitedIpFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(0, httpStatusLimitedIpFamilyConfig.getRequiredUniqueIpsCount());

    assertFalse(metadataTrainingConfigs.get(5).getDisabled());
    ThresholdFamilyConfig deviceThresholdFamilyConfig =
        metadataTrainingConfigs
            .get(5)
            .getMetadataTrainingConfig()
            .getDevice()
            .getThresholdFamilyConfig();
    ThresholdCountConfig deviceDiverseIpDiverseUserFamilyConfig =
        deviceThresholdFamilyConfig.getDiverseIpDiverseUserFamilyConfig();
    ThresholdCountConfig deviceDiverseIpLimitedUserFamilyConfig =
        deviceThresholdFamilyConfig.getDiverseIpLimitedUserFamilyConfig();
    ThresholdCountConfig deviceLimitedIpFamilyConfig =
        deviceThresholdFamilyConfig.getLimitedIpFamilyConfig();
    assertEquals(20, deviceDiverseIpDiverseUserFamilyConfig.getRequiredCallsCount());
    assertEquals(10, deviceDiverseIpDiverseUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(10, deviceDiverseIpDiverseUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(20, deviceDiverseIpLimitedUserFamilyConfig.getRequiredCallsCount());
    assertEquals(0, deviceDiverseIpLimitedUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(10, deviceDiverseIpLimitedUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(20, deviceLimitedIpFamilyConfig.getRequiredCallsCount());
    assertEquals(0, deviceLimitedIpFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(0, deviceLimitedIpFamilyConfig.getRequiredUniqueIpsCount());

    assertFalse(metadataTrainingConfigs.get(6).getDisabled());
    assertEquals(
        20,
        metadataTrainingConfigs.get(6).getMetadataTrainingConfig().getSsrf().getMaxAllowedHosts());
    ThresholdFamilyConfig protocolThresholdFamilyConfig =
        metadataTrainingConfigs
            .get(6)
            .getMetadataTrainingConfig()
            .getSsrf()
            .getProtocolThresholdFamilyConfig();
    ThresholdCountConfig protocolThresholdDiverseIpDiverseUserFamilyConfig =
        protocolThresholdFamilyConfig.getDiverseIpDiverseUserFamilyConfig();
    ThresholdCountConfig protocolThresholdDiverseIpLimitedUserFamilyConfig =
        protocolThresholdFamilyConfig.getDiverseIpLimitedUserFamilyConfig();
    ThresholdCountConfig protocolThresholdLimitedIpFamilyConfig =
        protocolThresholdFamilyConfig.getLimitedIpFamilyConfig();
    assertEquals(20, protocolThresholdDiverseIpDiverseUserFamilyConfig.getRequiredCallsCount());
    assertEquals(
        5, protocolThresholdDiverseIpDiverseUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(5, protocolThresholdDiverseIpDiverseUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(20, protocolThresholdDiverseIpLimitedUserFamilyConfig.getRequiredCallsCount());
    assertEquals(
        0, protocolThresholdDiverseIpLimitedUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(5, protocolThresholdDiverseIpLimitedUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(20, protocolThresholdLimitedIpFamilyConfig.getRequiredCallsCount());
    assertEquals(0, protocolThresholdLimitedIpFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(0, protocolThresholdLimitedIpFamilyConfig.getRequiredUniqueIpsCount());

    assertFalse(metadataTrainingConfigs.get(7).getDisabled());
    ContentSizeTrainingConfig contentSizeTrainingConfig =
        metadataTrainingConfigs.get(7).getMetadataTrainingConfig().getContentSize();
    assertEquals(1000, contentSizeTrainingConfig.getRequestRangeSize());
    assertEquals(1000, contentSizeTrainingConfig.getResponseRangeSize());

    assertFalse(metadataTrainingConfigs.get(8).getDisabled());
    AccessorsTrainingConfig accessorsTrainingConfig =
        metadataTrainingConfigs.get(8).getMetadataTrainingConfig().getAccessors();
    ThresholdFamilyConfig apiThresholdFamilyConfig =
        accessorsTrainingConfig.getApiThresholdFamilyConfig();
    ThresholdCountConfig apiThresholdDiverseIpDiverseUserFamilyConfig =
        apiThresholdFamilyConfig.getDiverseIpDiverseUserFamilyConfig();
    ThresholdCountConfig apiThresholdDiverseIpLimitedUserFamilyConfig =
        apiThresholdFamilyConfig.getDiverseIpLimitedUserFamilyConfig();
    ThresholdCountConfig apiThresholdLimitedIpFamilyConfig =
        apiThresholdFamilyConfig.getLimitedIpFamilyConfig();
    assertEquals(100, apiThresholdDiverseIpDiverseUserFamilyConfig.getRequiredCallsCount());
    assertEquals(10, apiThresholdDiverseIpDiverseUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(10, apiThresholdDiverseIpDiverseUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(100, apiThresholdDiverseIpLimitedUserFamilyConfig.getRequiredCallsCount());
    assertEquals(0, apiThresholdDiverseIpLimitedUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(10, apiThresholdDiverseIpLimitedUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(100, apiThresholdLimitedIpFamilyConfig.getRequiredCallsCount());
    assertEquals(0, apiThresholdLimitedIpFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(0, apiThresholdLimitedIpFamilyConfig.getRequiredUniqueIpsCount());
    ThresholdFamilyConfig queryParamThresholdFamilyConfig =
        accessorsTrainingConfig.getQueryParamThresholdFamilyConfig();
    ThresholdCountConfig queryParamThresholdDiverseIpDiverseUserFamilyConfig =
        queryParamThresholdFamilyConfig.getDiverseIpDiverseUserFamilyConfig();
    ThresholdCountConfig queryParamThresholdDiverseIpLimitedUserFamilyConfig =
        queryParamThresholdFamilyConfig.getDiverseIpLimitedUserFamilyConfig();
    ThresholdCountConfig queryParamThresholdLimitedIpFamilyConfig =
        queryParamThresholdFamilyConfig.getLimitedIpFamilyConfig();
    assertEquals(100, queryParamThresholdDiverseIpDiverseUserFamilyConfig.getRequiredCallsCount());
    assertEquals(
        10, queryParamThresholdDiverseIpDiverseUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(
        10, queryParamThresholdDiverseIpDiverseUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(100, queryParamThresholdDiverseIpLimitedUserFamilyConfig.getRequiredCallsCount());
    assertEquals(
        0, queryParamThresholdDiverseIpLimitedUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(
        10, queryParamThresholdDiverseIpLimitedUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(100, queryParamThresholdLimitedIpFamilyConfig.getRequiredCallsCount());
    assertEquals(0, queryParamThresholdLimitedIpFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(0, queryParamThresholdLimitedIpFamilyConfig.getRequiredUniqueIpsCount());
    ThresholdFamilyConfig requestHeaderThresholdFamilyConfig =
        accessorsTrainingConfig.getRequestHeaderThresholdFamilyConfig();
    ThresholdCountConfig requestHeaderThresholdDiverseIpDiverseUserFamilyConfig =
        requestHeaderThresholdFamilyConfig.getDiverseIpDiverseUserFamilyConfig();
    ThresholdCountConfig requestHeaderThresholdDiverseIpLimitedUserFamilyConfig =
        requestHeaderThresholdFamilyConfig.getDiverseIpLimitedUserFamilyConfig();
    ThresholdCountConfig requestHeaderThresholdLimitedIpFamilyConfig =
        requestHeaderThresholdFamilyConfig.getLimitedIpFamilyConfig();
    assertEquals(
        100, requestHeaderThresholdDiverseIpDiverseUserFamilyConfig.getRequiredCallsCount());
    assertEquals(
        10, requestHeaderThresholdDiverseIpDiverseUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(
        10, requestHeaderThresholdDiverseIpDiverseUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(
        100, requestHeaderThresholdDiverseIpLimitedUserFamilyConfig.getRequiredCallsCount());
    assertEquals(
        0, requestHeaderThresholdDiverseIpLimitedUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(
        10, requestHeaderThresholdDiverseIpLimitedUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(100, requestHeaderThresholdLimitedIpFamilyConfig.getRequiredCallsCount());
    assertEquals(0, requestHeaderThresholdLimitedIpFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(0, requestHeaderThresholdLimitedIpFamilyConfig.getRequiredUniqueIpsCount());
    ThresholdFamilyConfig requestCookieThresholdFamilyConfig =
        accessorsTrainingConfig.getRequestCookieThresholdFamilyConfig();
    ThresholdCountConfig requestCookieThresholdDiverseIpDiverseUserFamilyConfig =
        requestCookieThresholdFamilyConfig.getDiverseIpDiverseUserFamilyConfig();
    ThresholdCountConfig requestCookieThresholdDiverseIpLimitedUserFamilyConfig =
        requestCookieThresholdFamilyConfig.getDiverseIpLimitedUserFamilyConfig();
    ThresholdCountConfig requestCookieThresholdLimitedIpFamilyConfig =
        requestCookieThresholdFamilyConfig.getLimitedIpFamilyConfig();
    assertEquals(
        100, requestCookieThresholdDiverseIpDiverseUserFamilyConfig.getRequiredCallsCount());
    assertEquals(
        10, requestCookieThresholdDiverseIpDiverseUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(
        10, requestCookieThresholdDiverseIpDiverseUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(
        100, requestCookieThresholdDiverseIpLimitedUserFamilyConfig.getRequiredCallsCount());
    assertEquals(
        0, requestCookieThresholdDiverseIpLimitedUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(
        10, requestCookieThresholdDiverseIpLimitedUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(100, requestCookieThresholdLimitedIpFamilyConfig.getRequiredCallsCount());
    assertEquals(0, requestCookieThresholdLimitedIpFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(0, requestCookieThresholdLimitedIpFamilyConfig.getRequiredUniqueIpsCount());
    ThresholdFamilyConfig requestBodyParamThresholdFamilyConfig =
        accessorsTrainingConfig.getRequestBodyParamThresholdFamilyConfig();
    ThresholdCountConfig requestBodyParamThresholdDiverseIpDiverseUserFamilyConfig =
        requestBodyParamThresholdFamilyConfig.getDiverseIpDiverseUserFamilyConfig();
    ThresholdCountConfig requestBodyParamThresholdDiverseIpLimitedUserFamilyConfig =
        requestBodyParamThresholdFamilyConfig.getDiverseIpLimitedUserFamilyConfig();
    ThresholdCountConfig requestBodyParamThresholdLimitedIpFamilyConfig =
        requestBodyParamThresholdFamilyConfig.getLimitedIpFamilyConfig();
    assertEquals(
        50, requestBodyParamThresholdDiverseIpDiverseUserFamilyConfig.getRequiredCallsCount());
    assertEquals(
        5,
        requestBodyParamThresholdDiverseIpDiverseUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(
        5, requestBodyParamThresholdDiverseIpDiverseUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(
        50, requestBodyParamThresholdDiverseIpLimitedUserFamilyConfig.getRequiredCallsCount());
    assertEquals(
        0,
        requestBodyParamThresholdDiverseIpLimitedUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(
        5, requestBodyParamThresholdDiverseIpLimitedUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(50, requestBodyParamThresholdLimitedIpFamilyConfig.getRequiredCallsCount());
    assertEquals(0, requestBodyParamThresholdLimitedIpFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(0, requestBodyParamThresholdLimitedIpFamilyConfig.getRequiredUniqueIpsCount());
    ThresholdFamilyConfig paramTypeThresholdFamilyConfig =
        accessorsTrainingConfig.getParamTypeThresholdFamilyConfig();
    ThresholdCountConfig paramTypeThresholdDiverseIpDiverseUserFamilyConfig =
        paramTypeThresholdFamilyConfig.getDiverseIpDiverseUserFamilyConfig();
    ThresholdCountConfig paramTypeThresholdDiverseIpLimitedUserFamilyConfig =
        paramTypeThresholdFamilyConfig.getDiverseIpLimitedUserFamilyConfig();
    ThresholdCountConfig paramTypeThresholdLimitedIpFamilyConfig =
        paramTypeThresholdFamilyConfig.getLimitedIpFamilyConfig();
    assertEquals(50, paramTypeThresholdDiverseIpDiverseUserFamilyConfig.getRequiredCallsCount());
    assertEquals(
        5, paramTypeThresholdDiverseIpDiverseUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(5, paramTypeThresholdDiverseIpDiverseUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(50, paramTypeThresholdDiverseIpLimitedUserFamilyConfig.getRequiredCallsCount());
    assertEquals(
        0, paramTypeThresholdDiverseIpLimitedUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(5, paramTypeThresholdDiverseIpLimitedUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(50, paramTypeThresholdLimitedIpFamilyConfig.getRequiredCallsCount());
    assertEquals(0, paramTypeThresholdLimitedIpFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(0, paramTypeThresholdLimitedIpFamilyConfig.getRequiredUniqueIpsCount());

    assertFalse(metadataTrainingConfigs.get(9).getDisabled());
    ThresholdsFamilyTrainingConfig thresholdsFamilyTrainingConfig =
        metadataTrainingConfigs.get(9).getMetadataTrainingConfig().getThresholdsFamily();
    assertEquals(172800000, thresholdsFamilyTrainingConfig.getLearningTimeMillis());
    assertEquals(10, thresholdsFamilyTrainingConfig.getRequiredIpsForDiverseSet());
    assertEquals(10, thresholdsFamilyTrainingConfig.getRequiredUserIdsForDiverseSet());
    assertEquals(60, thresholdsFamilyTrainingConfig.getRequiredPercentForAuthenticated());
    assertEquals(List.of(404), thresholdsFamilyTrainingConfig.getBadStatusIps().getValuesList());
    assertEquals(
        List.of(401, 403, 301, 308),
        thresholdsFamilyTrainingConfig.getBadStatusUserIds().getValuesList());

    assertFalse(metadataTrainingConfigs.get(10).getDisabled());
    EnumerationsTrainingConfig enumerationsTrainingConfig =
        metadataTrainingConfigs.get(10).getMetadataTrainingConfig().getEnum();
    assertEquals(5, enumerationsTrainingConfig.getMaxEnumerations());
    assertEquals(10, enumerationsTrainingConfig.getEnumValueMaxLength());
    assertEquals(99.9, enumerationsTrainingConfig.getMinEnumOccurrencePercent());

    assertFalse(metadataTrainingConfigs.get(11).getDisabled());

    assertTrue(metadataTrainingConfigs.get(12).getDisabled());
    JwtParamsTrainingConfig jwtParamsTrainingConfig =
        metadataTrainingConfigs.get(12).getMetadataTrainingConfig().getJwtParams();
    ThresholdFamilyConfig jwtParamsFamilyConfig =
        jwtParamsTrainingConfig.getThresholdFamilyConfig();
    ThresholdCountConfig jwtParamsIpDiverseUserFamilyConfig =
        jwtParamsFamilyConfig.getDiverseIpDiverseUserFamilyConfig();
    ThresholdCountConfig jwtParamsIpLimitedUserFamilyConfig =
        jwtParamsFamilyConfig.getDiverseIpLimitedUserFamilyConfig();
    ThresholdCountConfig jwtParamsIpFamilyConfig = jwtParamsFamilyConfig.getLimitedIpFamilyConfig();
    assertEquals(20, jwtParamsIpDiverseUserFamilyConfig.getRequiredCallsCount());
    assertEquals(10, jwtParamsIpDiverseUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(10, jwtParamsIpDiverseUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(20, jwtParamsIpLimitedUserFamilyConfig.getRequiredCallsCount());
    assertEquals(0, jwtParamsIpLimitedUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(10, jwtParamsIpLimitedUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(20, jwtParamsIpFamilyConfig.getRequiredCallsCount());
    assertEquals(0, jwtParamsIpFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(0, jwtParamsIpFamilyConfig.getRequiredUniqueIpsCount());
    List<String> jwtparamRegexes = jwtParamsTrainingConfig.getIncludeParamRegexes().getValuesList();
    jwtparamRegexes.get(0).startsWith("^traceableai.jwt.payload(.+)\\.");
    jwtparamRegexes.get(1).startsWith("^traceableai.jwt.header(.+)\\.");
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
        fetchTrainerConfig(customerConfigScope).getTrainingConfigsList().stream()
            .filter(TrainingConfig::hasVulnerabilityTrainingConfig)
            .filter(
                trainingConfig ->
                    trainingConfig.getVulnerabilityTrainingConfig().hasLackOfEncryption())
            .collect(Collectors.toUnmodifiableList())
            .get(0)
            .getVulnerabilityTrainingConfig()
            .getLackOfEncryption()
            .getHttpsCallsConfig()
            .getMinTotalOccurrences());
    assertEquals(
        50,
        fetchTrainerConfig(serviceConfigScope).getTrainingConfigsList().stream()
            .filter(TrainingConfig::hasVulnerabilityTrainingConfig)
            .filter(
                trainingConfig ->
                    trainingConfig.getVulnerabilityTrainingConfig().hasLackOfEncryption())
            .collect(Collectors.toUnmodifiableList())
            .get(0)
            .getVulnerabilityTrainingConfig()
            .getLackOfEncryption()
            .getHttpsCallsConfig()
            .getMinTotalOccurrences());
    assertEquals(
        50,
        fetchTrainerConfig(apiConfigScope).getTrainingConfigsList().stream()
            .filter(TrainingConfig::hasVulnerabilityTrainingConfig)
            .filter(
                trainingConfig ->
                    trainingConfig.getVulnerabilityTrainingConfig().hasLackOfEncryption())
            .collect(Collectors.toUnmodifiableList())
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
        fetchTrainerConfig(customerConfigScope).getTrainingConfigsList().stream()
            .filter(TrainingConfig::hasVulnerabilityTrainingConfig)
            .filter(
                trainingConfig ->
                    trainingConfig.getVulnerabilityTrainingConfig().hasLackOfEncryption())
            .collect(Collectors.toUnmodifiableList())
            .get(0)
            .getVulnerabilityTrainingConfig()
            .getLackOfEncryption()
            .getHttpsCallsConfig()
            .getMinTotalOccurrences());
    assertEquals(
        60,
        fetchTrainerConfig(serviceConfigScope).getTrainingConfigsList().stream()
            .filter(TrainingConfig::hasVulnerabilityTrainingConfig)
            .filter(
                trainingConfig ->
                    trainingConfig.getVulnerabilityTrainingConfig().hasLackOfEncryption())
            .collect(Collectors.toUnmodifiableList())
            .get(0)
            .getVulnerabilityTrainingConfig()
            .getLackOfEncryption()
            .getHttpsCallsConfig()
            .getMinTotalOccurrences());
    assertEquals(
        60,
        fetchTrainerConfig(apiConfigScope).getTrainingConfigsList().stream()
            .filter(TrainingConfig::hasVulnerabilityTrainingConfig)
            .filter(
                trainingConfig ->
                    trainingConfig.getVulnerabilityTrainingConfig().hasLackOfEncryption())
            .collect(Collectors.toUnmodifiableList())
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
        fetchTrainerConfig(customerConfigScope).getTrainingConfigsList().stream()
            .filter(TrainingConfig::hasVulnerabilityTrainingConfig)
            .filter(
                trainingConfig ->
                    trainingConfig.getVulnerabilityTrainingConfig().hasLackOfEncryption())
            .collect(Collectors.toUnmodifiableList())
            .get(0)
            .getVulnerabilityTrainingConfig()
            .getLackOfEncryption()
            .getHttpsCallsConfig()
            .getMinTotalOccurrences());
    // service config remains unchanged
    assertEquals(
        60,
        fetchTrainerConfig(serviceConfigScope).getTrainingConfigsList().stream()
            .filter(TrainingConfig::hasVulnerabilityTrainingConfig)
            .filter(
                trainingConfig ->
                    trainingConfig.getVulnerabilityTrainingConfig().hasLackOfEncryption())
            .collect(Collectors.toUnmodifiableList())
            .get(0)
            .getVulnerabilityTrainingConfig()
            .getLackOfEncryption()
            .getHttpsCallsConfig()
            .getMinTotalOccurrences());
    assertEquals(
        70,
        fetchTrainerConfig(apiConfigScope).getTrainingConfigsList().stream()
            .filter(TrainingConfig::hasVulnerabilityTrainingConfig)
            .filter(
                trainingConfig ->
                    trainingConfig.getVulnerabilityTrainingConfig().hasLackOfEncryption())
            .collect(Collectors.toUnmodifiableList())
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
        fetchTrainerConfig(customerConfigScope).getTrainingConfigsList().stream()
            .filter(TrainingConfig::hasVulnerabilityTrainingConfig)
            .filter(
                trainingConfig ->
                    trainingConfig.getVulnerabilityTrainingConfig().hasLackOfEncryption())
            .collect(Collectors.toUnmodifiableList())
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
                                    .setRequestRangeSize(2000)
                                    .build())
                            .build())
                    .build())
            .build();
    updateTrainerConfig(scopedTrainingConfig);
    List<TrainingConfig> trainingConfigs =
        fetchTrainerConfig(customerConfigScope).getTrainingConfigsList();
    assertEquals(41, trainingConfigs.size());
    for (TrainingConfig trainingConfig : trainingConfigs) {
      if (trainingConfig.getTrainingConfigCase() == TrainingConfigCase.METADATA_TRAINING_CONFIG
          && trainingConfig.getMetadataTrainingConfig().getConfigCase()
              == MetadataTrainingConfig.ConfigCase.CONTENT_SIZE) {
        assertEquals(
            2000,
            trainingConfig.getMetadataTrainingConfig().getContentSize().getRequestRangeSize());
      } else if (trainingConfig.getTrainingConfigCase()
              == TrainingConfigCase.METADATA_TRAINING_CONFIG
          && trainingConfig.getMetadataTrainingConfig().getConfigCase()
              == MetadataTrainingConfig.ConfigCase.PARAM_EXCLUSIONS) {
        assertEquals(
            7,
            trainingConfig
                .getMetadataTrainingConfig()
                .getParamExclusions()
                .getParamKeyExclusionConfigsList()
                .size());
        assertEquals(
            4,
            trainingConfig
                .getMetadataTrainingConfig()
                .getParamExclusions()
                .getParamLevelExclusionConfigsList()
                .size());
      } else if (trainingConfig.getTrainingConfigCase()
              == TrainingConfigCase.VULNERABILITY_TRAINING_CONFIG
          && trainingConfig.getVulnerabilityTrainingConfig().hasLackOfEncryption()) {
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

    List<TrainingConfig> scopedVulnerabilityTrainingConfigs =
        getScopedTrainingConfig(scopedTrainingConfigs, customerConfigScope)
            .getTrainingConfigsList()
            .stream()
            .filter(TrainingConfig::hasVulnerabilityTrainingConfig)
            .filter(
                trainingConfig ->
                    trainingConfig.getVulnerabilityTrainingConfig().hasLackOfEncryption())
            .collect(Collectors.toUnmodifiableList());
    assertEquals(1, scopedTrainingConfigs.size());
    assertEquals(
        50,
        scopedVulnerabilityTrainingConfigs
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
            .stream()
            .filter(TrainingConfig::hasVulnerabilityTrainingConfig)
            .filter(
                trainingConfig ->
                    trainingConfig.getVulnerabilityTrainingConfig().hasLackOfEncryption())
            .collect(Collectors.toUnmodifiableList())
            .get(0)
            .getVulnerabilityTrainingConfig()
            .getLackOfEncryption()
            .getHttpsCallsConfig()
            .getMinTotalOccurrences());
    assertEquals(
        60,
        getScopedTrainingConfig(scopedTrainingConfigs, serviceConfigScope)
            .getTrainingConfigsList()
            .stream()
            .filter(TrainingConfig::hasVulnerabilityTrainingConfig)
            .filter(
                trainingConfig ->
                    trainingConfig.getVulnerabilityTrainingConfig().hasLackOfEncryption())
            .collect(Collectors.toUnmodifiableList())
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
            .stream()
            .filter(TrainingConfig::hasVulnerabilityTrainingConfig)
            .filter(
                trainingConfig ->
                    trainingConfig.getVulnerabilityTrainingConfig().hasLackOfEncryption())
            .collect(Collectors.toUnmodifiableList())
            .get(0)
            .getVulnerabilityTrainingConfig()
            .getLackOfEncryption()
            .getHttpsCallsConfig()
            .getMinTotalOccurrences());
    assertEquals(
        60,
        getScopedTrainingConfig(scopedTrainingConfigs, serviceConfigScope)
            .getTrainingConfigsList()
            .stream()
            .filter(TrainingConfig::hasVulnerabilityTrainingConfig)
            .filter(
                trainingConfig ->
                    trainingConfig.getVulnerabilityTrainingConfig().hasLackOfEncryption())
            .collect(Collectors.toUnmodifiableList())
            .get(0)
            .getVulnerabilityTrainingConfig()
            .getLackOfEncryption()
            .getHttpsCallsConfig()
            .getMinTotalOccurrences());
    assertEquals(
        70,
        getScopedTrainingConfig(scopedTrainingConfigs, apiConfigScope)
            .getTrainingConfigsList()
            .stream()
            .filter(TrainingConfig::hasVulnerabilityTrainingConfig)
            .filter(
                trainingConfig ->
                    trainingConfig.getVulnerabilityTrainingConfig().hasLackOfEncryption())
            .collect(Collectors.toUnmodifiableList())
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
    System.out.println(trainingConfigs);
    int sz = trainingConfigs.size();
    System.out.println(sz);
    //    System.out.println(trainingConfigs.get(sz-1));
    //    return trainingConfigs.get(sz-1);
    for (ScopedTrainingConfig trainingConfig : trainingConfigs) {
      if (trainingConfig.getConfigScope().equals(configScope)) {
        //        System.out.println(trainingConfig);
        return trainingConfig;
      }
    }
    return ScopedTrainingConfig.getDefaultInstance();
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
