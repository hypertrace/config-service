package ai.traceable.localprocessing.config.service.apinaming.http.namingconfig;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import ai.traceable.anomaly.config.service.v1.StringList;
import ai.traceable.anomaly.config.service.v1.trainer.ApiNamingTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.ThresholdRegexConfig;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.TrieModelTrainingConfig;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.localprocessing.config.service.apinaming.http.utils.HttpApiNamingConfigInfo;
import ai.traceable.localprocessing.config.service.config.http.HttpApiNamingConfig;
import ai.traceable.platform.apientity.TrieNodeType;
import com.google.protobuf.GeneratedMessageV3;
import java.util.List;
import java.util.regex.Pattern;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DefaultHttpApiNamingConfigManagerTest {
  HttpApiNamingConfigManager httpApiNamingConfigManager;

  @BeforeEach
  void setUp() {
    HttpApiNamingConfig httpApiNamingConfig = mock(HttpApiNamingConfig.class);
    doReturn(List.of("fallback-regex")).when(httpApiNamingConfig).getFallbackRegexes();

    UuidGenerator uuidGenerator = mock(UuidGenerator.class);
    doReturn("random-hash").when(uuidGenerator).generateId((GeneratedMessageV3) any());

    httpApiNamingConfigManager =
        new DefaultHttpApiNamingConfigManager(httpApiNamingConfig, uuidGenerator);
  }

  @Test
  void getHttpApiNamingConfigInfoDefaultMediumRegex() {
    List<TrainingConfig> trainingConfigList =
        List.of(
            TrainingConfig.newBuilder()
                .setApiNamingTrainingConfig(
                    ApiNamingTrainingConfig.newBuilder()
                        .setTrieModelTrainingConfig(
                            TrieModelTrainingConfig.newBuilder()
                                .setEmbryonicThreshold(200)
                                .setMaxNumberOfTriePaths(20000)
                                .setAllowRegexList(
                                    StringList.newBuilder().addValues("allow-regex").build())
                                .setExtensions(
                                    StringList.newBuilder()
                                        .addValues("extension-1")
                                        .addValues("extension-2")
                                        .build())
                                .setIds(
                                    ThresholdRegexConfig.newBuilder()
                                        .setThreshold(10)
                                        .setRegexList(
                                            StringList.newBuilder().addValues("id-regex").build())
                                        .build())
                                .setLowCardinality(
                                    ThresholdRegexConfig.newBuilder()
                                        .setThreshold(20)
                                        .setRegexList(
                                            StringList.newBuilder().addValues("low-regex").build())
                                        .build())
                                .setHighCardinality(
                                    ThresholdRegexConfig.newBuilder()
                                        .setThreshold(50)
                                        .setRegexList(
                                            StringList.newBuilder().addValues("high-regex").build())
                                        .build())
                                .build())
                        .build())
                .build());

    HttpApiNamingConfigInfo response =
        httpApiNamingConfigManager.getHttpApiNamingConfigInfo(trainingConfigList, List.of(), "");
    assertEquals("random-hash", response.getHttpApiNamingConfig().getHash());
    assertEquals(200, response.getEmbryonicThreshold());
    assertEquals(20000, response.getMaxNumberOfTriePaths());
    assertEquals(
        List.of("fallback-regex"),
        response.getHttpApiNamingConfig().getFallbackWildcardRegexesList());
    assertEquals(List.of("allow-regex"), response.getSegmentWhitelistRegexes());
    assertEquals(List.of("extension-1", "extension-2"), response.getExtensions());
    assertEquals("id-regex", response.getWildcardConfigMap().get(TrieNodeType.ID));
    assertEquals("low-regex", response.getWildcardConfigMap().get(TrieNodeType.LOW_CARDINALITY));
    assertEquals("high-regex", response.getWildcardConfigMap().get(TrieNodeType.HIGH_CARDINALITY));
    assertEquals(
        "(?!^(id-regex|low-regex|high-regex|allow-regex|.*\\.extension-1|.*\\.extension-2)$)^.*$",
        response.getWildcardConfigMap().get(TrieNodeType.MEDIUM_CARDINALITY));

    assertTrue(
        Pattern.matches(
            response.getWildcardConfigMap().get(TrieNodeType.MEDIUM_CARDINALITY), "medium-regex"));
    assertFalse(
        Pattern.matches(
            response.getWildcardConfigMap().get(TrieNodeType.MEDIUM_CARDINALITY), "high-regex"));
    assertFalse(
        Pattern.matches(
            response.getWildcardConfigMap().get(TrieNodeType.MEDIUM_CARDINALITY), "low-regex"));
    assertFalse(
        Pattern.matches(
            response.getWildcardConfigMap().get(TrieNodeType.MEDIUM_CARDINALITY), "id-regex"));
    assertFalse(
        Pattern.matches(
            response.getWildcardConfigMap().get(TrieNodeType.MEDIUM_CARDINALITY), "allow-regex"));
    assertFalse(
        Pattern.matches(
            response.getWildcardConfigMap().get(TrieNodeType.MEDIUM_CARDINALITY), "1.extension-2"));
  }

  @Test
  void getHttpApiNamingConfigInfoMediumRegexSet() {
    List<TrainingConfig> trainingConfigList =
        List.of(
            TrainingConfig.newBuilder()
                .setApiNamingTrainingConfig(
                    ApiNamingTrainingConfig.newBuilder()
                        .setTrieModelTrainingConfig(
                            TrieModelTrainingConfig.newBuilder()
                                .setEmbryonicThreshold(200)
                                .setMaxNumberOfTriePaths(20000)
                                .setAllowRegexList(
                                    StringList.newBuilder().addValues("allow-regex").build())
                                .setExtensions(
                                    StringList.newBuilder().addValues("extension").build())
                                .setIds(
                                    ThresholdRegexConfig.newBuilder()
                                        .setThreshold(10)
                                        .setRegexList(
                                            StringList.newBuilder()
                                                .addValues("id-regex-1")
                                                .addValues("id-regex-2")
                                                .build())
                                        .build())
                                .setLowCardinality(
                                    ThresholdRegexConfig.newBuilder()
                                        .setThreshold(20)
                                        .setRegexList(
                                            StringList.newBuilder().addValues("low-regex").build())
                                        .build())
                                .setMediumCardinality(
                                    ThresholdRegexConfig.newBuilder()
                                        .setThreshold(30)
                                        .setRegexList(
                                            StringList.newBuilder()
                                                .addValues("medium-regex")
                                                .build())
                                        .build())
                                .setHighCardinality(
                                    ThresholdRegexConfig.newBuilder()
                                        .setThreshold(50)
                                        .setRegexList(
                                            StringList.newBuilder().addValues("high-regex").build())
                                        .build())
                                .build())
                        .build())
                .build());

    HttpApiNamingConfigInfo response =
        httpApiNamingConfigManager.getHttpApiNamingConfigInfo(trainingConfigList, List.of(), "");
    assertEquals("random-hash", response.getHttpApiNamingConfig().getHash());
    assertEquals(200, response.getEmbryonicThreshold());
    assertEquals(20000, response.getMaxNumberOfTriePaths());
    assertEquals(
        List.of("fallback-regex"),
        response.getHttpApiNamingConfig().getFallbackWildcardRegexesList());
    assertEquals(List.of("allow-regex"), response.getSegmentWhitelistRegexes());
    assertEquals(List.of("extension"), response.getExtensions());
    assertEquals("id-regex-1|id-regex-2", response.getWildcardConfigMap().get(TrieNodeType.ID));
    assertEquals("low-regex", response.getWildcardConfigMap().get(TrieNodeType.LOW_CARDINALITY));
    assertEquals("high-regex", response.getWildcardConfigMap().get(TrieNodeType.HIGH_CARDINALITY));
    assertEquals(
        "medium-regex", response.getWildcardConfigMap().get(TrieNodeType.MEDIUM_CARDINALITY));
  }
}
