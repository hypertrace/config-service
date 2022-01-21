package ai.traceable.anomaly.config.service.trainer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import ai.traceable.anomaly.config.service.AnomalyConfigServiceConfig;
import ai.traceable.anomaly.config.service.registry.common.ConfigConverter;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingConfig;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import org.junit.jupiter.api.Test;

public class TrainerConfigServiceConfigTest {
  private static final String FILE_PATH = "trainer/trainerConfigServiceConfig.conf";

  @Test
  void testConfig() {
    Config trainerServiceConfig = ConfigFactory.parseResources(FILE_PATH);
    AnomalyConfigServiceConfig anomalyConfigServiceConfig =
        new AnomalyConfigServiceConfig(trainerServiceConfig);
    ConfigConverter configConverter = new ConfigConverter();
    TrainerConfigServiceConfig trainerConfigServiceConfig =
        new TrainerConfigServiceConfig(anomalyConfigServiceConfig, configConverter);
    List<TrainingConfig> apiNamingTrainingConfigs =
        trainerConfigServiceConfig.getApiNamingTrainingConfigs();

    assertEquals(2, apiNamingTrainingConfigs.size());

    assertFalse(apiNamingTrainingConfigs.get(0).getDisabled());
    assertEquals(
        List.of(".css", ".html"),
        apiNamingTrainingConfigs
            .get(0)
            .getApiNamingTrainingConfig()
            .getUrlFilterConfig()
            .getUrlRejectRegexPatterns()
            .getValuesList());
    assertEquals(
        List.of("v\\d+"),
        apiNamingTrainingConfigs
            .get(1)
            .getApiNamingTrainingConfig()
            .getTrieModelTrainingConfig()
            .getAllowRegexList()
            .getValuesList());
    assertEquals(
        List.of(
            "(\\{){0,1}[0-9a-fA-F]{8}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{12}(\\}){0,1}",
            "\\d+"),
        apiNamingTrainingConfigs
            .get(1)
            .getApiNamingTrainingConfig()
            .getTrieModelTrainingConfig()
            .getIds()
            .getRegexList()
            .getValuesList());
    assertEquals(
        List.of(),
        apiNamingTrainingConfigs
            .get(1)
            .getApiNamingTrainingConfig()
            .getTrieModelTrainingConfig()
            .getLowCardinality()
            .getRegexList()
            .getValuesList());
    assertEquals(
        List.of("[a-zA-Z]*?([-_+]?[a-zA-Z]+)+[-_+]?"),
        apiNamingTrainingConfigs
            .get(1)
            .getApiNamingTrainingConfig()
            .getTrieModelTrainingConfig()
            .getHighCardinality()
            .getRegexList()
            .getValuesList());
    assertEquals(
        List.of("json", "xml"),
        apiNamingTrainingConfigs
            .get(1)
            .getApiNamingTrainingConfig()
            .getTrieModelTrainingConfig()
            .getExtensions()
            .getValuesList());

    assertEquals(
        1,
        apiNamingTrainingConfigs
            .get(1)
            .getApiNamingTrainingConfig()
            .getTrieModelTrainingConfig()
            .getIds()
            .getThreshold());
    assertEquals(
        10,
        apiNamingTrainingConfigs
            .get(1)
            .getApiNamingTrainingConfig()
            .getTrieModelTrainingConfig()
            .getLowCardinality()
            .getThreshold());
    assertEquals(
        25,
        apiNamingTrainingConfigs
            .get(1)
            .getApiNamingTrainingConfig()
            .getTrieModelTrainingConfig()
            .getMediumCardinality()
            .getThreshold());
    assertEquals(
        45,
        apiNamingTrainingConfigs
            .get(1)
            .getApiNamingTrainingConfig()
            .getTrieModelTrainingConfig()
            .getHighCardinality()
            .getThreshold());
    assertEquals(
        100,
        apiNamingTrainingConfigs
            .get(1)
            .getApiNamingTrainingConfig()
            .getTrieModelTrainingConfig()
            .getEmbryonicThreshold());
  }
}
