package ai.traceable.anomaly.config.service.common;

import static ai.traceable.anomaly.config.service.common.AnomalyConfigServiceUtils.mergeConfigs;
import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.anomaly.config.service.v1.StringList;
import ai.traceable.anomaly.config.service.v1.trainer.ApiNamingTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.CustomRuleConfig;
import ai.traceable.anomaly.config.service.v1.trainer.CustomRulesListConfig;
import ai.traceable.anomaly.config.service.v1.trainer.ThresholdRegexConfig;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.TrieModelTrainingConfig;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;

public class AnomalyConfigServiceUtilsTest {

  @Test
  void testMergeMessages() throws IOException {
    TrainingConfig trainingConfig1 =
        TrainingConfig.newBuilder()
            .setApiNamingTrainingConfig(
                ApiNamingTrainingConfig.newBuilder()
                    .setTrieModelTrainingConfig(
                        TrieModelTrainingConfig.newBuilder()
                            .setEmbryonicThreshold(100)
                            .setMaxNumberOfTriePaths(10000)
                            .setLowCardinality(
                                buildThresholdRegexConfig(10, List.of("low-1", "low-2")))
                            .setHighCardinality(
                                buildThresholdRegexConfig(100, List.of("high-1", "high-2")))
                            .setAllowRegexList(buildStringList(List.of("url-1", "url-2")))
                            .build())
                    .build())
            .build();

    TrainingConfig trainingConfig2 =
        TrainingConfig.newBuilder()
            .setApiNamingTrainingConfig(
                ApiNamingTrainingConfig.newBuilder()
                    .setTrieModelTrainingConfig(
                        TrieModelTrainingConfig.newBuilder()
                            .setEmbryonicThreshold(50)
                            .setMaxNumberOfTriePaths(1000)
                            .setMediumCardinality(
                                buildThresholdRegexConfig(50, List.of("med-1", "med-2")))
                            .setHighCardinality(
                                buildThresholdRegexConfig(1000, List.of("high-10", "high-20")))
                            .setExtensions(buildStringList(List.of("ext-1", "ext-2")))
                            .build())
                    .build())
            .build();

    TrainingConfig trainingConfig =
        TrainingConfig.newBuilder()
            .setApiNamingTrainingConfig(
                ApiNamingTrainingConfig.newBuilder()
                    .setTrieModelTrainingConfig(
                        TrieModelTrainingConfig.newBuilder()
                            .setEmbryonicThreshold(50)
                            .setMaxNumberOfTriePaths(1000)
                            .setAllowRegexList(buildStringList(List.of("url-1", "url-2")))
                            .setLowCardinality(
                                buildThresholdRegexConfig(10, List.of("low-1", "low-2")))
                            .setMediumCardinality(
                                buildThresholdRegexConfig(50, List.of("med-1", "med-2")))
                            .setHighCardinality(
                                buildThresholdRegexConfig(1000, List.of("high-10", "high-20")))
                            .setExtensions(buildStringList(List.of("ext-1", "ext-2")))
                            .build())
                    .build())
            .build();

    TrainingConfig mergedConfig = (TrainingConfig) mergeConfigs(trainingConfig1, trainingConfig2);
    assertEquals(trainingConfig, mergedConfig);
  }

  @Test
  void testMergeRepeatedMessage() {
    TrainingConfig trainingConfig1 =
        TrainingConfig.newBuilder()
            .setApiNamingTrainingConfig(
                ApiNamingTrainingConfig.newBuilder()
                    .setCustomRulesListConfig(
                        CustomRulesListConfig.newBuilder()
                            .addAllCustomRulesConfig(
                                List.of(
                                    buildCustomRuleConfig("regex-1", "url-1"),
                                    buildCustomRuleConfig("regex-2", "url-2")))
                            .build())
                    .build())
            .build();
    TrainingConfig trainingConfig2 =
        TrainingConfig.newBuilder()
            .setApiNamingTrainingConfig(
                ApiNamingTrainingConfig.newBuilder()
                    .setCustomRulesListConfig(
                        CustomRulesListConfig.newBuilder()
                            .addAllCustomRulesConfig(
                                List.of(
                                    buildCustomRuleConfig("regex-3", "url-3"),
                                    buildCustomRuleConfig("regex-4", "url-4"),
                                    buildCustomRuleConfig("regex-5", "url-5")))
                            .build())
                    .build())
            .build();

    TrainingConfig mergedConfig;
    mergedConfig = (TrainingConfig) mergeConfigs(trainingConfig1, trainingConfig2);
    assertEquals(trainingConfig2, mergedConfig);

    mergedConfig = (TrainingConfig) mergeConfigs(trainingConfig2, trainingConfig1);
    assertEquals(trainingConfig1, mergedConfig);
  }

  private ThresholdRegexConfig buildThresholdRegexConfig(int threshold, List<String> regexList) {
    return ThresholdRegexConfig.newBuilder()
        .setThreshold(threshold)
        .setRegexList(buildStringList(regexList))
        .build();
  }

  private StringList buildStringList(List<String> values) {
    return StringList.newBuilder().addAllValues(values).build();
  }

  private CustomRuleConfig buildCustomRuleConfig(String regex, String url) {
    return CustomRuleConfig.newBuilder().setRegex(regex).setUrlPattern(url).build();
  }
}
