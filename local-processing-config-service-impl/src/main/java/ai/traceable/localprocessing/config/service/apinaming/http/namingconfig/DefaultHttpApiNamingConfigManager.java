package ai.traceable.localprocessing.config.service.apinaming.http.namingconfig;

import ai.traceable.anomaly.config.service.v1.trainer.ApiNamingTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.CustomRulesListConfig;
import ai.traceable.anomaly.config.service.v1.trainer.ThresholdRegexConfig;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.TrieModelTrainingConfig;
import ai.traceable.localprocessing.config.service.apinaming.http.utils.HttpApiNamingConfigInfo;
import ai.traceable.localprocessing.config.service.config.http.HttpApiNamingConfig;
import ai.traceable.localprocessing.config.service.utils.UuidGenerator;
import ai.traceable.localprocessing.config.service.v1.HttpApiNamingCustomRule;
import ai.traceable.localprocessing.config.service.v1.WildcardConfig;
import ai.traceable.localprocessing.config.service.v1.WildcardType;
import com.google.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class DefaultHttpApiNamingConfigManager implements HttpApiNamingConfigManager {

  private final UuidGenerator uuidGenerator;
  private final HttpApiNamingConfig httpApiNamingConfig;

  @Inject
  public DefaultHttpApiNamingConfigManager(
      HttpApiNamingConfig httpApiNamingConfig, UuidGenerator uuidGenerator) {
    this.httpApiNamingConfig = httpApiNamingConfig;
    this.uuidGenerator = uuidGenerator;
  }

  public HttpApiNamingConfigInfo getHttpApiNamingConfigInfo(
      List<TrainingConfig> trainingConfigs, String configHash) {
    ai.traceable.localprocessing.config.service.v1.HttpApiNamingConfig.Builder
        httpApiNamingConfigBuilder =
            ai.traceable.localprocessing.config.service.v1.HttpApiNamingConfig.newBuilder();
    Optional<TrieModelTrainingConfig> maybeTrieModelTrainingConfig =
        getTrieModelTrainingConfig(trainingConfigs);

    int embryonicThreshold =
        maybeTrieModelTrainingConfig
            .map(TrieModelTrainingConfig::getEmbryonicThreshold)
            .orElseGet(httpApiNamingConfig::getDefaultEmbryonicThreshold);

    maybeTrieModelTrainingConfig.ifPresent(
        trieModelTrainingConfig ->
            httpApiNamingConfigBuilder
                .addAllExtensions(trieModelTrainingConfig.getExtensions().getValuesList())
                .addAllSegmentWhitelistRegexes(
                    trieModelTrainingConfig.getAllowRegexList().getValuesList())
                .addAllWildcardConfigs(convertWildcardConfigs(trieModelTrainingConfig)));

    getCustomRulesListConfig(trainingConfigs)
        .ifPresent(
            customRulesListConfig ->
                httpApiNamingConfigBuilder.addAllApiNamingCustomRules(
                    convertCustomRules(customRulesListConfig)));

    httpApiNamingConfigBuilder.addAllFallbackWildcardRegexes(
        httpApiNamingConfig.getFallbackRegexes());
    String hash = uuidGenerator.generateId(httpApiNamingConfigBuilder.build());
    if (!hash.equals(configHash)) {
      return new HttpApiNamingConfigInfo(
          httpApiNamingConfigBuilder.setHash(hash).build(), embryonicThreshold);
    }
    return new HttpApiNamingConfigInfo(
        ai.traceable.localprocessing.config.service.v1.HttpApiNamingConfig.newBuilder()
            .setHash(hash)
            .build(),
        embryonicThreshold);
  }

  private List<WildcardConfig> convertWildcardConfigs(
      TrieModelTrainingConfig trieModelTrainingConfig) {
    // Ordering of wildcard configs decides the priority
    // ID > LOW_CARDINALITY > HIGH_CARDINALITY > MEDIUM_CARDINALITY
    return List.of(
        buildWildcardConfig(WildcardType.WILDCARD_TYPE_ID, trieModelTrainingConfig.getIds()),
        buildWildcardConfig(
            WildcardType.WILDCARD_TYPE_LOW_CARDINALITY,
            trieModelTrainingConfig.getLowCardinality()),
        buildWildcardConfig(
            WildcardType.WILDCARD_TYPE_HIGH_CARDINALITY,
            trieModelTrainingConfig.getHighCardinality()),
        buildWildcardConfig(
            WildcardType.WILDCARD_TYPE_MEDIUM_CARDINALITY,
            trieModelTrainingConfig.getMediumCardinality()));
  }

  private WildcardConfig buildWildcardConfig(
      WildcardType wildcardType, ThresholdRegexConfig thresholdRegexConfig) {
    return WildcardConfig.newBuilder()
        .setWildcardType(wildcardType)
        .addAllIdentificationRegexes(thresholdRegexConfig.getRegexList().getValuesList())
        .build();
  }

  private List<HttpApiNamingCustomRule> convertCustomRules(
      CustomRulesListConfig customRulesListConfig) {
    // TODO: Order the rules, once the priority is available from training config service APIs
    return customRulesListConfig.getCustomRulesConfigList().stream()
        .map(
            customRuleConfig ->
                HttpApiNamingCustomRule.newBuilder()
                    .setRegexPattern(customRuleConfig.getRegex())
                    .setUrlPattern(customRuleConfig.getUrlPattern())
                    .build())
        .collect(Collectors.toUnmodifiableList());
  }

  private Optional<CustomRulesListConfig> getCustomRulesListConfig(
      List<TrainingConfig> trainingConfigs) {
    return trainingConfigs.stream()
        .filter(TrainingConfig::hasApiNamingTrainingConfig)
        .map(TrainingConfig::getApiNamingTrainingConfig)
        .filter(ApiNamingTrainingConfig::hasCustomRulesListConfig)
        .map(ApiNamingTrainingConfig::getCustomRulesListConfig)
        .findAny();
  }

  private Optional<TrieModelTrainingConfig> getTrieModelTrainingConfig(
      List<TrainingConfig> trainingConfigs) {
    return trainingConfigs.stream()
        .filter(TrainingConfig::hasApiNamingTrainingConfig)
        .map(TrainingConfig::getApiNamingTrainingConfig)
        .filter(ApiNamingTrainingConfig::hasTrieModelTrainingConfig)
        .map(ApiNamingTrainingConfig::getTrieModelTrainingConfig)
        .findAny();
  }
}
