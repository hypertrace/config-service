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
import org.hypertrace.span.processing.config.service.v1.ApiNamingRule;
import org.hypertrace.span.processing.config.service.v1.ApiNamingRuleConfig;
import org.hypertrace.span.processing.config.service.v1.SegmentMatchingBasedConfig;

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
      List<TrainingConfig> trainingConfigs, List<ApiNamingRule> apiNamingRules, String configHash) {
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

    httpApiNamingConfigBuilder.addAllApiNamingCustomRules(getCustomRulesList(apiNamingRules));

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

  // TODO: get rid of below two methods once done with migration
  private Optional<CustomRulesListConfig> getCustomRulesListConfig(
      List<TrainingConfig> trainingConfigs) {
    return trainingConfigs.stream()
        .filter(TrainingConfig::hasApiNamingTrainingConfig)
        .map(TrainingConfig::getApiNamingTrainingConfig)
        .filter(ApiNamingTrainingConfig::hasCustomRulesListConfig)
        .map(ApiNamingTrainingConfig::getCustomRulesListConfig)
        .findAny();
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

  private List<HttpApiNamingCustomRule> getCustomRulesList(List<ApiNamingRule> apiNamingRules) {
    // TODO: Order the rules, once the priority is available from training config service APIs
    return apiNamingRules.stream()
        .map(apiNamingRule -> convertCustomRules(apiNamingRule.getRuleInfo().getRuleConfig()))
        .collect(Collectors.toUnmodifiableList());
  }

  private HttpApiNamingCustomRule convertCustomRules(ApiNamingRuleConfig apiNamingRuleConfig) {
    String urlPattern = "";
    String regexPattern = "";
    if (apiNamingRuleConfig.hasSegmentMatchingBasedConfig()) {
      SegmentMatchingBasedConfig segmentMatchingBasedConfig =
          apiNamingRuleConfig.getSegmentMatchingBasedConfig();
      urlPattern = String.join("/", segmentMatchingBasedConfig.getRegexesList());
      regexPattern = String.join("/", segmentMatchingBasedConfig.getValuesList());
    }
    return HttpApiNamingCustomRule.newBuilder()
        .setUrlPattern(urlPattern)
        .setRegexPattern(regexPattern)
        .build();
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
