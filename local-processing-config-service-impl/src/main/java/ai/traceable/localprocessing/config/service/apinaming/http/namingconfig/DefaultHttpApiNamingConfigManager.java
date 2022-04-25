package ai.traceable.localprocessing.config.service.apinaming.http.namingconfig;

import ai.traceable.anomaly.config.service.v1.trainer.ApiNamingTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.CustomRulesListConfig;
import ai.traceable.anomaly.config.service.v1.trainer.ThresholdRegexConfig;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.TrieModelTrainingConfig;
import ai.traceable.localprocessing.config.service.apinaming.http.utils.HttpApiNamingConfigInfo;
import ai.traceable.localprocessing.config.service.config.http.HttpApiNamingConfig;
import ai.traceable.localprocessing.config.service.utils.UuidGenerator;
import ai.traceable.localprocessing.config.service.v1.ApiNamingPattern;
import ai.traceable.localprocessing.config.service.v1.HttpApiNamingCustomRule;
import ai.traceable.localprocessing.config.service.v1.Segment;
import ai.traceable.localprocessing.config.service.v1.Wildcard;
import ai.traceable.platform.apientity.TrieNodeType;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.span.processing.config.service.v1.ApiNamingRule;
import org.hypertrace.span.processing.config.service.v1.ApiNamingRuleConfig;
import org.hypertrace.span.processing.config.service.v1.SegmentMatchingBasedConfig;

@Slf4j
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

    List<String> segmentWhitelistRegexes = new ArrayList<>();
    List<String> extensions = new ArrayList<>();
    EnumMap<TrieNodeType, String> wildcardConfigsMap = new EnumMap<>(TrieNodeType.class);
    if (maybeTrieModelTrainingConfig.isPresent()) {
      TrieModelTrainingConfig trieModelTrainingConfig = maybeTrieModelTrainingConfig.get();
      extensions = trieModelTrainingConfig.getExtensions().getValuesList();
      segmentWhitelistRegexes = trieModelTrainingConfig.getAllowRegexList().getValuesList();
      wildcardConfigsMap = buildWildcardConfigMap(trieModelTrainingConfig);
    }
    httpApiNamingConfigBuilder.addAllSegmentWhitelistRegexes(segmentWhitelistRegexes);
    httpApiNamingConfigBuilder.addAllApiNamingCustomRules(getCustomRulesList(apiNamingRules));
    getCustomRulesListConfig(trainingConfigs)
        .ifPresent(
            customRulesListConfig ->
                httpApiNamingConfigBuilder.addAllApiNamingCustomRules(
                    convertCustomRule(customRulesListConfig)));

    httpApiNamingConfigBuilder.addAllFallbackWildcardRegexes(
        httpApiNamingConfig.getFallbackRegexes());
    String hash = uuidGenerator.generateId(httpApiNamingConfigBuilder.build());
    if (!hash.equals(configHash)) {
      return new HttpApiNamingConfigInfo(
          httpApiNamingConfigBuilder.setHash(hash).build(),
          embryonicThreshold,
          wildcardConfigsMap,
          segmentWhitelistRegexes,
          extensions);
    }
    return new HttpApiNamingConfigInfo(
        ai.traceable.localprocessing.config.service.v1.HttpApiNamingConfig.newBuilder()
            .setHash(hash)
            .build(),
        embryonicThreshold,
        wildcardConfigsMap,
        segmentWhitelistRegexes,
        extensions);
  }

  private EnumMap<TrieNodeType, String> buildWildcardConfigMap(
      TrieModelTrainingConfig trieModelTrainingConfig) {
    EnumMap<TrieNodeType, String> wildcardConfigMap = new EnumMap<>(TrieNodeType.class);
    wildcardConfigMap.put(
        TrieNodeType.ID, buildWildcardIdentificationRegex(trieModelTrainingConfig.getIds()));
    wildcardConfigMap.put(
        TrieNodeType.LOW_CARDINALITY,
        buildWildcardIdentificationRegex(trieModelTrainingConfig.getLowCardinality()));
    wildcardConfigMap.put(
        TrieNodeType.MEDIUM_CARDINALITY,
        buildWildcardIdentificationRegex(trieModelTrainingConfig.getMediumCardinality()));
    wildcardConfigMap.put(
        TrieNodeType.HIGH_CARDINALITY,
        buildWildcardIdentificationRegex(trieModelTrainingConfig.getHighCardinality()));
    return wildcardConfigMap;
  }

  private String buildWildcardIdentificationRegex(ThresholdRegexConfig thresholdRegexConfig) {
    return String.join("|", thresholdRegexConfig.getRegexList().getValuesList());
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

  private List<HttpApiNamingCustomRule> convertCustomRule(
      CustomRulesListConfig customRulesListConfig) {
    // TODO: Order the rules, once the priority is available from training config service APIs
    return customRulesListConfig.getCustomRulesConfigList().stream()
        .map(
            customRuleConfig -> {
              String[] regexes = customRuleConfig.getRegex().split("/");
              String[] values = customRuleConfig.getUrlPattern().split("/");
              List<Segment> segments = new ArrayList<>();
              for (int i = 0; i < regexes.length && i < values.length; i++) {
                String regex = regexes[i];
                String value = values[i];
                if (regex.equals(value)) {
                  segments.add(Segment.newBuilder().setName(regex).build());
                } else {
                  segments.add(
                      Segment.newBuilder()
                          .setWildcard(
                              Wildcard.newBuilder()
                                  .setIdentificationRegex(regex)
                                  .setReplacementPattern(value)
                                  .build())
                          .build());
                }
              }
              return HttpApiNamingCustomRule.newBuilder()
                  .setApiNamingPattern(
                      ApiNamingPattern.newBuilder().addAllSegments(segments).build())
                  .build();
            })
        .collect(Collectors.toUnmodifiableList());
  }

  private List<HttpApiNamingCustomRule> getCustomRulesList(List<ApiNamingRule> apiNamingRules) {
    // TODO: Order the rules, once the priority is available from training config service APIs
    return apiNamingRules.stream()
        .map(
            apiNamingRule ->
                convertCustomRule(
                    apiNamingRule.getId(), apiNamingRule.getRuleInfo().getRuleConfig()))
        .collect(Collectors.toUnmodifiableList());
  }

  private HttpApiNamingCustomRule convertCustomRule(
      String customRuleId, ApiNamingRuleConfig apiNamingRuleConfig) {
    List<Segment> segmentList = new ArrayList<>();
    if (apiNamingRuleConfig.hasSegmentMatchingBasedConfig()) {
      SegmentMatchingBasedConfig segmentMatchingBasedConfig =
          apiNamingRuleConfig.getSegmentMatchingBasedConfig();
      Iterator<String> regexItr = segmentMatchingBasedConfig.getRegexesList().iterator();
      Iterator<String> valueItr = segmentMatchingBasedConfig.getValuesList().iterator();
      while (regexItr.hasNext() && valueItr.hasNext()) {
        String regex = regexItr.next();
        String value = valueItr.next();
        if (regex.equals(value)) {
          segmentList.add(Segment.newBuilder().setName(regex).build());
        } else {
          segmentList.add(
              Segment.newBuilder()
                  .setWildcard(
                      Wildcard.newBuilder()
                          .setIdentificationRegex(regex)
                          .setReplacementPattern(value)
                          .build())
                  .build());
        }
      }
    }
    return HttpApiNamingCustomRule.newBuilder()
        .setId(customRuleId)
        .setApiNamingPattern(ApiNamingPattern.newBuilder().addAllSegments(segmentList).build())
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
