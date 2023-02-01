package ai.traceable.localprocessing.config.service.apinaming.http.namingconfig;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Streams.zip;

import ai.traceable.anomaly.config.service.v1.trainer.ApiNamingTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.CustomRulesListConfig;
import ai.traceable.anomaly.config.service.v1.trainer.ThresholdRegexConfig;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.TrieModelTrainingConfig;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.localprocessing.config.service.apinaming.http.utils.HttpApiNamingConfigInfo;
import ai.traceable.localprocessing.config.service.config.http.HttpApiNamingConfig;
import ai.traceable.localprocessing.config.service.v1.ApiNamingPattern;
import ai.traceable.localprocessing.config.service.v1.HttpApiNamingCustomRule;
import ai.traceable.localprocessing.config.service.v1.Segment;
import ai.traceable.localprocessing.config.service.v1.Wildcard;
import ai.traceable.platform.apientity.http.model.NodeType;
import ai.traceable.span.processing.config.service.v1.ApiNamingRule;
import ai.traceable.span.processing.config.service.v1.ApiNamingRuleConfig;
import ai.traceable.span.processing.config.service.v1.ApiSpecBasedConfig;
import ai.traceable.span.processing.config.service.v1.AstScanBasedConfig;
import ai.traceable.span.processing.config.service.v1.SegmentMatchingBasedConfig;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DefaultHttpApiNamingConfigManager implements HttpApiNamingConfigManager {

  private final UuidGenerator uuidGenerator;
  private final HttpApiNamingConfig httpApiNamingConfig;
  private static final String OR_DELIMITER = "|";
  private static final String DEFAULT_MEDIUM_CARDINALITY_WILDCARD_IDENTIFICATION_REGEX = "^.*$";

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
            .filter(TrieModelTrainingConfig::hasEmbryonicThreshold)
            .map(TrieModelTrainingConfig::getEmbryonicThreshold)
            .orElseGet(httpApiNamingConfig::getDefaultEmbryonicThreshold);

    int maxNumberOfTriePaths =
        maybeTrieModelTrainingConfig
            .filter(TrieModelTrainingConfig::hasMaxNumberOfTriePaths)
            .map(TrieModelTrainingConfig::getMaxNumberOfTriePaths)
            .orElseGet(httpApiNamingConfig::getDefaultMaxNumberOfTriePaths);

    List<String> segmentWhitelistRegexes = new ArrayList<>();
    List<String> extensions = new ArrayList<>();
    EnumMap<NodeType, String> wildcardConfigsMap = new EnumMap<>(NodeType.class);
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
          extensions,
          maxNumberOfTriePaths);
    }
    return new HttpApiNamingConfigInfo(
        ai.traceable.localprocessing.config.service.v1.HttpApiNamingConfig.newBuilder()
            .setHash(hash)
            .build(),
        embryonicThreshold,
        wildcardConfigsMap,
        segmentWhitelistRegexes,
        extensions,
        maxNumberOfTriePaths);
  }

  private EnumMap<NodeType, String> buildWildcardConfigMap(
      TrieModelTrainingConfig trieModelTrainingConfig) {
    EnumMap<NodeType, String> wildcardConfigMap = new EnumMap<>(NodeType.class);
    wildcardConfigMap.put(
        NodeType.ID, buildWildcardIdentificationRegex(trieModelTrainingConfig.getIds()));
    wildcardConfigMap.put(
        NodeType.LOW_CARDINALITY,
        buildWildcardIdentificationRegex(trieModelTrainingConfig.getLowCardinality()));
    wildcardConfigMap.put(
        NodeType.MEDIUM_CARDINALITY,
        buildMediumCardinalityWildcardIdentificationRegex(trieModelTrainingConfig));
    wildcardConfigMap.put(
        NodeType.HIGH_CARDINALITY,
        buildWildcardIdentificationRegex(trieModelTrainingConfig.getHighCardinality()));

    return wildcardConfigMap;
  }

  private String buildWildcardIdentificationRegex(ThresholdRegexConfig thresholdRegexConfig) {
    return String.join(OR_DELIMITER, thresholdRegexConfig.getRegexList().getValuesList());
  }

  private String buildMediumCardinalityWildcardIdentificationRegex(
      TrieModelTrainingConfig trieModelTrainingConfig) {
    if (trieModelTrainingConfig.getMediumCardinality().getRegexList().getValuesCount() != 0) {
      return buildWildcardIdentificationRegex(trieModelTrainingConfig.getMediumCardinality());
    }

    // Regex which matches all valid extensions
    String extensionRegex =
        trieModelTrainingConfig.getExtensions().getValuesList().stream()
            .map(extension -> ".*\\." + extension)
            .collect(Collectors.joining(OR_DELIMITER));

    // Build a regex which includes everything not matching with other regexes
    String otherRegexes =
        Stream.of(
                buildWildcardIdentificationRegex(trieModelTrainingConfig.getIds()),
                buildWildcardIdentificationRegex(trieModelTrainingConfig.getLowCardinality()),
                buildWildcardIdentificationRegex(trieModelTrainingConfig.getHighCardinality()),
                String.join(
                    OR_DELIMITER, trieModelTrainingConfig.getAllowRegexList().getValuesList()),
                extensionRegex)
            .filter(s -> !isNullOrEmpty(s))
            .collect(Collectors.joining(OR_DELIMITER));

    // Look ahead negative regex of other regexes
    String negativeLookAheadOfOtherRegexes = "";
    if (!otherRegexes.isEmpty()) {
      negativeLookAheadOfOtherRegexes = "(?!^(" + otherRegexes + ")$)";
    }
    return negativeLookAheadOfOtherRegexes
        + DEFAULT_MEDIUM_CARDINALITY_WILDCARD_IDENTIFICATION_REGEX;
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
    List<Segment> segmentList;

    switch (apiNamingRuleConfig.getRuleConfigCase()) {
      case SEGMENT_MATCHING_BASED_CONFIG:
        SegmentMatchingBasedConfig segmentMatchingBasedConfig =
            apiNamingRuleConfig.getSegmentMatchingBasedConfig();
        segmentList =
            buildSegmentList(
                segmentMatchingBasedConfig.getRegexesList(),
                segmentMatchingBasedConfig.getValuesList());
        break;
      case API_SPEC_BASED_CONFIG:
        ApiSpecBasedConfig apiSpecBasedConfig = apiNamingRuleConfig.getApiSpecBasedConfig();
        segmentList =
            buildSegmentList(
                apiSpecBasedConfig.getRegexesList(), apiSpecBasedConfig.getValuesList());
        break;
      case AST_SCAN_BASED_CONFIG:
        AstScanBasedConfig astScanBasedConfig = apiNamingRuleConfig.getAstScanBasedConfig();
        segmentList =
            buildSegmentList(
                astScanBasedConfig.getRegexesList(), astScanBasedConfig.getValuesList());
        break;
      default:
        throw new UnsupportedOperationException("unknown rule config type: " + apiNamingRuleConfig);
    }

    return HttpApiNamingCustomRule.newBuilder()
        .setId(customRuleId)
        .setApiNamingPattern(ApiNamingPattern.newBuilder().addAllSegments(segmentList).build())
        .build();
  }

  private List<Segment> buildSegmentList(List<String> regexes, List<String> values) {
    return zip(
            regexes.stream(),
            values.stream(),
            (regex, value) -> {
              if (regex.equals(value)) {
                return Segment.newBuilder().setName(regex).build();
              } else {
                return Segment.newBuilder()
                    .setWildcard(
                        Wildcard.newBuilder()
                            .setIdentificationRegex(regex)
                            .setReplacementPattern(value)
                            .build())
                    .build();
              }
            })
        .collect(Collectors.toUnmodifiableList());
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
