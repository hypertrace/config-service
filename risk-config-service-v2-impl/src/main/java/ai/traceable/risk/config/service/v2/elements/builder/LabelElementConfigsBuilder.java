package ai.traceable.risk.config.service.v2.elements.builder;

import ai.traceable.risk.config.service.v2.RiskElementConfig;
import ai.traceable.risk.config.service.v2.elements.normalizer.LabelIdNormalizer;
import com.google.inject.Inject;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor(onConstructor_ = {@Inject})
public class LabelElementConfigsBuilder extends RiskElementConfigBuilder {

  private static final Set<String> EXCLUDED_LABEL_IDS = Set.of("External", "Internal");

  private final LabelPredicateBuilder predicateBuilder;
  private final LabelIdNormalizer labelIdNormalizer;

  public Collection<RiskElementConfig> mergeRiskElementConfigs(
      Collection<RiskElementConfig> highPriorityRiskElementConfigsList,
      Collection<RiskElementConfig> lowPriorityRiskElementConfigsList) {
    List<RiskElementConfig> normalizedHighPriorityRiskElementConfigs =
        normalizeElementConfigs(highPriorityRiskElementConfigsList);
    List<RiskElementConfig> normalizedLowPriorityRiskElementConfigs =
        normalizeElementConfigs(lowPriorityRiskElementConfigsList);
    Map<String, RiskElementConfig> lowPriorityElementConfigsMap =
        normalizedLowPriorityRiskElementConfigs.stream()
            .collect(Collectors.toMap(RiskElementConfig::getId, Function.identity()));
    for (RiskElementConfig riskElementConfig : normalizedHighPriorityRiskElementConfigs) {
      String id = riskElementConfig.getId();
      if (isExcludedLabel(id)) {
        continue;
      }
      RiskElementConfig mergedElementConfig = riskElementConfig;
      if (lowPriorityElementConfigsMap.containsKey(id)) {
        mergedElementConfig = mergeConfigs(riskElementConfig, lowPriorityElementConfigsMap.get(id));
      }
      lowPriorityElementConfigsMap.put(id, mergedElementConfig);
    }
    return predicateBuilder.buildRiskElementConfigsWithPredicates(
        lowPriorityElementConfigsMap.values());
  }

  private List<RiskElementConfig> normalizeElementConfigs(
      Collection<RiskElementConfig> elementConfigs) {
    return elementConfigs.stream()
        .map(this::buildNormalizedElementConfig)
        .collect(Collectors.toUnmodifiableList());
  }

  private RiskElementConfig buildNormalizedElementConfig(RiskElementConfig elementConfig) {
    String normalizedId = labelIdNormalizer.normalizeId(elementConfig.getId());
    return elementConfig.toBuilder().setId(normalizedId).build();
  }

  private boolean isExcludedLabel(String labelId) {
    return EXCLUDED_LABEL_IDS.contains(labelId);
  }
}
