package ai.traceable.risk.config.service.v2.elements.builder;

import ai.traceable.risk.config.service.v2.RiskElementConfig;
import com.google.inject.Inject;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor(onConstructor_ = {@Inject})
public class LabelElementConfigsBuilder extends RiskElementConfigBuilder {

  private static final Set<String> EXCLUDED_LABEL_IDS = Set.of("external");

  private final LabelPredicateBuilder predicateBuilder;

  public Collection<RiskElementConfig> mergeRiskElementConfigs(
      Collection<RiskElementConfig> highPriorityRiskElementConfigsList,
      Collection<RiskElementConfig> lowPriorityRiskElementConfigsList) {
    Map<String, RiskElementConfig> lowPriorityElementConfigsMap =
        lowPriorityRiskElementConfigsList.stream()
            .collect(Collectors.toMap(RiskElementConfig::getId, Function.identity()));
    for (RiskElementConfig riskElementConfig : highPriorityRiskElementConfigsList) {
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

  private boolean isExcludedLabel(String labelId) {
    return EXCLUDED_LABEL_IDS.contains(labelId);
  }
}
