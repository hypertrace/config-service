package ai.traceable.anomaly.config.service.modsec.rules;

import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleType;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecCrsRulesData;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecCrsRulesTarget;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Builder;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface ModsecManager {
  ModsecCrsRules getModsecCrsRules(
      RequestContext requestContext,
      ModsecRuleVersion modsecRuleVersion,
      ModsecCrsRulesTarget rulesTarget,
      @Deprecated List<AnomalySubRuleType> subRuleTypes,
      boolean removeDisabledRules,
      AnomalyConfigScope scope);

  @Builder
  class ModsecCrsRules {
    private final Map<AnomalySubRuleType, String> modsecBlobsForRuleTypes;
    private final String aggregatedModsecBlob;

    public ModsecCrsRules() {
      modsecBlobsForRuleTypes = Map.of();
      aggregatedModsecBlob = "";
    }

    public ModsecCrsRules(
        Map<AnomalySubRuleType, String> modsecBlobsForRuleTypes, String aggregatedModsecBlob) {
      this.modsecBlobsForRuleTypes = modsecBlobsForRuleTypes;
      this.aggregatedModsecBlob = aggregatedModsecBlob;
    }

    public String getAggregatedModsecBlob() {
      return aggregatedModsecBlob;
    }

    public List<ModsecCrsRulesData> getModsecCrsRulesData() {
      return modsecBlobsForRuleTypes.entrySet().stream()
          .map(
              entry ->
                  ModsecCrsRulesData.newBuilder()
                      .setSubRuleType(entry.getKey())
                      .setModsecCrsRulesBlob(entry.getValue())
                      .build())
          .collect(Collectors.toUnmodifiableList());
    }
  }
}
