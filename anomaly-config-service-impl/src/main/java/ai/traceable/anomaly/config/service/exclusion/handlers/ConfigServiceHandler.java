package ai.traceable.anomaly.config.service.exclusion.handlers;

import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyExclusionRuleConfig;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.hypertrace.config.objectstore.ConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class ConfigServiceHandler {
  private final AnomalyExclusionRuleConfigStore anomalyExclusionRuleConfigStore;

  @Inject
  ConfigServiceHandler(AnomalyExclusionRuleConfigStore anomalyExclusionRuleConfigStore) {
    this.anomalyExclusionRuleConfigStore = anomalyExclusionRuleConfigStore;
  }

  public AnomalyExclusionRuleConfig getExclusionConfigByRuleId(
      String ruleId, RequestContext requestContext) {
    return this.anomalyExclusionRuleConfigStore.getData(requestContext, ruleId).orElseThrow();
  }

  public List<AnomalyExclusionRuleConfig> getAllExclusionConfigs(RequestContext requestContext) {
    return this.anomalyExclusionRuleConfigStore.getAllObjects(requestContext).stream()
        .map(ConfigObject::getData)
        .collect(Collectors.toUnmodifiableList());
  }

  public AnomalyExclusionRuleConfig upsertExclusionConfigByRuleId(
      AnomalyExclusionRuleConfig anomalyExclusionRuleConfig, RequestContext requestContext) {
    return this.anomalyExclusionRuleConfigStore
        .upsertObject(requestContext, anomalyExclusionRuleConfig)
        .getData();
  }

  public void deleteExclusionConfigByRuleId(String ruleId, RequestContext requestContext) {
    this.anomalyExclusionRuleConfigStore.deleteObject(requestContext, ruleId);
  }
}
