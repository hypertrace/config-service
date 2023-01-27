package ai.traceable.detection.exclusion.config.service.v1.rules;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRule;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleInfo;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleScope;
import ai.traceable.detection.exclusion.config.service.v1.GetRulesFilter;
import io.grpc.Status;
import java.util.List;
import javax.inject.Inject;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class DetectionExclusionRulesManager implements RulesManager {
  private final DetectionExclusionRulesStore rulesStore;
  private final UuidGenerator uuidGenerator;

  @Inject
  public DetectionExclusionRulesManager(
      DetectionExclusionRulesStore rulesStore, UuidGenerator uuidGenerator) {
    this.rulesStore = rulesStore;
    this.uuidGenerator = uuidGenerator;
  }

  @Override
  public List<DetectionExclusionRule> getDetectionExclusionRules(
      RequestContext requestContext, GetRulesFilter filter) {
    if (filter.equals(GetRulesFilter.getDefaultInstance())) {
      return rulesStore.getAllConfigData(requestContext);
    }
    return rulesStore.getAllConfigData(requestContext, filter);
  }

  @Override
  public DetectionExclusionRule updateDetectionExclusionRule(
      RequestContext requestContext, DetectionExclusionRule rule) {
    return rulesStore.upsertObject(requestContext, rule).getData();
  }

  @Override
  public DetectionExclusionRule createDetectionExclusionRule(
      RequestContext requestContext,
      DetectionExclusionRuleScope ruleScope,
      DetectionExclusionRuleInfo ruleInfo) {
    DetectionExclusionRule rule =
        DetectionExclusionRule.newBuilder()
            .setId(uuidGenerator.generateRandomId())
            .setRuleInfo(ruleInfo)
            .setRuleScope(ruleScope)
            .build();
    return rulesStore.upsertObject(requestContext, rule).getData();
  }

  @Override
  public void deleteDetectionExclusionRule(RequestContext requestContext, String ruleId) {
    rulesStore
        .deleteObject(requestContext, ruleId)
        .orElseThrow(() -> Status.NOT_FOUND.asRuntimeException(requestContext.buildTrailers()));
  }
}
