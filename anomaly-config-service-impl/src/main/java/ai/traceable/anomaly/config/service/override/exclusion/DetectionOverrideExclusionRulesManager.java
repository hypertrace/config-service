package ai.traceable.anomaly.config.service.override.exclusion;

import ai.traceable.anomaly.config.service.v1.override.CreateDetectionExclusionRuleRequest;
import ai.traceable.anomaly.config.service.v1.override.DetectionExclusionRule;
import ai.traceable.anomaly.config.service.v1.override.GetDetectionExclusionRulesFilter;
import ai.traceable.config.utils.UuidGenerator;
import io.grpc.Status;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class DetectionOverrideExclusionRulesManager implements ExclusionRulesManager {
  private final DetectionOverrideRulesStore rulesStore;
  private final UuidGenerator uuidGenerator;

  @Inject
  DetectionOverrideExclusionRulesManager(
      DetectionOverrideRulesStore rulesStore, UuidGenerator uuidGenerator) {
    this.rulesStore = rulesStore;
    this.uuidGenerator = uuidGenerator;
  }

  @Override
  public List<DetectionExclusionRule> getDetectionExclusionRules(
      RequestContext requestContext, GetDetectionExclusionRulesFilter filter) {
    if (filter.equals(GetDetectionExclusionRulesFilter.getDefaultInstance())) {
      return rulesStore.getAllConfigData(requestContext);
    }
    return rulesStore.getAllConfigData(requestContext, filter);
  }

  @Override
  public DetectionExclusionRule createDetectionExclusionRule(
      RequestContext requestContext, CreateDetectionExclusionRuleRequest request) {
    String ruleId = uuidGenerator.generateRandomId();
    DetectionExclusionRule rule =
        DetectionExclusionRule.newBuilder()
            .setId(ruleId)
            .setDescription(request.getDescription())
            .setRuleScope(request.getRuleScope())
            .setConfig(request.getConfig())
            .build();
    return rulesStore.upsertObject(requestContext, rule).getData();
  }

  @Override
  public DetectionExclusionRule updateDetectionExclusionRule(
      RequestContext requestContext, DetectionExclusionRule rule) {
    String ruleId = rule.getId();
    if (!doesRuleExist(requestContext, ruleId)) {
      throw new NoSuchElementException(
          String.format(
              "Unable to update as detection exclusion rule with id = %s does not exist", ruleId));
    }
    return rulesStore.upsertObject(requestContext, rule).getData();
  }

  @Override
  public void deleteDetectionExclusionRule(RequestContext requestContext, String id) {
    rulesStore
        .deleteObject(requestContext, id)
        .map(ContextualConfigObject::getData)
        .orElseThrow(Status.NOT_FOUND::asRuntimeException);
  }

  private boolean doesRuleExist(RequestContext requestContext, String id) {
    Optional<DetectionExclusionRule> optionalRule = rulesStore.getData(requestContext, id);
    return optionalRule.isPresent();
  }
}
