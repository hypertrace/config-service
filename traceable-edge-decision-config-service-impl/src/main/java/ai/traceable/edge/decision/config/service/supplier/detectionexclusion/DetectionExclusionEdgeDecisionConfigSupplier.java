package ai.traceable.edge.decision.config.service.supplier.detectionexclusion;

import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionConfigServiceGrpc;
import ai.traceable.detection.exclusion.config.service.v1.ExclusionTarget;
import ai.traceable.detection.exclusion.config.service.v1.GetDetectionExclusionEdgeDecisionRulesRequest;
import ai.traceable.detection.exclusion.config.service.v1.GetRulesFilter;
import ai.traceable.detection.exclusion.config.service.v1.RuleEvaluationPoint;
import ai.traceable.edge.decision.config.service.supplier.EdgeDecisionEngineConfigSupplier;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.edge.decision.config.service.v1.EdgeInputKind;
import ai.traceable.edge.decision.config.service.v1.GetEdgeDecisionConfigsFilter;
import com.google.inject.Inject;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = @Inject)
public class DetectionExclusionEdgeDecisionConfigSupplier
    implements EdgeDecisionEngineConfigSupplier {

  private static final GetRulesFilter DETECTION_EXCLUSION_RULES_FILTER =
      GetRulesFilter.newBuilder()
          .addExclusionTargets(ExclusionTarget.EXCLUSION_TARGET_BLOCK)
          .addExclusionTargets(ExclusionTarget.EXCLUSION_TARGET_ALLOW)
          .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
          .setDisabled(false)
          .build();
  private final DetectionExclusionConfigServiceGrpc.DetectionExclusionConfigServiceBlockingStub
      stub;

  @Override
  public String getName() {
    return DetectionExclusionEdgeDecisionConfigSupplier.class.getSimpleName();
  }

  @Override
  public EdgeDecisionEngineConfig get(
      RequestContext requestContext, GetEdgeDecisionConfigsFilter filter) {
    if (filter != null) {
      List<EdgeInputKind> inputKinds = filter.getEdgeInputKindsList();
      if (!inputKinds.isEmpty()
          && !inputKinds.contains(EdgeInputKind.EDGE_INPUT_KIND_HTTP_REQUEST)) {
        // currently, this supplier doesn't provide any other input kind.
        return EdgeDecisionEngineConfig.getDefaultInstance();
      }
    }
    try {
      return requestContext.call(
          () ->
              stub.getDetectionExclusionEdgeDecisionRules(
                      GetDetectionExclusionEdgeDecisionRulesRequest.newBuilder()
                          .setFilter(DETECTION_EXCLUSION_RULES_FILTER)
                          .build())
                  .getEdgeDecisionEngineConfig());
    } catch (Exception e) {
      log.error("Error in fetching detection exclusion rules for edge decision config", e);
      return EdgeDecisionEngineConfig.getDefaultInstance();
    }
  }
}
