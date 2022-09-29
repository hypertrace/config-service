package ai.traceable.anomaly.config.service.override.exclusion;

import ai.traceable.anomaly.config.service.v1.override.CreateDetectionExclusionRuleRequest;
import ai.traceable.anomaly.config.service.v1.override.DetectionExclusionRule;
import ai.traceable.anomaly.config.service.v1.override.GetDetectionExclusionRulesFilter;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface ExclusionRulesManager {

  List<DetectionExclusionRule> getDetectionExclusionRules(
      RequestContext requestContext, GetDetectionExclusionRulesFilter filter);

  DetectionExclusionRule createDetectionExclusionRule(
      RequestContext requestContext, CreateDetectionExclusionRuleRequest request);

  DetectionExclusionRule updateDetectionExclusionRule(
      RequestContext requestContext, DetectionExclusionRule rule);

  void deleteDetectionExclusionRule(RequestContext requestContext, String id)
      throws InvalidProtocolBufferException;
}
