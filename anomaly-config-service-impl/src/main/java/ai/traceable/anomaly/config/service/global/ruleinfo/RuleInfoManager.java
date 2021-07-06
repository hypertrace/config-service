package ai.traceable.anomaly.config.service.global.ruleinfo;

import ai.traceable.anomaly.config.service.v1.AnomalyEventFamily;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface RuleInfoManager {
  List<AnomalyRuleInfo> getAnomalyRuleInfos(
      RequestContext requestContext, List<AnomalyEventFamily> eventFamilies);
}
