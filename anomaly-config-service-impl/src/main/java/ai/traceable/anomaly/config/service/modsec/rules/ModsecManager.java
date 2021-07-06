package ai.traceable.anomaly.config.service.modsec.rules;

import ai.traceable.anomaly.config.service.v1.AnomalySubRuleType;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecCrsRulesData;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface ModsecManager {
  List<ModsecCrsRulesData> getModsecCrsRules(
      RequestContext requestContext, List<AnomalySubRuleType> requestTypes);
}
