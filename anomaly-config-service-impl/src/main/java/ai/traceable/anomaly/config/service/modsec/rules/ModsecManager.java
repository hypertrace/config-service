package ai.traceable.anomaly.config.service.modsec.rules;

import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleType;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecCrsRulesData;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface ModsecManager {
  List<ModsecCrsRulesData> getModsecCrsRules(
      RequestContext requestContext,
      ModsecRuleVersion modsecRuleVersion,
      List<AnomalySubRuleType> requestTypes,
      boolean removeDisabledRules,
      AnomalyConfigScope scope);
}
