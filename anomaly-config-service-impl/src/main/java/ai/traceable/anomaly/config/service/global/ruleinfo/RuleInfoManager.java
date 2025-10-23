package ai.traceable.anomaly.config.service.global.ruleinfo;

import ai.traceable.anomaly.config.service.v1.AnomalyEventFamily;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleTypeVersion;
import ai.traceable.anomaly.config.service.v1.RuleVersion;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface RuleInfoManager {
  List<AnomalyRuleInfo> getModsecAnomalyRuleInfo(
      RequestContext requestContext,
      ModsecRuleVersion ruleVersion,
      RuleVersion version,
      boolean useTestModsecRules);

  List<AnomalyRuleInfo> getAllApiProtectionAnomalyRuleInfo(
      RequestContext requestContext, RuleVersion version);

  List<AnomalyRuleInfo> getAnomalyRuleInfos(
      List<AnomalyEventFamily> eventFamilies,
      ModsecRuleVersion ruleVersion,
      List<AnomalyRuleTypeVersion> anomalyRuleTypeVersions,
      boolean useTestModsecRules);
}
