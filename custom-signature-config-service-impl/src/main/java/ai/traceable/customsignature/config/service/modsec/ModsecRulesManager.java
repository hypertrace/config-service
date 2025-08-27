package ai.traceable.customsignature.config.service.modsec;

import ai.traceable.anomaly.config.service.v1.modsec.ModsecCrsRulesTarget;
import ai.traceable.customsignature.config.service.v1.ClauseGroup;
import ai.traceable.customsignature.config.service.v1.CustomModsecRuleVersion;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesResponse;
import ai.traceable.customsignature.config.service.v1.RuleDefinition;
import io.grpc.Status;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface ModsecRulesManager {

  GetCustomSignatureModsecRulesResponse getModsecRules(
      RequestContext requestContext,
      List<CustomSignatureRule> customSignatureRules,
      CustomModsecRuleVersion customModsecRuleVersion,
      boolean includeAllPartialModsecRules,
      ModsecCrsRulesTarget modsecCrsRulesTarget,
      List<String> serviceNames);

  Status validateModsecRule(String ruleName, RuleDefinition ruleDefinition);

  boolean containsModsecConvertibleClauses(ClauseGroup clauseGroup);
}
