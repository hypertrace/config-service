package ai.traceable.customsignature.config.service.modsec;

import ai.traceable.customsignature.config.service.v1.CustomModsecRuleVersion;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesResponse;
import ai.traceable.customsignature.config.service.v1.RuleDefinition;
import io.grpc.Status;
import java.util.List;

public interface ModsecRulesManager {

  GetCustomSignatureModsecRulesResponse getModsecRules(
      List<CustomSignatureRule> customSignatureRules,
      CustomModsecRuleVersion customModsecRuleVersion);

  Status validateModsecRule(String ruleName, RuleDefinition ruleDefinition);
}
