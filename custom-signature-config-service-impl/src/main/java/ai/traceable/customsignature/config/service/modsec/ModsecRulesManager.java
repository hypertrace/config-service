package ai.traceable.customsignature.config.service.modsec;

import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesResponse;
import java.util.List;

public interface ModsecRulesManager {

  GetCustomSignatureModsecRulesResponse getModsecRules(
      List<CustomSignatureRule> customSignatureRules);
}
