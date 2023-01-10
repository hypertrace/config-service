package ai.traceable.customsignature.config.service.modsec.directives;

import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;

public interface ModsecDirectivesManager {
  String getModsecHeader(ModsecRuleVersion modsecRuleVersion);
}
