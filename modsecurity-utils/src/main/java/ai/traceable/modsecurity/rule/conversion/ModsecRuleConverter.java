package ai.traceable.modsecurity.rule.conversion;

import ai.traceable.modsecurity.rule.api.v1.CustomModsecRule;
import java.util.List;

public interface ModsecRuleConverter {

  String NEW_LINES_DELIMITER = "\n\n";

  String getJNIValidatedModsecRule(CustomModsecRule customModsecRule) throws Exception;

  String getValidatedModsecRule(CustomModsecRule customModsecRule) throws Exception;

  String getModsecRulesBlob(List<CustomModsecRule> customModsecRules) throws Exception;
}
