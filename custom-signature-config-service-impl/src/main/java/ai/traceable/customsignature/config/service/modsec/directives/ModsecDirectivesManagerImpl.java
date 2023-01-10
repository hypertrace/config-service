package ai.traceable.customsignature.config.service.modsec.directives;

import ai.traceable.anomaly.config.service.registry.modsec.ModsecRulesRegistry;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import com.google.inject.Inject;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ModsecDirectivesManagerImpl implements ModsecDirectivesManager {
  private final ModsecRulesRegistry modsecRulesRegistry;
  private static final String NEW_LINES_DELIMITER = "\n\n";

  @Inject
  public ModsecDirectivesManagerImpl(ModsecRulesRegistry modsecRulesRegistry) {
    this.modsecRulesRegistry = modsecRulesRegistry;
  }

  @Override
  public String getModsecHeader(ModsecRuleVersion modsecRuleVersion) {
    try {
      return modsecRulesRegistry.getModsecHeader(modsecRuleVersion) + NEW_LINES_DELIMITER;
    } catch (Exception e) {
      log.error("Failed to get modsec directives for custom modsec rules", e);
      throw new RuntimeException("Failed to get modsec directives for custom modsec rules", e);
    }
  }
}
