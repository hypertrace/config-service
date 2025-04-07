package ai.traceable.detection.exclusion.config.service.v1.rules.modsec;

import ai.traceable.customsignature.config.service.modsec.CustomModsecRuleConverter;
import ai.traceable.customsignature.config.service.v1.Clause;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Singleton
public class ModsecBlobConverter {
  private static final String EMPTY_BLOB = "";
  private static final String MODSEC_MATCH_MESSAGE =
      "URL Regex and key-value conditions corresponding to exclusion rule - %s";
  private static final String MODSEC_LOG_MESSAGE =
      "Matched URL and request criteria corresponding to exclusion Rule";

  private final CustomModsecRuleConverter customModsecRuleConverter;

  @Inject
  public ModsecBlobConverter(CustomModsecRuleConverter customModsecRuleConverter) {
    this.customModsecRuleConverter = customModsecRuleConverter;
  }

  public ModsecBlobResult convertToModsecRule(
      String ruleIdentifier, List<Clause> ANDClausesList, AtomicLong modsecIdAssignment) {
    try {
      if (ANDClausesList.isEmpty()) {
        return new ModsecBlobResult(EMPTY_BLOB, ruleIdentifier);
      }
      // Exclusions do not require verification by Coraza, as we rely on ModSecurity to handle
      // exclusions.
      return new ModsecBlobResult(
          customModsecRuleConverter.getJNIValidatedModsecRuleWithCustomLogMsg(
              modsecIdAssignment.getAndIncrement(),
              ruleIdentifier,
              String.format(MODSEC_MATCH_MESSAGE, ruleIdentifier),
              ANDClausesList,
              MODSEC_LOG_MESSAGE),
          ruleIdentifier);
    } catch (Exception e) {
      log.warn("Cannot convert exclusion rule with id {} into modsec rule", ruleIdentifier, e);
      return new ModsecBlobResult(EMPTY_BLOB, "");
    }
  }

  @Value
  public static class ModsecBlobResult {
    String modsecBlob;
    String ruleId;
  }
}
