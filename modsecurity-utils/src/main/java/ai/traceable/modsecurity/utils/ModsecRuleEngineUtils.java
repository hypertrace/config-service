package ai.traceable.modsecurity.utils;

import ai.traceable.modsecurity.Attribute;
import ai.traceable.modsecurity.RuleEngine;
import ai.traceable.modsecurity.RuleMatch;
import io.grpc.Status;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ModsecRuleEngineUtils {
  private static final boolean loadNativeLibrarySuccess = loadNativeRuleEngineLibrary();

  private static boolean loadNativeRuleEngineLibrary() {
    try {
      RuleEngine.loadNativeLibrary();
      return true;
    } catch (IOException e) {
      log.warn("Failed loading rule engine native library with error:", e);
      return false;
    }
  }

  public static Status validate(String modsecRuleBlob) {
    if (loadNativeRuleEngineLibrary() == false) {
      log.warn("Skipping modsec SecRule validation.. Native libraries for rule engine not loaded!");
      return Status.UNKNOWN;
    }
    try {
      RuleEngine ruleEngine = RuleEngine.create(modsecRuleBlob);
      if (ruleEngine == null) {
        return Status.UNKNOWN.withDescription(
            String.format(
                "Null Rule Engine is getting returned for the Modsec Rule: %s", modsecRuleBlob));
      }
      RuleEngine.destroy(ruleEngine);
    } catch (Exception e) {
      return Status.INVALID_ARGUMENT
          .withCause(e)
          .withDescription(
              String.format(
                  "Exception while creating Rule Engine from Modsec Rule:%s", modsecRuleBlob));
    }
    return Status.OK;
  }

  public static List<RuleMatch> getModsecRuleMatches(
      String modsecRuleBlob, Map<String, String> attributesMap) {
    if (loadNativeRuleEngineLibrary() == false) {
      log.warn("Skipping modsec SecRule evaluation.. Native libraries for rule engine not loaded!");
      return Collections.emptyList();
    }
    RuleEngine ruleEngine = createRuleEngine(modsecRuleBlob);
    List<RuleMatch> matches = getModsecRuleMatches(ruleEngine, attributesMap);
    if (ruleEngine != null) {
      RuleEngine.destroy(ruleEngine);
    }
    return matches;
  }

  public static RuleEngine createRuleEngine(String modsecRuleBlob) {
    if (loadNativeRuleEngineLibrary() == false) {
      log.warn("Skipping modsec SecRule evaluation.. Native libraries for rule engine not loaded!");
      return null;
    }
    return RuleEngine.create(modsecRuleBlob);
  }

  public static List<RuleMatch> getModsecRuleMatches(
      RuleEngine ruleEngine, Map<String, String> attributesMap) {
    if (ruleEngine == null) {
      log.warn("Null Rule Engine found - skipping modsec SecRule evaluation..");
      return Collections.emptyList();
    }
    if (attributesMap == null) {
      log.warn("Null Rule Engine found - skipping modsec SecRule evaluation..");
      return Collections.emptyList();
    }
    ArrayList<Attribute> attributes =
        attributesMap.entrySet().stream()
            .map(entry -> new Attribute(entry.getKey(), entry.getValue()))
            .collect(Collectors.toCollection(ArrayList::new));
    List<RuleMatch> matches = ruleEngine.process(attributes);
    return matches;
  }
}
