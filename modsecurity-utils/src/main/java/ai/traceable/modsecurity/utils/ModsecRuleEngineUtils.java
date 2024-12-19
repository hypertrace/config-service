package ai.traceable.modsecurity.utils;

import ai.traceable.modsecurity.Attribute;
import ai.traceable.modsecurity.RuleEngine;
import ai.traceable.modsecurity.RuleMatch;
import ai.traceable.platform.coraza.waf.service.CorazaWafServiceClient;
import io.grpc.Status;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ModsecRuleEngineUtils {
  private static final boolean loadNativeLibrarySuccess = loadNativeRuleEngineLibrary();

  private static final CorazaWafServiceClient client =
      new CorazaWafServiceClient("localhost", 9000, Duration.ofSeconds(5));

  private static boolean loadNativeRuleEngineLibrary() {
    try {
      RuleEngine.loadNativeLibrary();
      return true;
    } catch (IOException e) {
      log.warn("Failed loading rule engine native library with error:", e);
      return false;
    }
  }

  public static Status corazaValidate(String modsecRuleBlob) {
    String wafID = "testWaf";
    try {
      client.intWaf(wafID, modsecRuleBlob, true);
      return Status.OK;
    } catch (Exception e) {
      return Status.INVALID_ARGUMENT
          .withCause(e)
          .withDescription(
              String.format("Exception while validating Rule - %s with Coraza", modsecRuleBlob));
    }
  }

  public static Status modsecValidate(String modsecRuleBlob) {
    if (loadNativeLibrarySuccess == false) {
      log.warn("Skipping modsec SecRule validation.. Native libraries for rule engine not loaded!");
      return Status.UNKNOWN;
    }
    try {
      boolean isValid = RuleEngine.validateRule(modsecRuleBlob);
      if (isValid) {
        return Status.OK;
      } else {
        return Status.INVALID_ARGUMENT.withDescription(
            String.format("Validation failed for Modsec Rule:%s", modsecRuleBlob));
      }
    } catch (Exception e) {
      return Status.INVALID_ARGUMENT
          .withCause(e)
          .withDescription(
              String.format("Exception while validating Modsec Rule:%s", modsecRuleBlob));
    }
  }

  public static boolean validateRuleBlob(String modsecRuleBlob) {
    try {
      Status modsecStatus = modsecValidate(modsecRuleBlob);
      Status corazaStatus = corazaValidate(modsecRuleBlob);
      return (modsecStatus.isOk() || modsecStatus.equals(Status.UNKNOWN))
          && (corazaStatus.isOk() || corazaStatus.equals(Status.UNKNOWN));
    } catch (Exception e) {
      log.error("Exception while validating Modsec Rule Blob: {}", modsecRuleBlob, e);
      return false;
    }
  }

  public static List<RuleMatch> getModsecRuleMatches(
      String modsecRuleBlob, Map<String, String> attributesMap) {
    if (loadNativeLibrarySuccess == false) {
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
    if (loadNativeLibrarySuccess == false) {
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
