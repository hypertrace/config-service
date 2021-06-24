package ai.traceable.anomaly.config.service.registry.modsec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.anomaly.config.service.registry.common.ConfigConverter;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleInfo;
import ai.traceable.modsecurity.RuleEngine;
import com.google.common.io.Resources;
import com.google.re2j.Matcher;
import com.google.re2j.Pattern;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.SystemUtils;
import org.junit.jupiter.api.Test;

public class ModsecRulesRegistryTest {
  private ModsecCrsRulesHandler modsecCrsRulesHandler =
      new ModsecCrsRulesHandler(new ModsecRuleUtils());
  private ModsecRulesRegistryImpl modsecRulesRegistry =
      new ModsecRulesRegistryImpl(new ConfigConverter(), modsecCrsRulesHandler);

  @Test
  public void testModsecCrsRules() throws IOException {
    if (SystemUtils.IS_OS_LINUX) {
      RuleEngine.loadNativeLibrary();
      assertNotNull(RuleEngine.create(modsecRulesRegistry.getModsecSafeCrsRulesBlob()));
      assertNotNull(RuleEngine.create(modsecRulesRegistry.getModsecRegularCrsRulesBlob()));
    }
  }

  @Test
  public void testRules() {
    Map<String, AnomalyRuleInfo> anomalyRuleInfos = modsecRulesRegistry.getModsecRuleInfos();
    assertEquals(11, anomalyRuleInfos.size());
    assertEquals(
        "crs_912 :: Denial of Service (DOS) attack\n"
            + "crs_913 :: Scanner Detection\n"
            + "crs_921 :: HTTP Protocol Attacks\n"
            + "crs_930 :: Local File Inclusion\n"
            + "crs_931 :: Remote File Inclusion\n"
            + "crs_932 :: Remote Code Execution\n"
            + "crs_934 :: NodeJS Injection\n"
            + "crs_941 :: Cross Site Scripting (XSS)\n"
            + "crs_942 :: SQL Injection\n"
            + "crs_943 :: Session Fixation\n"
            + "crs_944 :: Java Apache Struts Attacks",
        anomalyRuleInfos.values().stream()
            .map(
                anomalyRuleInfo ->
                    anomalyRuleInfo.getRuleId() + " :: " + anomalyRuleInfo.getRuleName())
            .sorted()
            .collect(Collectors.joining("\n")));
    anomalyRuleInfos
        .values()
        .forEach(anomalyRuleInfo -> anomalyRuleInfo.getRuleId().startsWith("crs_"));
  }

  @Test
  public void testSubRules() {
    Map<String, AnomalyRuleInfo> anomalyRuleInfos = modsecRulesRegistry.getModsecRuleInfos();

    // check correctness of sub-rules..
    anomalyRuleInfos.forEach(
        (ruleId, anomalyRuleInfo) ->
            anomalyRuleInfo
                .getSubRuleInfosList()
                .forEach(anomalySubRuleInfo -> anomalyRuleInfo.getRuleId().startsWith(ruleId)));

    // check count of sub-rules..
    Set<String> expectedIdMatches = new HashSet<>();
    expectedIdMatches.addAll(
        getIdMatches(
            modsecCrsRulesHandler.loadModsecCrsFileContents("modsec/crs/modsec-safe-rules.conf")));
    expectedIdMatches.addAll(
        getIdMatches(
            modsecCrsRulesHandler.loadModsecCrsFileContents(
                "modsec/crs/modsec-regular-rules.conf")));

    Set<String> idMatches =
        anomalyRuleInfos.values().stream()
            .flatMap(rule -> rule.getSubRuleInfosList().stream())
            .map(AnomalySubRuleInfo::getRuleId)
            .map(ruleId -> ruleId.substring(4))
            .collect(Collectors.toSet());
    assertEquals(expectedIdMatches.size(), idMatches.size());
    idMatches.forEach(id -> assertTrue(expectedIdMatches.contains(id)));

    // check for no duplicate names..
    Set<String> subRuleNames = new HashSet<>();
    anomalyRuleInfos.values().stream()
        .flatMap(rule -> rule.getSubRuleInfosList().stream())
        .forEach(
            anomalySubRuleInfo -> {
              assertFalse(subRuleNames.contains(anomalySubRuleInfo.getRuleName()));
              subRuleNames.add(anomalySubRuleInfo.getRuleName());
            });

    // check safe-rules
    assertEquals(
        loadModsecFileContents("modsec/modsec-safe-rules.txt"),
        anomalyRuleInfos.values().stream()
            .flatMap(rule -> rule.getSubRuleInfosList().stream())
            .filter(AnomalySubRuleInfo::getBlockingAvailable)
            .map(
                anomalySubRuleInfo ->
                    anomalySubRuleInfo.getRuleId() + " :: " + anomalySubRuleInfo.getRuleName())
            .sorted()
            .collect(Collectors.joining("\n")));

    // check unsafe-rules
    assertEquals(
        loadModsecFileContents("modsec/modsec-unsafe-rules.txt"),
        anomalyRuleInfos.values().stream()
            .flatMap(rule -> rule.getSubRuleInfosList().stream())
            .filter(anomalySubRuleInfo -> !anomalySubRuleInfo.getBlockingAvailable())
            .map(
                anomalySubRuleInfo ->
                    anomalySubRuleInfo.getRuleId() + " :: " + anomalySubRuleInfo.getRuleName())
            .sorted()
            .collect(Collectors.joining("\n")));
  }

  private Set<String> getIdMatches(String text) {
    Set<String> idMatches = new HashSet<>();
    Arrays.asList(text.split("SecRule"))
        .forEach(
            phrase -> {
              Matcher idMatcher = Pattern.compile("id:([0-9]+)").matcher(phrase);
              Matcher msgMatcher = Pattern.compile("msg:'(.*)'").matcher(phrase);
              while (idMatcher.find() && msgMatcher.find()) {
                idMatches.add(idMatcher.group(1));
              }
            });
    return idMatches;
  }

  private String loadModsecFileContents(String crsFilePath) {
    URL resourceUrl = getClass().getClassLoader().getResource(crsFilePath);
    if (resourceUrl == null) {
      throw new RuntimeException(
          String.format("Unable to locate modsec crs file: %s", crsFilePath));
    } else {
      try {
        return Resources.toString(resourceUrl, StandardCharsets.UTF_8);
      } catch (Exception e) {
        throw new RuntimeException(
            String.format("Unable to read modsec crs file: %s", resourceUrl), e);
      }
    }
  }
}
