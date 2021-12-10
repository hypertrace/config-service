package ai.traceable.anomaly.config.service.registry.modsec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.anomaly.config.service.registry.common.ConfigConverter;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleType;
import ai.traceable.modsecurity.RuleEngine;
import com.google.common.io.Resources;
import com.google.re2j.Matcher;
import com.google.re2j.Pattern;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
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
      assertNotNull(
          RuleEngine.create(
              modsecRulesRegistry.getModsecCrsRulesBlob(
                  AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_REGULAR)));
      assertNotNull(
          RuleEngine.create(
              modsecRulesRegistry.getModsecCrsRulesBlob(
                  AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE)));
      assertNotNull(
          RuleEngine.create(
              modsecRulesRegistry.getModsecCrsRulesBlob(
                  AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK)));
    }

    String secRuleRemoveByIdKeyword = "SecRuleRemoveById";
    List<AnomalySubRuleInfo> subRules =
        modsecRulesRegistry.getModsecRuleInfos().values().stream()
            .map(AnomalyRuleInfo::getSubRuleInfosList)
            .flatMap(List::stream)
            .collect(Collectors.toList());
    long regularRulesCount =
        subRules.stream()
            .filter(
                subRule ->
                    subRule
                        .getSubRuleTypesList()
                        .contains(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_REGULAR))
            .collect(Collectors.counting());
    {
      String crsRulesBlob =
          modsecRulesRegistry.getModsecCrsRulesBlob(
              AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_REGULAR);
      // all rules in file used
      assertEquals(1, crsRulesBlob.split(secRuleRemoveByIdKeyword).length);
    }
    {
      String crsRulesBlob =
          modsecRulesRegistry.getModsecCrsRulesBlob(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE);
      long safeRulesCount =
          subRules.stream()
              .filter(
                  subRule ->
                      subRule
                          .getSubRuleTypesList()
                          .contains(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE))
              .collect(Collectors.counting());
      // few rules in file not marked safe
      assertEquals(
          regularRulesCount - safeRulesCount,
          crsRulesBlob.split(secRuleRemoveByIdKeyword).length - 1);
      assertEquals(34, crsRulesBlob.split(secRuleRemoveByIdKeyword).length);
    }
    {
      String crsRulesBlob =
          modsecRulesRegistry.getModsecCrsRulesBlob(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK);
      long blockingRulesCount =
          subRules.stream()
              .filter(
                  subRule ->
                      subRule
                          .getSubRuleTypesList()
                          .contains(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK))
              .collect(Collectors.counting());
      // few rules in file not marked safe
      assertEquals(
          regularRulesCount - blockingRulesCount,
          crsRulesBlob.split(secRuleRemoveByIdKeyword).length - 1);
      // few rules in file not marked for blocking
      assertEquals(61, crsRulesBlob.split(secRuleRemoveByIdKeyword).length);
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
    Set<String> expectedIdMatches =
        getIdMatches(loadModsecFileContents("modsec/crs/modsec-crs-rules.conf"));

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

    // check regular rules
    List<String> subRulesRead =
        Arrays.asList(loadModsecFileContents("modsec/modsec-regular-rules.txt").split("\\n"));
    Collections.sort(subRulesRead);
    List<String> subRulesCollected =
        anomalyRuleInfos.values().stream()
            .flatMap(rule -> rule.getSubRuleInfosList().stream())
            .filter(
                subRule ->
                    subRule
                        .getSubRuleTypesList()
                        .contains(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_REGULAR))
            .map(
                anomalySubRuleInfo ->
                    anomalySubRuleInfo.getRuleId() + " :: " + anomalySubRuleInfo.getRuleName())
            .sorted()
            .collect(Collectors.toList());
    // assertEquals(subRulesRead, subRulesCollected);
    for (int i = 0; i < subRulesRead.size(); i++) {
      assertEquals(subRulesRead.get(i), subRulesCollected.get(i));
    }
    assertEquals(subRulesRead.size(), subRulesCollected.size());

    // check safe rules
    subRulesRead =
        Arrays.asList(loadModsecFileContents("modsec/modsec-safe-rules.txt").split("\\n"));
    Collections.sort(subRulesRead);
    subRulesCollected =
        anomalyRuleInfos.values().stream()
            .flatMap(rule -> rule.getSubRuleInfosList().stream())
            .filter(
                subRule ->
                    subRule
                        .getSubRuleTypesList()
                        .contains(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE))
            .map(
                anomalySubRuleInfo ->
                    anomalySubRuleInfo.getRuleId() + " :: " + anomalySubRuleInfo.getRuleName())
            .sorted()
            .collect(Collectors.toList());
    for (int i = 0; i < subRulesRead.size(); i++) {
      assertEquals(subRulesRead.get(i), subRulesCollected.get(i));
    }
    assertEquals(subRulesRead.size(), subRulesCollected.size());

    // check blocking rules
    subRulesRead =
        Arrays.asList(loadModsecFileContents("modsec/modsec-blocking-rules.txt").split("\\n"));
    Collections.sort(subRulesRead);
    subRulesCollected =
        anomalyRuleInfos.values().stream()
            .flatMap(rule -> rule.getSubRuleInfosList().stream())
            .filter(
                subRule ->
                    subRule
                        .getSubRuleTypesList()
                        .contains(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK))
            .map(
                anomalySubRuleInfo ->
                    anomalySubRuleInfo.getRuleId() + " :: " + anomalySubRuleInfo.getRuleName())
            .sorted()
            .collect(Collectors.toList());
    // assertEquals(subRulesRead, subRulesCollected);
    for (int i = 0; i < subRulesRead.size(); i++) {
      assertEquals(subRulesRead.get(i), subRulesCollected.get(i));
    }
    assertEquals(subRulesRead.size(), subRulesCollected.size());
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
