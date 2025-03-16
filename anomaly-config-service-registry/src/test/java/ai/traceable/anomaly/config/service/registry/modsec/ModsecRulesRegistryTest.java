package ai.traceable.anomaly.config.service.registry.modsec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.anomaly.config.service.registry.common.ConfigConverter;
import ai.traceable.anomaly.config.service.v1.AnomalyEventDetails;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleType;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import ai.traceable.modsecurity.utils.ModsecRuleUtils;
import com.google.common.io.Resources;
import com.google.re2j.Matcher;
import com.google.re2j.Pattern;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

public class ModsecRulesRegistryTest {
  private final ModsecCrsRulesHandler modsecCrsRulesHandler =
      new ModsecCrsRulesHandler(new ModsecRuleUtils());
  private final ModsecRulesRegistryImpl modsecRulesRegistry =
      new ModsecRulesRegistryImpl(new ConfigConverter(), modsecCrsRulesHandler);

  @Test
  public void testModsecCrsRules() throws IOException {
    for (ModsecRuleVersion version : ModsecRuleVersion.values()) {
      testModsecCrsRules(version);
    }
  }

  private void testModsecCrsRules(ModsecRuleVersion version) throws IOException {
    Collection<AnomalyRuleInfo> anomalyRules =
        modsecRulesRegistry.getModsecRuleInfos(version).values();
    anomalyRules.forEach(
        rule -> {
          assertFalse(rule.getEventDetails().getDescription().isBlank());
          rule.getSubRuleInfosList()
              .forEach(
                  subRule -> {
                    assertFalse(subRule.getEventDetails().getDescription().isBlank());
                    assertFalse(subRule.getEventLabelsMap().isEmpty());
                  });
        });

    String secRuleRemoveByIdKeyword = "SecRuleRemoveById";
    List<AnomalySubRuleInfo> subRules =
        anomalyRules.stream()
            .map(AnomalyRuleInfo::getSubRuleInfosList)
            .flatMap(List::stream)
            .collect(Collectors.toList());

    long regularRulesCount =
        getRulesCount(subRules, AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_REGULAR);
    long safeRulesCount = getRulesCount(subRules, AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE);
    long blockingRulesCount =
        getRulesCount(subRules, AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK);

    long allRulesCount = subRules.size();
    assertEquals(allRulesCount, regularRulesCount + safeRulesCount);
    assertEquals(safeRulesCount, blockingRulesCount);

    {
      String crsRulesBlob =
          modsecRulesRegistry.getModsecCrsRulesBlob(
              List.of(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_REGULAR), version, Set.of());
      if (regularRulesCount == 0) {
        assertTrue(crsRulesBlob.isEmpty(), "Regular rules empty for version " + version);
      } else {
        assertEquals(
            allRulesCount - regularRulesCount,
            crsRulesBlob.split(secRuleRemoveByIdKeyword).length - 1,
            "Regular rules count for version " + version);
      }
    }
    {
      String crsRulesBlob =
          modsecRulesRegistry.getModsecCrsRulesBlob(
              List.of(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE), version, Set.of());
      assertEquals(
          allRulesCount - safeRulesCount,
          crsRulesBlob.split(secRuleRemoveByIdKeyword).length - 1,
          "Non safe rules count for version " + version);
    }
    {
      String crsRulesBlob =
          modsecRulesRegistry.getModsecCrsRulesBlob(
              List.of(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK), version, Set.of());
      // few rules in file not marked safe
      assertEquals(
          allRulesCount - blockingRulesCount,
          crsRulesBlob.split(secRuleRemoveByIdKeyword).length - 1,
          "Block rules count for version " + version);
    }
    {
      // safe and regular rules are mutually exclusive sets
      List<AnomalySubRuleInfo> regularSubRules =
          subRules.stream()
              .filter(
                  subRule ->
                      subRule
                          .getSubRuleTypesList()
                          .contains(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_REGULAR))
              .collect(Collectors.toList());
      List<AnomalySubRuleInfo> safeSubRules =
          subRules.stream()
              .filter(
                  subRule ->
                      subRule
                          .getSubRuleTypesList()
                          .contains(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE))
              .collect(Collectors.toList());
      Set<AnomalySubRuleInfo> allRulesSet =
          Stream.concat(regularSubRules.stream(), safeSubRules.stream())
              .collect(Collectors.toSet());
      assertEquals(allRulesSet.size(), regularSubRules.size() + safeSubRules.size());
    }
  }

  @Test
  public void testRuleVersions() {
    {
      String crsRulesBlob =
          modsecRulesRegistry.getModsecCrsRulesBlob(
              List.of(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_REGULAR),
              ModsecRuleVersion.MODSEC_RULE_VERSION_V3,
              Set.of());
      assertEquals(-1, crsRulesBlob.indexOf("SecArgumentsLimit 1000"));
    }
    {
      String crsRulesBlob =
          modsecRulesRegistry.getModsecCrsRulesBlob(
              List.of(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_REGULAR),
              ModsecRuleVersion.MODSEC_RULE_VERSION_V3_SECARG_LIMITS,
              Set.of());
      assertNotEquals(-1, crsRulesBlob.indexOf("SecArgumentsLimit 1000"));
    }
  }

  @Test
  public void testRules() {
    Map<String, AnomalyRuleInfo> anomalyRuleInfos =
        modsecRulesRegistry.getModsecRuleInfos(ModsecRuleVersion.MODSEC_RULE_VERSION_UNSPECIFIED);
    assertEquals(15, anomalyRuleInfos.size());
    assertEquals(
        "crs_101 :: Server Side Request Forgery (SSRF) Signatures\n"
            + "crs_102 :: XML External Entity Injection (XXE)\n"
            + "crs_103 :: Basic Authentication Violation\n"
            + "crs_104 :: GraphQL Attacks\n"
            + "crs_913 :: Scanner Detection\n"
            + "crs_921 :: HTTP Protocol Attacks\n"
            + "crs_930 :: Local File Inclusion\n"
            + "crs_931 :: Remote File Inclusion\n"
            + "crs_932 :: Remote Code Execution\n"
            + "crs_933 :: PHP Attacks\n"
            + "crs_934 :: NodeJS Injection\n"
            + "crs_941 :: Cross Site Scripting (XSS)\n"
            + "crs_942 :: SQL Injection\n"
            + "crs_943 :: Session Fixation\n"
            + "crs_944 :: Java Application Attacks",
        anomalyRuleInfos.values().stream()
            .map(
                anomalyRuleInfo ->
                    anomalyRuleInfo.getRuleId() + " :: " + anomalyRuleInfo.getRuleName())
            .sorted()
            .collect(Collectors.joining("\n")));
    anomalyRuleInfos
        .values()
        .forEach(
            anomalyRuleInfo -> {
              assertTrue(anomalyRuleInfo.getRuleId().startsWith("crs_"));
              if (anomalyRuleInfo.getRuleId().equals("crs_931")) {
                verifyEventDetails(anomalyRuleInfo.getEventDetails());
              }
              if (anomalyRuleInfo.getRuleId().equals("crs_944")) {
                assertEquals(14, anomalyRuleInfo.getSubRuleInfosCount());
                anomalyRuleInfo
                    .getSubRuleInfosList()
                    .forEach(
                        subRuleInfo -> {
                          if (Set.of("crs_944120", "crs_9440210")
                              .contains(subRuleInfo.getRuleId())) {
                            verifyEventDetails(subRuleInfo.getEventDetails());
                          }
                        });
              }
            });
  }

  private void verifyEventDetails(AnomalyEventDetails eventDetails) {
    assertFalse(eventDetails.getDescription().isBlank());
    assertFalse(eventDetails.getMitigation().isBlank());
    assertFalse(eventDetails.getImpact().isBlank());
    assertFalse(eventDetails.getReferences().isBlank());
  }

  @Test
  public void testSubRules() {
    Map<String, AnomalyRuleInfo> anomalyRuleInfos =
        modsecRulesRegistry.getModsecRuleInfos(ModsecRuleVersion.MODSEC_RULE_VERSION_CORAZA_V3);

    Map<String, AnomalyRuleInfo> testAnomalyRuleInfos =
        modsecRulesRegistry.getModsecRuleInfos(
            ModsecRuleVersion.MODSEC_RULE_VERSION_TEST_CORAZA_V3);

    // check correctness of sub-rules..
    anomalyRuleInfos.forEach(
        (ruleId, anomalyRuleInfo) ->
            anomalyRuleInfo
                .getSubRuleInfosList()
                .forEach(
                    anomalySubRuleInfo ->
                        assertTrue(anomalySubRuleInfo.getRuleId().startsWith(ruleId))));

    // check count of sub-rules..
    Set<String> expectedIdMatches =
        getIdMatches(loadModsecFileContents("modsec/crs/modsec-crs-rules.conf"));

    Set<String> idMatches =
        anomalyRuleInfos.values().stream()
            .flatMap(rule -> rule.getSubRuleInfosList().stream())
            .map(AnomalySubRuleInfo::getRuleId)
            .map(ruleId -> ruleId.substring(4))
            .collect(Collectors.toSet());

    // Diff between expected and actual rule ids
    Set<String> diff = new HashSet<>(expectedIdMatches);
    diff.removeAll(idMatches);
    assertEquals(0, diff.size(), "Missing rule ids: " + diff);

    assertEquals(expectedIdMatches.size(), idMatches.size());
    idMatches.forEach(id -> assertTrue(expectedIdMatches.contains(id)));

    // check for no duplicate names across both files. Normal and Test.
    Set<String> subRuleNames = new HashSet<>();
    anomalyRuleInfos.values().stream()
        .flatMap(rule -> rule.getSubRuleInfosList().stream())
        .forEach(
            anomalySubRuleInfo -> {
              assertFalse(
                  subRuleNames.contains(anomalySubRuleInfo.getRuleName()),
                  "Duplicate rule message detected - Rule ID: " + anomalySubRuleInfo.getRuleId());
              subRuleNames.add(anomalySubRuleInfo.getRuleName());
            });

    Set<String> testSubRuleNames = new HashSet<>();
    testAnomalyRuleInfos.values().stream()
        .flatMap(rule -> rule.getSubRuleInfosList().stream())
        .forEach(
            anomalySubRuleInfo -> {
              assertFalse(
                  testSubRuleNames.contains(anomalySubRuleInfo.getRuleName()),
                  "Duplicate Rule Name in Test File at "
                      + anomalySubRuleInfo.getRuleId().toString());
              testSubRuleNames.add(anomalySubRuleInfo.getRuleName());
            });

    // check labels
    List<String> subRulesRead =
        Arrays.asList(loadModsecFileContents("modsec/modsec-all-rules.txt").split("\\n"));
    Collections.sort(subRulesRead);

    List<String> subRulesCollected =
        anomalyRuleInfos.values().stream()
            .flatMap(rule -> rule.getSubRuleInfosList().stream())
            .map(
                anomalySubRuleInfo -> {
                  StringBuilder sb =
                      new StringBuilder(
                          anomalySubRuleInfo.getRuleId()
                              + " :: "
                              + anomalySubRuleInfo.getRuleName()
                              + " :: "
                              + anomalySubRuleInfo.getSeverityLevel());
                  String labels =
                      anomalySubRuleInfo.getEventLabelsMap().entrySet().stream()
                          .map(entry -> String.format("'%s:%s'", entry.getKey(), entry.getValue()))
                          .collect(Collectors.joining(" , "));
                  if (!labels.isBlank()) {
                    sb.append(" :: [ ");
                    sb.append(labels);
                    sb.append(" ]");
                  }
                  return sb.toString();
                })
            .sorted()
            .collect(Collectors.toList());

    // assertEquals(subRulesRead, subRulesCollected);
    for (int i = 0; i < subRulesRead.size(); i++) {
      assertEquals(subRulesRead.get(i), subRulesCollected.get(i));
    }
    assertEquals(subRulesRead.size(), subRulesCollected.size());

    // check regular rules
    subRulesRead =
        Arrays.asList(loadModsecFileContents("modsec/modsec-regular-rules.txt").split("\\n"));
    Collections.sort(subRulesRead);
    subRulesCollected =
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
      assertEquals(
          subRulesRead.get(i), subRulesCollected.get(i), "Regular rule mismatch at index " + i);
    }
    assertEquals(subRulesRead.size(), subRulesCollected.size(), "Regular rule count mismatch");

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
      assertEquals(
          subRulesRead.get(i), subRulesCollected.get(i), "Safe rule mismatch at index " + i);
    }
    assertEquals(subRulesRead.size(), subRulesCollected.size(), "Safe rule count mismatch");

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
      assertEquals(
          subRulesRead.get(i), subRulesCollected.get(i), "Blocking rule mismatch at index " + i);
    }
    assertEquals(subRulesRead.size(), subRulesCollected.size(), "Blocking rule count mismatch");

    subRulesCollected =
        testAnomalyRuleInfos.values().stream()
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
      assertEquals(
          subRulesRead.get(i),
          subRulesCollected.get(i),
          "Blocking rule mismatch at index " + i + " in Test File");
    }
    assertEquals(
        subRulesRead.size(), subRulesCollected.size(), "Blocking rule count mismatch in Test File");
  }

  @Test
  public void testSubRuleInfoConsistencyAcrossVersions() {
    Map<String, AnomalySubRuleInfo> anomalySubRuleInfos = new HashMap<>();
    for (ModsecRuleVersion version : ModsecRuleVersion.values()) {
      if (version.equals(ModsecRuleVersion.MODSEC_RULE_VERSION_UNSPECIFIED)
          || version.equals(ModsecRuleVersion.UNRECOGNIZED)) {
        continue;
      }
      Collection<AnomalyRuleInfo> rules = modsecRulesRegistry.getModsecRuleInfos(version).values();
      assertFalse(rules.isEmpty());
      for (AnomalyRuleInfo ruleInfo : rules) {
        for (AnomalySubRuleInfo subRuleInfo : ruleInfo.getSubRuleInfosList()) {
          if (anomalySubRuleInfos.containsKey(subRuleInfo.getRuleId())) {
            assertEquals(anomalySubRuleInfos.get(subRuleInfo.getRuleId()), subRuleInfo);
          }
          anomalySubRuleInfos.put(subRuleInfo.getRuleId(), subRuleInfo);
        }
      }
    }

    Map<String, AnomalySubRuleInfo> allMergedSubRules =
        modsecRulesRegistry
            .getModsecRuleInfos(ModsecRuleVersion.MODSEC_RULE_VERSION_UNSPECIFIED)
            .values()
            .stream()
            .map(AnomalyRuleInfo::getSubRuleInfosList)
            .flatMap(List::stream)
            .collect(Collectors.toMap(AnomalySubRuleInfo::getRuleId, Function.identity()));
    assertEquals(anomalySubRuleInfos, allMergedSubRules);
  }

  @Test
  public void testModsecCrsSensitiveAgentRules() {
    String[] modsecCrsAllRules =
        loadModsecFileContents("modsec/crs/modsec-crs-rules.conf").split("\n\n");
    String[] modsecCrsSensitiveAgentRules =
        loadModsecFileContents("modsec/crs/modsec-crs-sensitive-agent-rules.conf").split("\n\n");

    int j = 0;
    for (int i = 0; i < modsecCrsAllRules.length; i++) {
      if (modsecCrsAllRules[i].startsWith("SecRuleUpdateTargetById")) {
        if (j < modsecCrsSensitiveAgentRules.length
            && modsecCrsSensitiveAgentRules[j].startsWith("SecRuleUpdateTargetById")) {
          j++;
        }
        continue;
      } else if (!modsecCrsAllRules[i].startsWith("SecRule")) {
        assertEquals(modsecCrsAllRules[i], modsecCrsSensitiveAgentRules[j++]);
      } else if (!modsecCrsAllRules[i].contains("tag:'traceable/type/regular'")) {
        String sanitizedString =
            modsecCrsAllRules[i].replace(
                "found within %{MATCHED_VAR_NAME}: %{MATCHED_VAR}",
                "found within %{MATCHED_VAR_NAME}");
        assertEquals(sanitizedString, modsecCrsSensitiveAgentRules[j++]);
      }
    }
    assertEquals(modsecCrsSensitiveAgentRules.length, j);
  }

  @Test
  public void testModsecTestRules() {
    // Check if the number of safe rules in test file match with the original file.
    List<String> testSubRules =
        modsecRulesRegistry
            .getModsecRuleInfos(ModsecRuleVersion.MODSEC_RULE_VERSION_TEST_CORAZA_V3)
            .values()
            .stream()
            .map(
                anomalySubRuleInfo ->
                    anomalySubRuleInfo.getRuleId() + " :: " + anomalySubRuleInfo.getRuleName())
            .sorted()
            .collect(Collectors.toList());

    List<String> subRules =
        modsecRulesRegistry
            .getModsecRuleInfos(ModsecRuleVersion.MODSEC_RULE_VERSION_CORAZA_V3)
            .values()
            .stream()
            .map(
                anomalySubRuleInfo ->
                    anomalySubRuleInfo.getRuleId() + " :: " + anomalySubRuleInfo.getRuleName())
            .sorted()
            .collect(Collectors.toList());

    assertEquals(testSubRules, subRules);
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

  private long getRulesCount(
      List<AnomalySubRuleInfo> subRules, AnomalySubRuleType anomalySubRuleType) {
    return subRules.stream()
        .filter(subRule -> subRule.getSubRuleTypesList().contains(anomalySubRuleType))
        .count();
  }
}
