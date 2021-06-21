package ai.traceable.anomaly.config.service.registry.modsec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import ai.traceable.anomaly.config.service.registry.common.ConfigConverter;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleInfo;
import ai.traceable.modsecurity.RuleEngine;
import com.google.re2j.Matcher;
import com.google.re2j.Pattern;
import java.io.IOException;
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
      assertNotNull(RuleEngine.create(modsecRulesRegistry.getModsecSafeCrsRulesBlob()));
      assertNotNull(RuleEngine.create(modsecRulesRegistry.getModsecRegularCrsRulesBlob()));
    }
  }

  @Test
  public void testRules() {
    Map<String, AnomalyRuleInfo> anomalyRuleInfos = modsecRulesRegistry.getModsecRuleInfos();
    assertEquals(10, anomalyRuleInfos.size());
    assertEquals(
        "crs_913 :: Scanner Detection\n"
            + "crs_921 :: HTTP Protocol Attacks\n"
            + "crs_930 :: Local File Inclusion\n"
            + "crs_931 :: Remote File Inclusion\n"
            + "crs_932 :: Remote Code Execution\n"
            + "crs_934 :: NodeJS Injection\n"
            + "crs_941 :: Cross Site Scripting\n"
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
    Set<String> idMatches = new HashSet<>();
    idMatches.addAll(
        getIdMatches(
            modsecCrsRulesHandler.loadModsecCrsFileContents("modsec/crs/modsec-safe-rules.conf")));
    idMatches.addAll(
        getIdMatches(
            modsecCrsRulesHandler.loadModsecCrsFileContents(
                "modsec/crs/modsec-regular-rules.conf")));
    int countSubRules =
        anomalyRuleInfos.values().stream()
            .map(AnomalyRuleInfo::getSubRuleInfosCount)
            .reduce(0, (a, b) -> a + b);
    assertEquals(idMatches.size(), countSubRules);
    assertEquals(28, countSubRules);

    // check for no duplicate names..
    List<String> subRuleNames =
        anomalyRuleInfos.values().stream()
            .flatMap(rule -> rule.getSubRuleInfosList().stream())
            .map(AnomalySubRuleInfo::getRuleName)
            .collect(Collectors.toList());
    assertEquals(new HashSet<>(subRuleNames).size(), subRuleNames.size());

    assertEquals(
        "crs_913100 :: User-Agent associated with security scanner :: blocking=true\n"
            + "crs_913110 :: Request header associated with security scanner :: blocking=true\n"
            + "crs_913120 :: Request filename/argument associated with security scanner :: blocking=true\n"
            + "crs_921110 :: HTTP Request Smuggling Attack :: blocking=true\n"
            + "crs_921140 :: HTTP Header Injection Attack via headers :: blocking=true\n"
            + "crs_930100 :: Path Traversal Attack (/../) (100) :: blocking=true\n"
            + "crs_930110 :: Path Traversal Attack (/../) (110) :: blocking=true\n"
            + "crs_930120 :: OS File Access Attempt :: blocking=true\n"
            + "crs_930130 :: Restricted File Access Attempt :: blocking=true\n"
            + "crs_931120 :: Possible Remote File Inclusion (RFI) Attack: URL Payload Used w/Trailing Question Mark Character (?) :: blocking=true\n"
            + "crs_932160 :: Remote Command Execution: Unix Shell Code Found :: blocking=true\n"
            + "crs_932170 :: Remote Command Execution: Shellshock (CVE-2014-6271) (170) :: blocking=true\n"
            + "crs_932171 :: Remote Command Execution: Shellshock (CVE-2014-6271) (171) :: blocking=true\n"
            + "crs_934100 :: Node.js Injection Attack :: blocking=true\n"
            + "crs_941110 :: XSS Filter - Category 1: Script Tag Vector :: blocking=true\n"
            + "crs_941200 :: IE XSS Filters - Attack using VML frames :: blocking=true\n"
            + "crs_941210 :: IE XSS Filters - Attack using obfuscated Javascript :: blocking=true\n"
            + "crs_941220 :: IE XSS Filters - using obfuscated VB Script :: blocking=true\n"
            + "crs_941230 :: IE XSS Filters - using \\'embed\\' tag :: blocking=true\n"
            + "crs_941240 :: IE XSS Filters - Attack using \\'import\\' or \\'implementation\\' attribute :: blocking=true\n"
            + "crs_941270 :: IE XSS Filters - Attack using \\'link\\' href :: blocking=true\n"
            + "crs_941280 :: IE XSS Filters - Attack using \\'base\\' tag :: blocking=true\n"
            + "crs_941290 :: IE XSS Filters - Attack using \\'applet\\' tag :: blocking=true\n"
            + "crs_941300 :: IE XSS Filters - Attack using \\'object\\' tag :: blocking=true\n"
            + "crs_941350 :: UTF-7 Encoding IE XSS - Attack Detected :: blocking=true\n"
            + "crs_942290 :: Basic MongoDB SQL injection attempts :: blocking=true\n"
            + "crs_943100 :: Possible Session Fixation Attack: Setting Cookie Values in HTML :: blocking=true\n"
            + "crs_944120 :: Possible payload execution and remote command execution: Java serialization (CVE-2015-5842) :: blocking=true",
        anomalyRuleInfos.values().stream()
            .flatMap(rule -> rule.getSubRuleInfosList().stream())
            .map(
                anomalySubRuleInfo ->
                    anomalySubRuleInfo.getRuleId()
                        + " :: "
                        + anomalySubRuleInfo.getRuleName()
                        + " :: blocking="
                        + anomalySubRuleInfo.getBlockingAvailable())
            .sorted()
            .collect(Collectors.joining("\n")));
  }

  private Set<String> getIdMatches(String text) {
    Set<String> idMatches = new HashSet<>();
    Matcher matcher = Pattern.compile("id:([0-9]+),").matcher(text);
    while (matcher.find()) {
      idMatches.add(matcher.group(1));
    }
    return idMatches;
  }
}
