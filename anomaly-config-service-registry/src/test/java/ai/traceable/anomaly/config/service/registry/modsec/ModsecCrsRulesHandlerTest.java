package ai.traceable.anomaly.config.service.registry.modsec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import ai.traceable.anomaly.config.service.utils.modsec.ModsecRuleUtils;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleType;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ModsecCrsRulesHandlerTest {

  @Test
  public void testParseModsecCrsRules() {
    ModsecCrsRulesHandler modsecCrsRulesHandler = new ModsecCrsRulesHandler(new ModsecRuleUtils());
    String sampleRules =
        "# REQUEST-913-SCANNER-DETECTION.conf\n"
            + "SecRule REQUEST_HEADERS:User-Agent \"@pm (hydra) .nasl absinthe arachni/ autogetcontent bilbo\" \\\n"
            + "    \"id:913100,\\\n"
            + "    phase:2,\\\n"
            + "    block,\\\n"
            + "    capture,\\\n"
            + "    t:none,t:lowercase,\\\n"
            + "    msg:'User-Agent associated with security scanner',\\\n"
            + "    tag:'traceable/type/regular,safe,block',\\\n"
            + "    logdata:'Matched Data: %{TX.0} found within %{MATCHED_VAR_NAME}: %{MATCHED_VAR}',\\\n"
            + "    tag:'paranoia-level/1',\\\n"
            + "    severity:'CRITICAL'\"\n"
            + "SecRule REQUEST_HEADERS_NAMES|REQUEST_HEADERS \"@pm acunetix-product acunetix-scanning-agreement \" \\\n"
            + "    \"id:913110,\\\n"
            + "    phase:2,\\\n"
            + "    capture,\\\n"
            + "    t:none,t:lowercase,\\\n"
            + "    msg:'Request header associated with security scanner',\\\n"
            + "    tag:'traceable/type/regular,safe,block',\\\n"
            + "    logdata:'Matched Data: %{TX.0} found within %{MATCHED_VAR_NAME}: %{MATCHED_VAR}',\\\n"
            + "    tag:'paranoia-level/1',\\\n"
            + "    severity:'CRITICAL'\"\n"
            + "SecRule REQUEST_HEADERS_NAMES|REQUEST_HEADERS \"@pm acunetix-product \" \\\n"
            + "    \"id:913110,\\\n"
            + "    phase:2,\\\n"
            + "    capture,\\\n"
            + "    t:none,t:lowercase,\\\n"
            + "    logdata:'Matched Data: %{TX.0} found within %{MATCHED_VAR_NAME}: %{MATCHED_VAR}',\\\n"
            + "    tag:'paranoia-level/1',\\\n"
            + "    severity:'CRITICAL'\"\n";

    Map<String, List<AnomalySubRuleInfo>> modsecRulesMap =
        modsecCrsRulesHandler.parseModsecCrsRules(sampleRules);
    assertEquals(1, modsecRulesMap.size());
    assertTrue(modsecRulesMap.containsKey("crs_913"));
    assertEquals(2, modsecRulesMap.get("crs_913").size());

    modsecRulesMap
        .get("crs_913")
        .forEach(
            subRule -> {
              if (subRule.getRuleId().equals("crs_913100")) {
                assertEquals("User-Agent associated with security scanner", subRule.getRuleName());
              } else if (subRule.getRuleId().equals("crs_913110")) {
                assertEquals(
                    "Request header associated with security scanner", subRule.getRuleName());
              } else {
                fail();
              }
            });
  }

  @Test
  public void testFilterMatchedModsecRules() {
    ModsecCrsRulesHandler modsecCrsRulesHandler = new ModsecCrsRulesHandler(new ModsecRuleUtils());
    AnomalySubRuleInfo anomalySubRuleInfo =
        AnomalySubRuleInfo.newBuilder()
            .addAllSubRuleTypes(List.of(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_REGULAR))
            .build();
    assertTrue(
        modsecCrsRulesHandler.matchModsecRule(
            anomalySubRuleInfo, AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_UNSAFE));

    anomalySubRuleInfo =
        AnomalySubRuleInfo.newBuilder()
            .addAllSubRuleTypes(
                List.of(
                    AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_REGULAR,
                    AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE))
            .build();
    assertFalse(
        modsecCrsRulesHandler.matchModsecRule(
            anomalySubRuleInfo, AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_UNSAFE));
  }

  @Test
  public void testParseModsecCrsRulesException() {
    ModsecCrsRulesHandler modsecCrsRulesHandler = new ModsecCrsRulesHandler(new ModsecRuleUtils());

    assertThrows(
        RuntimeException.class,
        () ->
            modsecCrsRulesHandler.parseModsecCrsRules(
                "# REQUEST-913-SCANNER-DETECTION.conf\n"
                    + "SecRule REQUEST_HEADERS:User-Agent \"@pm (hydra) .nasl \" \\\n"
                    + "    \"id:913100,\\\n"
                    + "    phase:2,\\\n"
                    + "    msg:''\"\n"));

    // no id-match
    assertEquals(
        0,
        modsecCrsRulesHandler
            .parseModsecCrsRules(
                "# REQUEST-913-SCANNER-DETECTION.conf\n"
                    + "SecRule REQUEST_HEADERS:User-Agent \"@pm (hydra) .nasl \" \\\n"
                    + "    \"phase:2\"\n")
            .size());

    // invalid id
    assertEquals(
        0,
        modsecCrsRulesHandler
            .parseModsecCrsRules(
                "# REQUEST-913-SCANNER-DETECTION.conf\n"
                    + "SecRule REQUEST_HEADERS:User-Agent \"@pm (hydra) .nasl \" \\\n"
                    + "    \"id:xyz,\\\n"
                    + "    phase:2\"\n")
            .size());
  }
}
