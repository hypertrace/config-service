package ai.traceable.anomaly.config.service.registry.modsec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.anomaly.config.service.v1.AnomalySubRuleInfo;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ModsecCrsRulesHandlerTest {

  @Test
  public void testParseModsecCrsRules() {
    ModsecCrsRulesHandler modsecCrsRulesHandler = new ModsecCrsRulesHandler(new ModsecRuleUtils());
    String sampleRules =
        "# REQUEST-913-SCANNER-DETECTION.conf\n"
            + "SecRule REQUEST_HEADERS:User-Agent \"@pm (hydra) .nasl absinthe arachni/ autogetcontent bilbo BFAC brutus brutus/aet bsqlbf cgichk cisco-torch commix core-project/1.0 crimscanner/ datacha0s dirbuster domino hunter dotdotpwn floodgate get-minimal gobuster grabber grendel-scan havij inspath jaascois zmeu Jorgee masscan metis mysqloit n-stealth nessus netsparker nikto nmap-nse nsauditor openvas pangolin paros pmafind prog.customcrawler s.t.a.l.k.e.r. springenwerk sqlmap sqlninja sysscan uil2pn user-agent: vega/ voideye w3af.sf.net w3af.sourceforge.net w3af.org webbandit webinspect webshag webtrends webvulnscan whatweb whcc/ wordpress xmlrpc exploit WPScan struts-pwn Detectify zgrab\" \\\n"
            + "    \"id:913100,\\\n"
            + "    phase:2,\\\n"
            + "    block,\\\n"
            + "    capture,\\\n"
            + "    t:none,t:lowercase,\\\n"
            + "    msg:'User-Agent associated with security scanner',\\\n"
            + "    logdata:'Matched Data: %{TX.0} found within %{MATCHED_VAR_NAME}: %{MATCHED_VAR}',\\\n"
            + "    tag:'paranoia-level/1',\\\n"
            + "    severity:'CRITICAL'\"\n"
            + "SecRule REQUEST_HEADERS_NAMES|REQUEST_HEADERS \"@pm acunetix-product acunetix-scanning-agreement acunetix-user-agreement myvar=1234 x-ratproxy-loop bytes=0-,5-0,5-1,5-2,5-3,5-4,5-5,5-6,5-7,5-8,5-9,5-10,5-11,5-12,5-13,5-14 x-scanner\" \\\n"
            + "    \"id:913110,\\\n"
            + "    phase:2,\\\n"
            + "    block,\\\n"
            + "    capture,\\\n"
            + "    t:none,t:lowercase,\\\n"
            + "    msg:'Request header associated with security scanner',\\\n"
            + "    logdata:'Matched Data: %{TX.0} found within %{MATCHED_VAR_NAME}: %{MATCHED_VAR}',\\\n"
            + "    tag:'paranoia-level/1',\\\n"
            + "    severity:'CRITICAL'\"\n";

    Map<String, List<AnomalySubRuleInfo>> modsecRulesMap =
        modsecCrsRulesHandler.parseModsecCrsRules(sampleRules);
    assertEquals(1, modsecRulesMap.size());
    assertTrue(modsecRulesMap.keySet().contains("crs_913"));
    assertEquals(2, modsecRulesMap.get("crs_913").size());

    AnomalySubRuleInfo anomalySubRuleInfo = modsecRulesMap.get("crs_913").get(0);
    assertEquals("crs_913100", anomalySubRuleInfo.getRuleId());
    assertEquals("User-Agent associated with security scanner", anomalySubRuleInfo.getRuleName());

    anomalySubRuleInfo = modsecRulesMap.get("crs_913").get(1);
    assertEquals("crs_913110", anomalySubRuleInfo.getRuleId());
    assertEquals(
        "Request header associated with security scanner", anomalySubRuleInfo.getRuleName());
  }
}
