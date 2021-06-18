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
      assertNotNull(RuleEngine.create(modsecRulesRegistry.getModsecSafeRulesBlob()));
    }
  }

  @Test
  public void testRules() {
    Map<String, AnomalyRuleInfo> anomalyRuleInfos = modsecRulesRegistry.getModsecRuleInfos();
    assertEquals(10, anomalyRuleInfos.size());

    String safeCrsRules =
        modsecCrsRulesHandler.loadModsecCrsFileContents("modsec/crs/modsec-safe-rules.conf");

    int numSafeRules = countMatches(safeCrsRules, "SecRule") - countMatches(safeCrsRules, "chain");
    int countSubRules =
        anomalyRuleInfos.values().stream()
            .map(AnomalyRuleInfo::getSubRuleInfosCount)
            .reduce(0, (a, b) -> a + b);
    assertEquals(numSafeRules, countSubRules);

    // check for no duplicate names..
    List<String> subRuleNames =
        anomalyRuleInfos.values().stream()
            .flatMap(rule -> rule.getSubRuleInfosList().stream())
            .map(AnomalySubRuleInfo::getRuleName)
            .collect(Collectors.toList());
    assertEquals(new HashSet<>(subRuleNames).size(), subRuleNames.size());
  }

  private int countMatches(String text, String str) {
    Matcher matcher = Pattern.compile(str).matcher(text);
    int count = 0;
    while (matcher.find()) {
      count++;
    }
    return count;
  }
}
