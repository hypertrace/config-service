package ai.traceable.config.service;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import ai.traceable.anomaly.config.service.registry.common.ConfigConverter;
import ai.traceable.anomaly.config.service.registry.modsec.ModsecCrsRulesHandler;
import ai.traceable.anomaly.config.service.registry.modsec.ModsecRulesRegistryImpl;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleType;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import ai.traceable.modsecurity.RuleEngine;
import ai.traceable.modsecurity.utils.ModsecRuleUtils;
import ai.traceable.platform.coraza.waf.service.CorazaWafServiceClient;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.SystemUtils;
import org.junit.jupiter.api.Test;

public class WafIntegrationTest extends TraceableConfigServiceIntegrationTestBase {

  private final ModsecCrsRulesHandler modsecCrsRulesHandler =
      new ModsecCrsRulesHandler(new ModsecRuleUtils());
  private final ModsecRulesRegistryImpl modsecRulesRegistry =
      new ModsecRulesRegistryImpl(new ConfigConverter(), modsecCrsRulesHandler);

  @Test
  public void testModsecCrsRuleEngine() throws IOException {

    String wafID = "TestCorazaWaf";
    String corazaClientString = "localhost";
    int corazaPort = 9000;
    // Load the native library only for Linux OS
    if (SystemUtils.IS_OS_LINUX) {
      RuleEngine.loadNativeLibrary();
    }
    CorazaWafServiceClient client =
        new CorazaWafServiceClient(corazaClientString, corazaPort, Duration.ofSeconds(5));

    for (ModsecRuleVersion version : ModsecRuleVersion.values()) {
      for (AnomalySubRuleType subRuleType : AnomalySubRuleType.values()) {
        try {
          if (subRuleType.equals(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_UNSPECIFIED)
              || subRuleType.equals(AnomalySubRuleType.UNRECOGNIZED)) {
            assertTrue(
                modsecRulesRegistry
                    .getModsecCrsRulesBlob(List.of(subRuleType), version, Set.of())
                    .isEmpty());
          } else {
            if (version.name().contains("CORAZA")) {
              try {
                client.initWaf(
                    wafID,
                    modsecRulesRegistry.getModsecCrsRulesBlob(
                        List.of(subRuleType), version, Set.of()));
              } catch (Exception e) {
                fail("Failed for CORAZA version: " + version + " subRuleType: " + subRuleType, e);
              }
            } else {
              if (SystemUtils.IS_OS_LINUX) {
                assertNotNull(
                    RuleEngine.create(
                        modsecRulesRegistry.getModsecCrsRulesBlob(
                            List.of(subRuleType), version, Set.of())),
                    "Failed for Modsec version: " + version + " subRuleType: " + subRuleType);
              }
            }
          }
        } catch (Exception e) {
          fail("Unexpected failure for version: " + version + " subRuleType: " + subRuleType, e);
        }
      }
    }
  }
}
