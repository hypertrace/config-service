package ai.traceable.anomaly.config.service;

import static org.junit.jupiter.api.Assertions.*;

import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.Test;

class AnomalyConfigServiceConfigTest {

  @Test
  void getModsecRuleVersion() {
    AnomalyConfigServiceConfig config =
        new AnomalyConfigServiceConfig(
            ConfigFactory.parseString("modsecRuleVersion = MODSEC_RULE_VERSION_V3"));
    assertEquals(ModsecRuleVersion.MODSEC_RULE_VERSION_V3, config.getModsecRuleVersion());
    config =
        new AnomalyConfigServiceConfig(
            ConfigFactory.parseString("modsecRuleVersion = MODSEC_RULE_VERSION_V3_SECARG_LIMITS"));
    assertEquals(
        ModsecRuleVersion.MODSEC_RULE_VERSION_V3_SECARG_LIMITS, config.getModsecRuleVersion());
  }
}
