package ai.traceable.anomaly.config.service.global;

import static ai.traceable.anomaly.config.service.v1.AnomalyConfidenceLevel.ANOMALY_CONFIDENCE_LEVEL_MEDIUM;
import static ai.traceable.license.metering.service.api.v1.LicenseInfo.Tier.TIER_ENTERPRISE;
import static ai.traceable.license.metering.service.api.v1.LicenseInfo.Tier.TIER_TEAM_TRIAL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatus;
import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.Test;

public class AnomalyGlobalConfigServiceConfigTest {

  @Test
  public void testConfig() {
    AnomalyGlobalConfigServiceConfig config =
        new AnomalyGlobalConfigServiceConfig(
            ConfigFactory.parseString(
                "  disabled = false\n"
                    + "  internal = true\n"
                    + "  minConfidenceLevel = ANOMALY_CONFIDENCE_LEVEL_MEDIUM\n"
                    + "  licenseTiers = [\n"
                    + "    {\n"
                    + "        tier = TIER_TEAM_TRIAL\n"
                    + "        disabled = true\n"
                    + "    }\n"
                    + "  ]"));
    {
      AnomalyConfigStatus configStatus = config.getConfigStatus(TIER_TEAM_TRIAL);
      assertTrue(configStatus.getInternal());
      assertTrue(configStatus.getDisabled());
    }
    {
      AnomalyConfigStatus configStatus = config.getConfigStatus(TIER_ENTERPRISE);
      assertTrue(configStatus.getInternal());
      assertFalse(configStatus.getDisabled());
    }
    assertEquals(ANOMALY_CONFIDENCE_LEVEL_MEDIUM, config.getMinConfidenceLevel());
  }
}
