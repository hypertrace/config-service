package ai.traceable.anomaly.config.service.global;

import static ai.traceable.license.metering.service.api.v1.LicenseInfo.Tier.TIER_ENTERPRISE;
import static ai.traceable.license.metering.service.api.v1.LicenseInfo.Tier.TIER_TEAM_TRIAL;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatus;
import com.typesafe.config.ConfigFactory;

public class AnomalyGlobalConfigServiceConfigTest {

  public void testConfig() {
    AnomalyGlobalConfigServiceConfig config =
        new AnomalyGlobalConfigServiceConfig(
            ConfigFactory.parseString(
                "global {\n"
                    + "  disabled = false\n"
                    + "  internal = true\n"
                    + "  licenseTiers = [\n"
                    + "    {\n"
                    + "        tier = TIER_TEAM_TRIAL\n"
                    + "        disabled = true\n"
                    + "    }\n"
                    + "  ]\n"
                    + "}"));
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
  }
}
