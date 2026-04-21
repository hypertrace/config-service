package ai.traceable.anomaly.config.service.global.ruleinfo;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import ai.traceable.anomaly.config.service.v1.AnomalySubRuleType;
import ai.traceable.anomaly.config.service.v1.RuleVersion;
import ai.traceable.anomaly.config.service.v1.RuleVersionType;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import ai.traceable.modsecurity.utils.ModsecRuleUtils;
import ai.traceable.protection.rules.webapp.v1.WebAppProtectionRulesProvider;
import ai.traceable.protection.rules.webapp.v1.WebAppProtectionRulesProviderModule;
import com.google.inject.Guice;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class WebAppRuleInfoProviderTest {

  private final WebAppProtectionRulesProvider webAppProtectionRulesProvider =
      Guice.createInjector(new WebAppProtectionRulesProviderModule())
          .getInstance(WebAppProtectionRulesProvider.class);
  private WebAppRuleInfoProvider webAppRuleInfoProvider;

  @BeforeEach
  void setUp() {
    webAppRuleInfoProvider =
        new WebAppRuleInfoProviderImpl(webAppProtectionRulesProvider, new ModsecRuleUtils());
  }

  @Test
  void testGetCrsRulesBlobWithDisabledRules() {
    RuleVersion stableVersion =
        RuleVersion.newBuilder()
            .setVersion("1.5.0")
            .setVersionType(RuleVersionType.RULE_VERSION_TYPE_STABLE)
            .build();
    Set<String> disabledRuleIds = Set.of();
    String crsRulesBlob =
        webAppRuleInfoProvider.getCrsRulesBlob(
            List.of(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK),
            ModsecRuleVersion.MODSEC_RULE_VERSION_V3,
            disabledRuleIds,
            stableVersion,
            true);
    assertNotNull(crsRulesBlob, "CRS rules blob should not be null");
    assertFalse(crsRulesBlob.isEmpty(), "CRS rules blob should not be empty");
  }

  @Test
  void testGetImpactScoringBlobWithDefaultVersion() {
    // Test with default version
    String impactScoringBlob =
        webAppRuleInfoProvider.getImpactScoringBlob(RuleVersion.getDefaultInstance());
    assertNotNull(impactScoringBlob, "Impact scoring blob should not be null");
    assertFalse(impactScoringBlob.isEmpty(), "Impact scoring blob should not be empty");
  }

  @Test
  void testGetImpactScoringBlobWithSpecificVersion() {
    String impactScoringBlob =
        webAppRuleInfoProvider.getImpactScoringBlob(RuleVersion.getDefaultInstance());
    assertNotNull(impactScoringBlob, "Impact scoring blob should not be null");
    assertFalse(impactScoringBlob.isEmpty(), "Impact scoring blob should not be empty");
  }
}
