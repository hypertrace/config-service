package ai.traceable.anomaly.config.service.global.version;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import ai.traceable.anomaly.config.service.v1.global.AvailableRuleVersions;
import ai.traceable.anomaly.config.service.v1.global.AvailableRuleVersionsFilter;
import ai.traceable.anomaly.config.service.v1.global.RuleType;
import ai.traceable.anomaly.config.service.v1.global.RuleVersionType;
import ai.traceable.protection.rules.apiprotect.v1.ApiProtectRuleAvailableVersions;
import ai.traceable.protection.rules.apiprotect.v1.ApiProtectionRulesProvider;
import ai.traceable.protection.rules.webapp.v1.WebAppProtectionRulesProvider;
import ai.traceable.protection.rules.webapp.v1.WebAppRuleAvailableVersions;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class RuleVersionManagerImplTest {

  @Mock private WebAppProtectionRulesProvider webAppProtectionRulesProvider;
  @Mock private ApiProtectionRulesProvider apiProtectionRulesProvider;
  @InjectMocks private RuleVersionManagerImpl ruleVersionManager;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  void testGetAvailableWebAppRuleVersions() {
    AvailableRuleVersionsFilter filter =
        AvailableRuleVersionsFilter.newBuilder()
            .addAllVersionTypes(List.of(RuleVersionType.RULE_VERSION_TYPE_STABLE))
            .build();

    when(webAppProtectionRulesProvider.getWebAppRuleAvailableVersions(any()))
        .thenReturn(WebAppRuleAvailableVersions.getDefaultInstance());
    AvailableRuleVersions result =
        ruleVersionManager.getAvailableRuleVersions(RuleType.RULE_TYPE_WEB_APPLICATION, filter);
    assertNotNull(result);
    assertEquals(RuleType.RULE_TYPE_WEB_APPLICATION, result.getRuleType());
    assertTrue(result.getVersionsList().isEmpty());
  }

  @Test
  void testGetAvailableApiProtectRuleVersions() {
    AvailableRuleVersionsFilter filter =
        AvailableRuleVersionsFilter.newBuilder()
            .addAllVersionTypes(List.of(RuleVersionType.RULE_VERSION_TYPE_STABLE))
            .build();

    when(apiProtectionRulesProvider.getApiProtectRuleAvailableVersions(any()))
        .thenReturn(ApiProtectRuleAvailableVersions.getDefaultInstance());
    AvailableRuleVersions result =
        ruleVersionManager.getAvailableRuleVersions(RuleType.RULE_TYPE_API_PROTECTION, filter);
    assertNotNull(result);
    assertEquals(RuleType.RULE_TYPE_API_PROTECTION, result.getRuleType());
    assertTrue(result.getVersionsList().isEmpty());
  }

  @Test
  void testInvalidRuleType() {
    AvailableRuleVersionsFilter filter = AvailableRuleVersionsFilter.newBuilder().build();

    Exception exception =
        assertThrows(
            RuntimeException.class,
            () -> ruleVersionManager.getAvailableRuleVersions(RuleType.UNRECOGNIZED, filter));
    String expectedMessage = "Can't get the number of an unknown enum value.";
    String actualMessage = exception.getMessage();
    assertTrue(actualMessage.contains(expectedMessage));
  }
}
