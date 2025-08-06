package ai.traceable.anomaly.config.service.global.version;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import ai.traceable.anomaly.config.service.v1.ChangeLog;
import ai.traceable.anomaly.config.service.v1.RuleType;
import ai.traceable.anomaly.config.service.v1.RuleVersion;
import ai.traceable.anomaly.config.service.v1.RuleVersionType;
import ai.traceable.anomaly.config.service.v1.global.AvailableRuleVersions;
import ai.traceable.anomaly.config.service.v1.global.AvailableRuleVersionsFilter;
import ai.traceable.anomaly.config.service.v1.global.RulesChangeLog;
import ai.traceable.anomaly.config.service.v1.global.ThreatRuleChange;
import ai.traceable.anomaly.config.service.v1.global.ThreatRuleUpdateDetails;
import ai.traceable.anomaly.config.service.v1.global.ThreatTypeChange;
import ai.traceable.protection.rules.apiprotect.v1.ApiProtectRuleAvailableVersions;
import ai.traceable.protection.rules.apiprotect.v1.ApiProtectRulesChangeLog;
import ai.traceable.protection.rules.apiprotect.v1.ApiProtectRulesVersion;
import ai.traceable.protection.rules.apiprotect.v1.ApiProtectRulesVersionUpdateDetails;
import ai.traceable.protection.rules.apiprotect.v1.ApiProtectThreatRuleChange;
import ai.traceable.protection.rules.apiprotect.v1.ApiProtectThreatRuleUpdateDetails;
import ai.traceable.protection.rules.apiprotect.v1.ApiProtectThreatTypeChange;
import ai.traceable.protection.rules.apiprotect.v1.ApiProtectThreatTypeUpdateDetails;
import ai.traceable.protection.rules.apiprotect.v1.ApiProtectionRulesProvider;
import ai.traceable.protection.rules.webapp.v1.StringKeyValueUpdate;
import ai.traceable.protection.rules.webapp.v1.StringList;
import ai.traceable.protection.rules.webapp.v1.StringValueUpdate;
import ai.traceable.protection.rules.webapp.v1.WebAppProtectionRulesProvider;
import ai.traceable.protection.rules.webapp.v1.WebAppRuleAvailableVersions;
import ai.traceable.protection.rules.webapp.v1.WebAppRulesChangeLog;
import ai.traceable.protection.rules.webapp.v1.WebAppRulesData;
import ai.traceable.protection.rules.webapp.v1.WebAppRulesVersion;
import ai.traceable.protection.rules.webapp.v1.WebAppRulesVersionType;
import ai.traceable.protection.rules.webapp.v1.WebAppRulesVersionUpdateDetails;
import ai.traceable.protection.rules.webapp.v1.WebAppSeverity;
import ai.traceable.protection.rules.webapp.v1.WebAppThreatRule;
import ai.traceable.protection.rules.webapp.v1.WebAppThreatRuleChange;
import ai.traceable.protection.rules.webapp.v1.WebAppThreatRuleDefinition;
import ai.traceable.protection.rules.webapp.v1.WebAppThreatRuleUpdateDetails;
import ai.traceable.protection.rules.webapp.v1.WebAppThreatRuleUpdateDetails.ThreatRuleUpdate;
import ai.traceable.protection.rules.webapp.v1.WebAppThreatType;
import ai.traceable.protection.rules.webapp.v1.WebAppThreatTypeChange;
import ai.traceable.protection.rules.webapp.v1.WebAppThreatTypeUpdateDetails;
import ai.traceable.protection.rules.webapp.v1.WebAppThreatTypeUpdateDetails.ThreatTypeUpdate;
import ai.traceable.protection.rules.webapp.v1.WebAppVersionedRules;
import java.util.Arrays;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
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

  @Test
  void testGetRulesChangeLog() {
    // Setup input parameters
    RequestContext requestContext = mock(RequestContext.class);
    RuleType ruleType = RuleType.RULE_TYPE_WEB_APPLICATION;
    RuleVersion currentVersion =
        RuleVersion.newBuilder()
            .setVersion("1.1.0")
            .setVersionType(RuleVersionType.RULE_VERSION_TYPE_STABLE)
            .build();
    RuleVersion previousVersion =
        RuleVersion.newBuilder()
            .setVersion("1.0.0")
            .setVersionType(RuleVersionType.RULE_VERSION_TYPE_STABLE)
            .build();
    WebAppRulesChangeLog.Builder webAppRulesChangeLogBuilder = WebAppRulesChangeLog.newBuilder();
    WebAppThreatTypeChange threatTypeChange =
        WebAppThreatTypeChange.newBuilder()
            .setThreatTypeIdsAdded(
                StringList.newBuilder().addAllValues(Arrays.asList("crs_123", "crs_133")).build())
            .build();
    webAppRulesChangeLogBuilder.addThreatTypeChanges(threatTypeChange);
    List<String> addedRules = Arrays.asList("crs_1234", "crs_4567", "crs_7890");
    WebAppThreatRuleChange ruleAdditionChange =
        WebAppThreatRuleChange.newBuilder()
            .setRuleIdsAdded(StringList.newBuilder().addAllValues(addedRules).build())
            .build();
    webAppRulesChangeLogBuilder.addRuleChanges(ruleAdditionChange);
    WebAppThreatRuleUpdateDetails ruleUpdateDetails =
        WebAppThreatRuleUpdateDetails.newBuilder()
            .setRuleId("crs_13579")
            .addUpdates(
                WebAppThreatRuleUpdateDetails.ThreatRuleUpdate.newBuilder()
                    .setSignatureUpdated(true)
                    .build())
            .build();
    WebAppThreatRuleChange ruleUpdateChange =
        WebAppThreatRuleChange.newBuilder().setRuleUpdated(ruleUpdateDetails).build();
    webAppRulesChangeLogBuilder.addRuleChanges(ruleUpdateChange);
    ArgumentCaptor<WebAppRulesVersion> currentVersionCaptor =
        ArgumentCaptor.forClass(WebAppRulesVersion.class);
    ArgumentCaptor<WebAppRulesVersion> previousVersionCaptor =
        ArgumentCaptor.forClass(WebAppRulesVersion.class);

    when(webAppProtectionRulesProvider.getWebAppRulesChangeLog(
            previousVersionCaptor.capture(), currentVersionCaptor.capture()))
        .thenReturn(webAppRulesChangeLogBuilder.build());
    RulesChangeLog result =
        ruleVersionManager.getRulesChangeLog(ruleType, currentVersion, previousVersion);
    verify(webAppProtectionRulesProvider)
        .getWebAppRulesChangeLog(any(WebAppRulesVersion.class), any(WebAppRulesVersion.class));
    assertEquals("1.1.0", currentVersionCaptor.getValue().getVersion());
    assertEquals(
        WebAppRulesVersionType.WEB_APP_RULES_VERSION_TYPE_STABLE,
        currentVersionCaptor.getValue().getVersionType());
    assertEquals("1.0.0", previousVersionCaptor.getValue().getVersion());
    assertEquals(
        WebAppRulesVersionType.WEB_APP_RULES_VERSION_TYPE_STABLE,
        previousVersionCaptor.getValue().getVersionType());
    assertNotNull(result);
    assertNotNull(result.getVersionUpdated());
    assertEquals("1.0.0", result.getVersionUpdated().getOldVersion().getVersion());
    assertEquals(
        RuleVersionType.RULE_VERSION_TYPE_STABLE,
        result.getVersionUpdated().getOldVersion().getVersionType());
    assertEquals("1.1.0", result.getVersionUpdated().getNewVersion().getVersion());
    assertEquals(
        RuleVersionType.RULE_VERSION_TYPE_STABLE,
        result.getVersionUpdated().getNewVersion().getVersionType());
    assertEquals(1, result.getThreatTypeChangesCount());
    ThreatTypeChange resultThreatTypeChange = result.getThreatTypeChanges(0);
    assertTrue(resultThreatTypeChange.hasThreatTypeIdsAdded());
    assertEquals(2, resultThreatTypeChange.getThreatTypeIdsAdded().getValuesCount());
    assertTrue(resultThreatTypeChange.getThreatTypeIdsAdded().getValuesList().contains("crs_123"));
    assertTrue(resultThreatTypeChange.getThreatTypeIdsAdded().getValuesList().contains("crs_133"));
    assertEquals(2, result.getRuleChangesCount());
    boolean foundRuleAdditions = false;
    boolean foundRuleUpdate = false;

    for (ThreatRuleChange change : result.getRuleChangesList()) {
      if (change.hasRuleIdsAdded()) {
        foundRuleAdditions = true;
        assertEquals(3, change.getRuleIdsAdded().getValuesCount());
        assertTrue(change.getRuleIdsAdded().getValuesList().contains("crs_1234"));
        assertTrue(change.getRuleIdsAdded().getValuesList().contains("crs_4567"));
        assertTrue(change.getRuleIdsAdded().getValuesList().contains("crs_7890"));
      } else if (change.hasRuleUpdated()) {
        foundRuleUpdate = true;
        ThreatRuleUpdateDetails updateDetails = change.getRuleUpdated();
        assertEquals("crs_13579", updateDetails.getRuleId());
        assertEquals(1, updateDetails.getUpdatesCount());
        assertTrue(updateDetails.getUpdates(0).getSignatureUpdated());
      }
    }
    assertTrue(foundRuleAdditions, "Did not find rule additions in the result");
    assertTrue(foundRuleUpdate, "Did not find rule update in the result");
  }

  @Test
  void testWebAppThreatTypeLabelUpdate() {
    RuleVersion currentVersion =
        RuleVersion.newBuilder()
            .setVersion("3.0.0")
            .setVersionType(RuleVersionType.RULE_VERSION_TYPE_BETA)
            .build();
    RuleVersion previousVersion =
        RuleVersion.newBuilder()
            .setVersion("2.5.0")
            .setVersionType(RuleVersionType.RULE_VERSION_TYPE_STABLE)
            .build();

    WebAppRulesChangeLog webAppChangeLog =
        WebAppRulesChangeLog.newBuilder()
            .setVersionUpdated(
                WebAppRulesVersionUpdateDetails.newBuilder()
                    .setOldVersion(WebAppRulesVersion.newBuilder().setVersion("2.5.0").build())
                    .setNewVersion(WebAppRulesVersion.newBuilder().setVersion("3.0.0").build())
                    .build())
            .addThreatTypeChanges(
                WebAppThreatTypeChange.newBuilder()
                    .setThreatTypeUpdated(
                        WebAppThreatTypeUpdateDetails.newBuilder()
                            .setThreatTypeId("crs_102")
                            .addUpdates(
                                ThreatTypeUpdate.newBuilder()
                                    .setThreatLabelUpdated(
                                        StringKeyValueUpdate.newBuilder()
                                            .setKey("severity")
                                            .setValueUpdated(
                                                StringValueUpdate.newBuilder()
                                                    .setOldValue("medium")
                                                    .setNewValue("high")
                                                    .build())
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();

    when(webAppProtectionRulesProvider.getWebAppRulesChangeLog(any(), any()))
        .thenReturn(webAppChangeLog);

    RulesChangeLog result =
        ruleVersionManager.getRulesChangeLog(
            RuleType.RULE_TYPE_WEB_APPLICATION, currentVersion, previousVersion);

    assertNotNull(result);
    assertEquals(1, result.getThreatTypeChangesCount());
    ThreatTypeChange change = result.getThreatTypeChanges(0);
    assertTrue(change.hasThreatTypeUpdated());
    assertEquals("crs_102", change.getThreatTypeUpdated().getThreatTypeId());
    assertEquals(1, change.getThreatTypeUpdated().getUpdatesCount());
    assertTrue(change.getThreatTypeUpdated().getUpdates(0).hasThreatLabelUpdated());
    assertEquals(
        "severity", change.getThreatTypeUpdated().getUpdates(0).getThreatLabelUpdated().getKey());
    assertTrue(
        change.getThreatTypeUpdated().getUpdates(0).getThreatLabelUpdated().hasValueUpdated());
    assertEquals(
        "medium",
        change
            .getThreatTypeUpdated()
            .getUpdates(0)
            .getThreatLabelUpdated()
            .getValueUpdated()
            .getOldValue());
    assertEquals(
        "high",
        change
            .getThreatTypeUpdated()
            .getUpdates(0)
            .getThreatLabelUpdated()
            .getValueUpdated()
            .getNewValue());
  }

  @Test
  void testApiProtectThreatTypeLabelUpdate() {
    RuleVersion currentVersion =
        RuleVersion.newBuilder()
            .setVersion("3.0.0")
            .setVersionType(RuleVersionType.RULE_VERSION_TYPE_BETA)
            .build();
    RuleVersion previousVersion =
        RuleVersion.newBuilder()
            .setVersion("2.5.0")
            .setVersionType(RuleVersionType.RULE_VERSION_TYPE_STABLE)
            .build();

    ApiProtectRulesChangeLog apiChangeLog =
        ApiProtectRulesChangeLog.newBuilder()
            .setVersionUpdated(
                ApiProtectRulesVersionUpdateDetails.newBuilder()
                    .setOldVersion(ApiProtectRulesVersion.newBuilder().setVersion("2.5.0").build())
                    .setNewVersion(ApiProtectRulesVersion.newBuilder().setVersion("3.0.0").build())
                    .build())
            .addThreatTypeChanges(
                ApiProtectThreatTypeChange.newBuilder()
                    .setThreatTypeUpdated(
                        ApiProtectThreatTypeUpdateDetails.newBuilder()
                            .setThreatTypeId("api_930")
                            .addUpdates(
                                ApiProtectThreatTypeUpdateDetails.ThreatTypeUpdate.newBuilder()
                                    .setThreatLabelUpdated(
                                        ai.traceable.protection.rules.apiprotect.v1
                                            .StringKeyValueUpdate.newBuilder()
                                            .setKey("severity")
                                            .setValueUpdated(
                                                ai.traceable.protection.rules.apiprotect.v1
                                                    .StringValueUpdate.newBuilder()
                                                    .setOldValue("low")
                                                    .setNewValue("medium")
                                                    .build())
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();

    when(apiProtectionRulesProvider.getApiProtectRulesChangeLog(any(), any()))
        .thenReturn(apiChangeLog);

    RulesChangeLog result =
        ruleVersionManager.getRulesChangeLog(
            RuleType.RULE_TYPE_API_PROTECTION, currentVersion, previousVersion);

    assertNotNull(result);
    assertEquals(1, result.getThreatTypeChangesCount());
    ThreatTypeChange change = result.getThreatTypeChanges(0);
    assertTrue(change.hasThreatTypeUpdated());
    assertEquals("api_930", change.getThreatTypeUpdated().getThreatTypeId());
    assertEquals(1, change.getThreatTypeUpdated().getUpdatesCount());
    assertTrue(change.getThreatTypeUpdated().getUpdates(0).hasThreatLabelUpdated());
    assertEquals(
        "severity", change.getThreatTypeUpdated().getUpdates(0).getThreatLabelUpdated().getKey());
    assertTrue(
        change.getThreatTypeUpdated().getUpdates(0).getThreatLabelUpdated().hasValueUpdated());
    assertEquals(
        "low",
        change
            .getThreatTypeUpdated()
            .getUpdates(0)
            .getThreatLabelUpdated()
            .getValueUpdated()
            .getOldValue());
    assertEquals(
        "medium",
        change
            .getThreatTypeUpdated()
            .getUpdates(0)
            .getThreatLabelUpdated()
            .getValueUpdated()
            .getNewValue());
  }

  @Test
  void testGetRulesChangeLogWebApp() {
    RequestContext requestContext = mock(RequestContext.class);
    RuleType ruleType = RuleType.RULE_TYPE_WEB_APPLICATION;
    RuleVersion currentVersion =
        RuleVersion.newBuilder()
            .setVersion("2.0.0")
            .setVersionType(RuleVersionType.RULE_VERSION_TYPE_STABLE)
            .build();
    RuleVersion previousVersion =
        RuleVersion.newBuilder()
            .setVersion("1.9.0")
            .setVersionType(RuleVersionType.RULE_VERSION_TYPE_BETA)
            .build();

    WebAppRulesChangeLog.Builder webAppRulesChangeLogBuilder = WebAppRulesChangeLog.newBuilder();
    WebAppThreatTypeChange threatTypeAddition =
        WebAppThreatTypeChange.newBuilder()
            .setThreatTypeIdsAdded(
                StringList.newBuilder().addAllValues(Arrays.asList("crs_942", "crs_941")).build())
            .build();
    webAppRulesChangeLogBuilder.addThreatTypeChanges(threatTypeAddition);
    WebAppThreatTypeChange threatTypeRemoval =
        WebAppThreatTypeChange.newBuilder()
            .setThreatTypeIdsRemoved(
                StringList.newBuilder().addAllValues(Arrays.asList("crs_920", "crs_921")).build())
            .build();
    webAppRulesChangeLogBuilder.addThreatTypeChanges(threatTypeRemoval);
    WebAppThreatTypeChange threatTypeUpdate =
        WebAppThreatTypeChange.newBuilder()
            .setThreatTypeUpdated(
                WebAppThreatTypeUpdateDetails.newBuilder()
                    .setThreatTypeId("crs_930")
                    .addUpdates(
                        WebAppThreatTypeUpdateDetails.ThreatTypeUpdate.newBuilder()
                            .setNameUpdated(
                                ai.traceable.protection.rules.webapp.v1.StringValueUpdate
                                    .newBuilder()
                                    .setOldValue("Old Name")
                                    .setNewValue("New Name")
                                    .build())
                            .build())
                    .build())
            .build();
    webAppRulesChangeLogBuilder.addThreatTypeChanges(threatTypeUpdate);
    List<String> addedRules = Arrays.asList("crs_9421", "crs_9422", "crs_9423");
    WebAppThreatRuleChange ruleAdditions =
        WebAppThreatRuleChange.newBuilder()
            .setRuleIdsAdded(StringList.newBuilder().addAllValues(addedRules).build())
            .build();
    webAppRulesChangeLogBuilder.addRuleChanges(ruleAdditions);
    List<String> removedRules = Arrays.asList("crs_9320", "crs_9321");
    WebAppThreatRuleChange ruleRemovals =
        WebAppThreatRuleChange.newBuilder()
            .setRuleIdsRemoved(StringList.newBuilder().addAllValues(removedRules).build())
            .build();
    webAppRulesChangeLogBuilder.addRuleChanges(ruleRemovals);
    WebAppThreatRuleUpdateDetails ruleUpdateDetails =
        WebAppThreatRuleUpdateDetails.newBuilder()
            .setRuleId("crs_9424")
            .addUpdates(
                WebAppThreatRuleUpdateDetails.ThreatRuleUpdate.newBuilder()
                    .setSignatureUpdated(true)
                    .build())
            .addUpdates(
                WebAppThreatRuleUpdateDetails.ThreatRuleUpdate.newBuilder()
                    .setNameUpdated(
                        ai.traceable.protection.rules.webapp.v1.StringValueUpdate.newBuilder()
                            .setOldValue("Old Name")
                            .setNewValue("New Name")
                            .build())
                    .build())
            .addUpdates(
                WebAppThreatRuleUpdateDetails.ThreatRuleUpdate.newBuilder()
                    .setSeverityUpdated(
                        ai.traceable.protection.rules.webapp.v1.StringValueUpdate.newBuilder()
                            .setOldValue("medium")
                            .setNewValue("high")
                            .build())
                    .build())
            .build();
    WebAppThreatRuleChange ruleUpdate =
        WebAppThreatRuleChange.newBuilder().setRuleUpdated(ruleUpdateDetails).build();
    webAppRulesChangeLogBuilder.addRuleChanges(ruleUpdate);

    when(webAppProtectionRulesProvider.getWebAppRulesChangeLog(
            any(WebAppRulesVersion.class), any(WebAppRulesVersion.class)))
        .thenReturn(webAppRulesChangeLogBuilder.build());

    RulesChangeLog result =
        ruleVersionManager.getRulesChangeLog(ruleType, currentVersion, previousVersion);
    assertNotNull(result);
    assertEquals("1.9.0", result.getVersionUpdated().getOldVersion().getVersion());
    assertEquals(
        RuleVersionType.RULE_VERSION_TYPE_BETA,
        result.getVersionUpdated().getOldVersion().getVersionType());
    assertEquals("2.0.0", result.getVersionUpdated().getNewVersion().getVersion());
    assertEquals(
        RuleVersionType.RULE_VERSION_TYPE_STABLE,
        result.getVersionUpdated().getNewVersion().getVersionType());
    assertEquals(3, result.getThreatTypeChangesCount());

    boolean foundThreatTypeAdditions = false;
    boolean foundThreatTypeRemovals = false;
    boolean foundThreatTypeUpdates = false;

    for (ThreatTypeChange change : result.getThreatTypeChangesList()) {
      if (change.hasThreatTypeIdsAdded()) {
        foundThreatTypeAdditions = true;
        assertEquals(2, change.getThreatTypeIdsAdded().getValuesCount());
        assertTrue(change.getThreatTypeIdsAdded().getValuesList().contains("crs_942"));
        assertTrue(change.getThreatTypeIdsAdded().getValuesList().contains("crs_941"));
      } else if (change.hasThreatTypeIdsRemoved()) {
        foundThreatTypeRemovals = true;
        assertEquals(2, change.getThreatTypeIdsRemoved().getValuesCount());
        assertTrue(change.getThreatTypeIdsRemoved().getValuesList().contains("crs_920"));
        assertTrue(change.getThreatTypeIdsRemoved().getValuesList().contains("crs_921"));
      } else if (change.hasThreatTypeUpdated()) {
        foundThreatTypeUpdates = true;
        assertEquals("crs_930", change.getThreatTypeUpdated().getThreatTypeId());
        assertEquals(1, change.getThreatTypeUpdated().getUpdatesCount());
        assertTrue(change.getThreatTypeUpdated().getUpdates(0).hasNameUpdated());
        assertEquals(
            "Old Name", change.getThreatTypeUpdated().getUpdates(0).getNameUpdated().getOldValue());
        assertEquals(
            "New Name", change.getThreatTypeUpdated().getUpdates(0).getNameUpdated().getNewValue());
      }
    }

    assertTrue(foundThreatTypeAdditions, "Did not find threat type additions");
    assertTrue(foundThreatTypeRemovals, "Did not find threat type removals");
    assertTrue(foundThreatTypeUpdates, "Did not find threat type updates");

    assertEquals(3, result.getRuleChangesCount());

    boolean foundRuleAdditions = false;
    boolean foundRuleRemovals = false;
    boolean foundRuleUpdate = false;

    for (ThreatRuleChange change : result.getRuleChangesList()) {
      if (change.hasRuleIdsAdded()) {
        foundRuleAdditions = true;
        assertEquals(3, change.getRuleIdsAdded().getValuesCount());
        assertTrue(change.getRuleIdsAdded().getValuesList().contains("crs_9421"));
        assertTrue(change.getRuleIdsAdded().getValuesList().contains("crs_9422"));
        assertTrue(change.getRuleIdsAdded().getValuesList().contains("crs_9423"));
      } else if (change.hasRuleIdsRemoved()) {
        foundRuleRemovals = true;
        assertEquals(2, change.getRuleIdsRemoved().getValuesCount());
        assertTrue(change.getRuleIdsRemoved().getValuesList().contains("crs_9320"));
        assertTrue(change.getRuleIdsRemoved().getValuesList().contains("crs_9321"));
      } else if (change.hasRuleUpdated()) {
        foundRuleUpdate = true;
        ThreatRuleUpdateDetails updateDetails = change.getRuleUpdated();
        assertEquals("crs_9424", updateDetails.getRuleId());
        assertEquals(3, updateDetails.getUpdatesCount());

        boolean foundSignatureUpdate = false;
        boolean foundNameUpdate = false;
        boolean foundSeverityUpdate = false;

        for (ThreatRuleUpdateDetails.ThreatRuleUpdate update : updateDetails.getUpdatesList()) {
          if (update.getSignatureUpdated()) {
            foundSignatureUpdate = true;
          } else if (update.hasNameUpdated()) {
            foundNameUpdate = true;
            assertEquals("Old Name", update.getNameUpdated().getOldValue());
            assertEquals("New Name", update.getNameUpdated().getNewValue());
          } else if (update.hasSeverityUpdated()) {
            foundSeverityUpdate = true;
            assertEquals("medium", update.getSeverityUpdated().getOldValue());
            assertEquals("high", update.getSeverityUpdated().getNewValue());
          }
        }

        assertTrue(foundSignatureUpdate, "Did not find signature update");
        assertTrue(foundNameUpdate, "Did not find name update");
        assertTrue(foundSeverityUpdate, "Did not find severity update");
      }
    }

    assertTrue(foundRuleAdditions, "Did not find rule additions");
    assertTrue(foundRuleRemovals, "Did not find rule removals");
    assertTrue(foundRuleUpdate, "Did not find rule update");
  }

  @Test
  void testGetChangeLogDocWebApp() {
    RuleVersion currentVersion =
        RuleVersion.newBuilder()
            .setVersion("3.0.0")
            .setVersionType(RuleVersionType.RULE_VERSION_TYPE_BETA)
            .build();

    RuleVersion previousVersion =
        RuleVersion.newBuilder()
            .setVersion("2.5.0")
            .setVersionType(RuleVersionType.RULE_VERSION_TYPE_STABLE)
            .build();

    WebAppRulesChangeLog webAppRulesChangeLog =
        WebAppRulesChangeLog.newBuilder()
            .setVersionUpdated(
                WebAppRulesVersionUpdateDetails.newBuilder()
                    .setOldVersion(
                        WebAppRulesVersion.newBuilder()
                            .setVersion(previousVersion.getVersion())
                            .setVersionType(
                                WebAppRulesVersionType.WEB_APP_RULES_VERSION_TYPE_STABLE)
                            .build())
                    .setNewVersion(
                        WebAppRulesVersion.newBuilder()
                            .setVersion(currentVersion.getVersion())
                            .setVersionType(WebAppRulesVersionType.WEB_APP_RULES_VERSION_TYPE_BETA)
                            .setVersionHighlights("Added new XSS rules")
                            .build())
                    .build())
            .addRuleChanges(
                WebAppThreatRuleChange.newBuilder()
                    .setRuleIdsRemoved(StringList.newBuilder().addValues("crs_1239876").build())
                    .build())
            .addRuleChanges(
                WebAppThreatRuleChange.newBuilder()
                    .setRuleIdsAdded(
                        StringList.newBuilder()
                            .addValues("crs_1234567")
                            .addValues("crs_1235679")
                            .build()))
            .addRuleChanges(
                WebAppThreatRuleChange.newBuilder()
                    .setRuleUpdated(
                        WebAppThreatRuleUpdateDetails.newBuilder()
                            .setRuleId("crs_123657")
                            .addUpdates(
                                ThreatRuleUpdate.newBuilder().setSignatureUpdated(true).build()))
                    .build())
            .build();

    WebAppVersionedRules webAppVersionedRule =
        WebAppVersionedRules.newBuilder()
            .setRulesData(
                WebAppRulesData.newBuilder()
                    .addThreatTypes(
                        WebAppThreatType.newBuilder().setTypeId("crs_123").setTypeName("Type Name"))
                    .addThreatRules(
                        WebAppThreatRule.newBuilder()
                            .setThreatRuleId("crs_1234567")
                            .setRuleDefinition(
                                WebAppThreatRuleDefinition.newBuilder()
                                    .setRuleName("Threat Rule Name 1")
                                    .setThreatTypeId("crs_123")
                                    .setSeverity(WebAppSeverity.WEB_APP_SEVERITY_CRITICAL)
                                    .build()))
                    .addThreatRules(
                        WebAppThreatRule.newBuilder()
                            .setThreatRuleId("crs_1235679")
                            .setRuleDefinition(
                                WebAppThreatRuleDefinition.newBuilder()
                                    .setRuleName("Threat Rule Name 2")
                                    .setThreatTypeId("crs_123")
                                    .setSeverity(WebAppSeverity.WEB_APP_SEVERITY_HIGH)
                                    .build()))
                    .addThreatRules(
                        WebAppThreatRule.newBuilder()
                            .setThreatRuleId("crs_1239876")
                            .setRuleDefinition(
                                WebAppThreatRuleDefinition.newBuilder()
                                    .setRuleName("Threat Rule Name 3")
                                    .setThreatTypeId("crs_123")
                                    .setSeverity(WebAppSeverity.WEB_APP_SEVERITY_LOW)
                                    .build()))
                    .addThreatRules(
                        WebAppThreatRule.newBuilder()
                            .setThreatRuleId("crs_123657")
                            .setRuleDefinition(
                                WebAppThreatRuleDefinition.newBuilder()
                                    .setRuleName("Threat Rule Name 4")
                                    .setThreatTypeId("crs_123")
                                    .setSeverity(WebAppSeverity.WEB_APP_SEVERITY_MEDIUM)
                                    .build()))
                    .build())
            .build();

    when(webAppProtectionRulesProvider.getWebAppRulesChangeLog(any(), any()))
        .thenReturn(webAppRulesChangeLog);
    when(webAppProtectionRulesProvider.getWebAppVersionedRules(any()))
        .thenReturn(List.of(webAppVersionedRule));

    ChangeLog result =
        ruleVersionManager.getChangeLogDoc(
            RuleType.RULE_TYPE_WEB_APPLICATION, currentVersion, previousVersion);

    assertNotNull(result);
    assertEquals(RuleType.RULE_TYPE_WEB_APPLICATION, result.getRuleType());
    assertEquals(currentVersion, result.getCurrentVersion());
    assertEquals(previousVersion, result.getPreviousVersion());
    assertEquals("Added new XSS rules", result.getHighlights().getValues(0));
    assertEquals(2, result.getAddedRulesTable().getRowsList().size());
    assertEquals(1, result.getRemovedRulesTable().getRowsList().size());
    assertEquals(1, result.getUpdatedRulesTable().getRowsList().size());
    verify(webAppProtectionRulesProvider)
        .getWebAppRulesChangeLog(
            argThat(version -> version.getVersion().equals(previousVersion.getVersion())),
            argThat(version -> version.getVersion().equals(currentVersion.getVersion())));
  }

  @Test
  void testGetRulesChangeLogApiProtect() {
    RuleType ruleType = RuleType.RULE_TYPE_API_PROTECTION;
    RuleVersion currentVersion =
        RuleVersion.newBuilder()
            .setVersion("3.0.0")
            .setVersionType(RuleVersionType.RULE_VERSION_TYPE_BETA)
            .build();
    RuleVersion previousVersion =
        RuleVersion.newBuilder()
            .setVersion("2.5.0")
            .setVersionType(RuleVersionType.RULE_VERSION_TYPE_STABLE)
            .build();

    ApiProtectRulesChangeLog.Builder apiProtectRulesChangeLogBuilder =
        ApiProtectRulesChangeLog.newBuilder();
    ApiProtectThreatTypeChange threatTypeAddition =
        ApiProtectThreatTypeChange.newBuilder()
            .setThreatTypeIdsAdded(
                ai.traceable.protection.rules.apiprotect.v1.StringList.newBuilder()
                    .addAllValues(Arrays.asList("api_942", "api_941"))
                    .build())
            .build();
    apiProtectRulesChangeLogBuilder.addThreatTypeChanges(threatTypeAddition);
    ApiProtectThreatTypeChange threatTypeRemoval =
        ApiProtectThreatTypeChange.newBuilder()
            .setThreatTypeIdsRemoved(
                ai.traceable.protection.rules.apiprotect.v1.StringList.newBuilder()
                    .addAllValues(Arrays.asList("api_920", "api_921"))
                    .build())
            .build();
    apiProtectRulesChangeLogBuilder.addThreatTypeChanges(threatTypeRemoval);
    ApiProtectThreatTypeUpdateDetails threatTypeUpdateDetails =
        ApiProtectThreatTypeUpdateDetails.newBuilder()
            .setThreatTypeId("api_930")
            .addUpdates(
                ApiProtectThreatTypeUpdateDetails.ThreatTypeUpdate.newBuilder()
                    .setNameUpdated(
                        ai.traceable.protection.rules.apiprotect.v1.StringValueUpdate.newBuilder()
                            .setOldValue("Old API Name")
                            .setNewValue("New API Name")
                            .build())
                    .build())
            .build();
    ApiProtectThreatTypeChange threatTypeUpdate =
        ApiProtectThreatTypeChange.newBuilder()
            .setThreatTypeUpdated(threatTypeUpdateDetails)
            .build();
    apiProtectRulesChangeLogBuilder.addThreatTypeChanges(threatTypeUpdate);
    List<String> addedRules = Arrays.asList("api_9421", "api_9422", "api_9423");
    ApiProtectThreatRuleChange ruleAdditions =
        ApiProtectThreatRuleChange.newBuilder()
            .setRuleIdsAdded(
                ai.traceable.protection.rules.apiprotect.v1.StringList.newBuilder()
                    .addAllValues(addedRules)
                    .build())
            .build();
    apiProtectRulesChangeLogBuilder.addRuleChanges(ruleAdditions);
    List<String> removedRules = Arrays.asList("api_9320", "api_9321");
    ApiProtectThreatRuleChange ruleRemovals =
        ApiProtectThreatRuleChange.newBuilder()
            .setRuleIdsRemoved(
                ai.traceable.protection.rules.apiprotect.v1.StringList.newBuilder()
                    .addAllValues(removedRules)
                    .build())
            .build();
    apiProtectRulesChangeLogBuilder.addRuleChanges(ruleRemovals);
    ApiProtectThreatRuleUpdateDetails ruleUpdateDetails =
        ApiProtectThreatRuleUpdateDetails.newBuilder()
            .setRuleId("api_9424")
            .addUpdates(
                ApiProtectThreatRuleUpdateDetails.ThreatRuleUpdate.newBuilder()
                    .setNameUpdated(
                        ai.traceable.protection.rules.apiprotect.v1.StringValueUpdate.newBuilder()
                            .setOldValue("Old API Rule")
                            .setNewValue("New API Rule")
                            .build())
                    .build())
            .build();
    ApiProtectThreatRuleChange ruleUpdate =
        ApiProtectThreatRuleChange.newBuilder().setRuleUpdated(ruleUpdateDetails).build();
    apiProtectRulesChangeLogBuilder.addRuleChanges(ruleUpdate);

    when(apiProtectionRulesProvider.getApiProtectRulesChangeLog(
            any(ApiProtectRulesVersion.class), any(ApiProtectRulesVersion.class)))
        .thenReturn(apiProtectRulesChangeLogBuilder.build());

    RulesChangeLog result =
        ruleVersionManager.getRulesChangeLog(ruleType, currentVersion, previousVersion);

    assertNotNull(result);

    assertEquals("2.5.0", result.getVersionUpdated().getOldVersion().getVersion());
    assertEquals(
        RuleVersionType.RULE_VERSION_TYPE_STABLE,
        result.getVersionUpdated().getOldVersion().getVersionType());
    assertEquals("3.0.0", result.getVersionUpdated().getNewVersion().getVersion());
    assertEquals(
        RuleVersionType.RULE_VERSION_TYPE_BETA,
        result.getVersionUpdated().getNewVersion().getVersionType());
    assertEquals(3, result.getThreatTypeChangesCount());

    boolean foundThreatTypeAdditions = false;
    boolean foundThreatTypeRemovals = false;
    boolean foundThreatTypeUpdates = false;

    for (ThreatTypeChange change : result.getThreatTypeChangesList()) {
      if (change.hasThreatTypeIdsAdded()) {
        foundThreatTypeAdditions = true;
        assertEquals(2, change.getThreatTypeIdsAdded().getValuesCount());
        assertTrue(change.getThreatTypeIdsAdded().getValuesList().contains("api_942"));
        assertTrue(change.getThreatTypeIdsAdded().getValuesList().contains("api_941"));
      } else if (change.hasThreatTypeIdsRemoved()) {
        foundThreatTypeRemovals = true;
        assertEquals(2, change.getThreatTypeIdsRemoved().getValuesCount());
        assertTrue(change.getThreatTypeIdsRemoved().getValuesList().contains("api_920"));
        assertTrue(change.getThreatTypeIdsRemoved().getValuesList().contains("api_921"));
      } else if (change.hasThreatTypeUpdated()) {
        foundThreatTypeUpdates = true;
        assertEquals("api_930", change.getThreatTypeUpdated().getThreatTypeId());
        assertEquals(1, change.getThreatTypeUpdated().getUpdatesCount());
        assertTrue(change.getThreatTypeUpdated().getUpdates(0).hasNameUpdated());
        assertEquals(
            "Old API Name",
            change.getThreatTypeUpdated().getUpdates(0).getNameUpdated().getOldValue());
        assertEquals(
            "New API Name",
            change.getThreatTypeUpdated().getUpdates(0).getNameUpdated().getNewValue());
      }
    }

    assertTrue(foundThreatTypeAdditions, "Did not find threat type additions");
    assertTrue(foundThreatTypeRemovals, "Did not find threat type removals");
    assertTrue(foundThreatTypeUpdates, "Did not find threat type updates");
    assertEquals(3, result.getRuleChangesCount());

    boolean foundRuleAdditions = false;
    boolean foundRuleRemovals = false;
    boolean foundRuleUpdate = false;

    for (ThreatRuleChange change : result.getRuleChangesList()) {
      if (change.hasRuleIdsAdded()) {
        foundRuleAdditions = true;
        assertEquals(3, change.getRuleIdsAdded().getValuesCount());
        assertTrue(change.getRuleIdsAdded().getValuesList().contains("api_9421"));
        assertTrue(change.getRuleIdsAdded().getValuesList().contains("api_9422"));
        assertTrue(change.getRuleIdsAdded().getValuesList().contains("api_9423"));
      } else if (change.hasRuleIdsRemoved()) {
        foundRuleRemovals = true;
        assertEquals(2, change.getRuleIdsRemoved().getValuesCount());
        assertTrue(change.getRuleIdsRemoved().getValuesList().contains("api_9320"));
        assertTrue(change.getRuleIdsRemoved().getValuesList().contains("api_9321"));
      } else if (change.hasRuleUpdated()) {
        foundRuleUpdate = true;
        ThreatRuleUpdateDetails updateDetails = change.getRuleUpdated();
        assertEquals("api_9424", updateDetails.getRuleId());
        assertEquals(1, updateDetails.getUpdatesCount());
        assertTrue(updateDetails.getUpdates(0).hasNameUpdated());
        assertEquals("Old API Rule", updateDetails.getUpdates(0).getNameUpdated().getOldValue());
        assertEquals("New API Rule", updateDetails.getUpdates(0).getNameUpdated().getNewValue());
      }
    }

    assertTrue(foundRuleAdditions, "Did not find rule additions");
    assertTrue(foundRuleRemovals, "Did not find rule removals");
    assertTrue(foundRuleUpdate, "Did not find rule update");
  }
}
