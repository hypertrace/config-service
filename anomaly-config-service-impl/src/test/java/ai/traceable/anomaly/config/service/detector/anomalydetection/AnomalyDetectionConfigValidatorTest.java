package ai.traceable.anomaly.config.service.detector.anomalydetection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.anomaly.config.service.common.AnomalyConfigValidator;
import ai.traceable.anomaly.config.service.global.ruleinfo.RuleInfoManager;
import ai.traceable.anomaly.config.service.global.status.GlobalAnomalyConfigStatusManager;
import ai.traceable.anomaly.config.service.registry.apidef.ApiDefinitionRegistryImpl;
import ai.traceable.anomaly.config.service.registry.common.ConfigConverter;
import ai.traceable.anomaly.config.service.registry.modsec.ModsecCrsRulesHandler;
import ai.traceable.anomaly.config.service.registry.modsec.ModsecRulesRegistryImpl;
import ai.traceable.anomaly.config.service.registry.session.SessionRulesRegistryImpl;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleAction;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleType;
import ai.traceable.anomaly.config.service.v1.StringList;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.ApiDefinitionMetadataAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ApiStateBasedAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.BlockingMetadataAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ContentSizeAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.CustomIpAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.CustomRegionAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.CustomRulesAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.CustomRulesAnomalyDetectionConfig.EmailDomainAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.CustomRulesAnomalyDetectionConfig.IpTypeAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.CustomRulesAnomalyDetectionConfig.MaliciousSourcesRulesAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.DeleteAnomalyConfigOption;
import ai.traceable.anomaly.config.service.v1.detector.DeleteScopedAnomalyDetectionConfigRequest;
import ai.traceable.anomaly.config.service.v1.detector.GetAnomalyDetectionConfigsFilter;
import ai.traceable.anomaly.config.service.v1.detector.GetScopedAnomalyDetectionConfigRequest;
import ai.traceable.anomaly.config.service.v1.detector.GetUnresolvedScopedAnomalyDetectionConfigRequest;
import ai.traceable.anomaly.config.service.v1.detector.IntegerAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.LearntApiAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.MissingParamAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAllDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAnomalyRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.MultiValuedStringParamRule;
import ai.traceable.anomaly.config.service.v1.detector.MultiValuedStringParamRulesList;
import ai.traceable.anomaly.config.service.v1.detector.ObjectBolaAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.SessionDefinitionMetadataAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.UnderDiscoveryApiAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.UnderThresholdLearningApiAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.UnknownParamAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.UpdateScopedAnomalyDetectionConfigRequest;
import ai.traceable.anomaly.config.service.v1.detector.UserIdBolaAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.global.ScopedAnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import ai.traceable.modsecurity.utils.ModsecRuleUtils;
import io.grpc.Status;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AnomalyDetectionConfigValidatorTest {

  private final AnomalyConfigValidator anomalyConfigValidator = new AnomalyConfigValidator();
  private final AnomalyDetectionConfigRegexValidator anomalyDetectionConfigRegexValidator =
      new AnomalyDetectionConfigRegexValidator();
  private final ConfigConverter configConverter = new ConfigConverter();
  private final ModsecRuleUtils modsecRuleUtils = new ModsecRuleUtils();
  private final ModsecCrsRulesHandler modsecCrsRulesHandler =
      new ModsecCrsRulesHandler(modsecRuleUtils);
  private RuleInfoManager ruleInfoManager;
  private GlobalAnomalyConfigStatusManager globalAnomalyConfigStatusManager;
  private AnomalyDetectionConfigManager anomalyDetectionConfigManager;
  private AnomalyDetectionConfigValidator validator;
  private final AnomalyConfigScope configScope =
      AnomalyConfigScope.newBuilder()
          .setCustomerScope(AnomalyCustomerScope.getDefaultInstance())
          .build();

  @BeforeEach
  void setup() {
    ruleInfoManager = mock(RuleInfoManager.class);
    globalAnomalyConfigStatusManager = mock(GlobalAnomalyConfigStatusManager.class);
    anomalyDetectionConfigManager = mock(AnomalyDetectionConfigManager.class);
    when(globalAnomalyConfigStatusManager.getScopedAnomalyConfigStatus(any(), any()))
        .thenReturn(ScopedAnomalyConfigStatus.newBuilder().build());
    when(anomalyDetectionConfigManager.getScopedAnomalyDetectionConfig(any(), any(), any()))
        .thenReturn(ScopedAnomalyDetectionConfig.newBuilder().build());
    when(ruleInfoManager.getModsecAnomalyRuleInfo(any(), any(), any(), anyBoolean()))
        .thenReturn(Collections.emptyList());
    validator =
        new AnomalyDetectionConfigValidator(
            anomalyConfigValidator,
            new ModsecConfigValidator(
                ruleInfoManager,
                globalAnomalyConfigStatusManager,
                anomalyDetectionConfigManager,
                ModsecRuleVersion.MODSEC_RULE_VERSION_CORAZA_V3),
            anomalyDetectionConfigRegexValidator,
            new ApiDefinitionRegistryImpl(configConverter),
            new SessionRulesRegistryImpl(configConverter),
            new ModsecRulesRegistryImpl(configConverter, modsecCrsRulesHandler));
  }

  @Test
  void testGetRequest() {
    Status status = validator.validate(GetScopedAnomalyDetectionConfigRequest.getDefaultInstance());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("valid config scope"));

    status =
        validator.validate(
            GetScopedAnomalyDetectionConfigRequest.newBuilder()
                .setConfigScope(configScope)
                .build());
    assertEquals(Status.OK.getCode(), status.getCode());
  }

  @Test
  void testGetUnresolvedRequest() {
    Status status =
        validator.validate(GetUnresolvedScopedAnomalyDetectionConfigRequest.getDefaultInstance());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("valid config scope"));

    status =
        validator.validate(
            GetUnresolvedScopedAnomalyDetectionConfigRequest.newBuilder()
                .setConfigScope(configScope)
                .build());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("valid config filter"));

    status =
        validator.validate(
            GetUnresolvedScopedAnomalyDetectionConfigRequest.newBuilder()
                .setConfigScope(configScope)
                .setFilter(GetAnomalyDetectionConfigsFilter.getDefaultInstance())
                .build());
    assertEquals(Status.OK.getCode(), status.getCode());
  }

  @Test
  void testDeleteRequest() {
    Status status =
        validator.validate(DeleteScopedAnomalyDetectionConfigRequest.getDefaultInstance());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("valid config scope"));

    ScopedAnomalyDetectionConfig scopedAnomalyDetectionConfig =
        ScopedAnomalyDetectionConfig.newBuilder()
            .setConfigScope(configScope)
            .addAnomalyDetectionConfigs(
                AnomalyDetectionConfig.newBuilder()
                    .setApiDefinitionMetadataAnomalyDetectionConfig(
                        ApiDefinitionMetadataAnomalyDetectionConfig.newBuilder()
                            .setInteger(IntegerAnomalyConfig.getDefaultInstance())
                            .build())
                    .build())
            .build();
    status =
        validator.validate(
            DeleteScopedAnomalyDetectionConfigRequest.newBuilder()
                .setScopedAnomalyDetectionConfig(scopedAnomalyDetectionConfig)
                .build());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("valid delete option"));
    status =
        validator.validate(
            DeleteScopedAnomalyDetectionConfigRequest.newBuilder()
                .setDeleteAnomalyConfigOption(
                    DeleteAnomalyConfigOption.DELETE_ANOMALY_CONFIG_OPTION_WHOLE_DETECTION_CONFIG)
                .setScopedAnomalyDetectionConfig(scopedAnomalyDetectionConfig)
                .build());
    assertEquals(Status.OK.getCode(), status.getCode());
  }

  @Test
  void testModsecUpdateValidation() {
    UpdateScopedAnomalyDetectionConfigRequest updateRequest;
    AnomalyDetectionConfig anomalyDetectionConfig1, anomalyDetectionConfig2;
    AnomalySubRuleConfig subRuleConfig1, subRuleConfig2;

    Status status =
        validator.validate(
            UpdateScopedAnomalyDetectionConfigRequest.getDefaultInstance(),
            mock(RequestContext.class));
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("valid config scope"));

    anomalyDetectionConfig1 =
        AnomalyDetectionConfig.newBuilder()
            .setModsecurityAnomalyDetectionConfig(
                ModsecurityAnomalyDetectionConfig.newBuilder()
                    .setModsecAnomalyRule(
                        ModsecurityAnomalyRuleConfig.newBuilder()
                            .setAnomalyRuleId("crs_913")
                            .build()))
            .build();
    anomalyDetectionConfig2 =
        AnomalyDetectionConfig.newBuilder()
            .setModsecurityAnomalyDetectionConfig(
                ModsecurityAnomalyDetectionConfig.newBuilder()
                    .setModsecAnomalyRule(
                        ModsecurityAnomalyRuleConfig.newBuilder()
                            .setAnomalyRuleId("crs_913")
                            .build()))
            .build();
    updateRequest =
        UpdateScopedAnomalyDetectionConfigRequest.newBuilder()
            .setScopedAnomalyDetectionConfig(
                ScopedAnomalyDetectionConfig.newBuilder()
                    .setConfigScope(configScope)
                    .addAllAnomalyDetectionConfigs(
                        List.of(anomalyDetectionConfig1, anomalyDetectionConfig2))
                    .build())
            .build();

    status = validator.validate(updateRequest, mock(RequestContext.class));

    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(
        status
            .getDescription()
            .contains("should have only one modsec config with ruleId: crs_913"));

    subRuleConfig1 = AnomalySubRuleConfig.newBuilder().setSubRuleId("crs_913100").build();

    anomalyDetectionConfig1 =
        AnomalyDetectionConfig.newBuilder()
            .setModsecurityAnomalyDetectionConfig(
                ModsecurityAnomalyDetectionConfig.newBuilder()
                    .setModsecAnomalyRule(
                        ModsecurityAnomalyRuleConfig.newBuilder()
                            .setAnomalyRuleId("crs_913")
                            .addAllSubRuleConfigs(List.of(subRuleConfig1, subRuleConfig1))
                            .build()))
            .build();
    updateRequest =
        UpdateScopedAnomalyDetectionConfigRequest.newBuilder()
            .setScopedAnomalyDetectionConfig(
                ScopedAnomalyDetectionConfig.newBuilder()
                    .setConfigScope(configScope)
                    .addAnomalyDetectionConfigs(anomalyDetectionConfig1)
                    .build())
            .build();

    status = validator.validate(updateRequest, mock(RequestContext.class));

    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(
        status
            .getDescription()
            .contains(
                "should have only one subRule config with subRuleId: crs_913100 in modsec config with ruleId: crs_913"));

    AnomalyDetectionConfig modsecAllDetectionConfig =
        AnomalyDetectionConfig.newBuilder()
            .setModsecurityAnomalyDetectionConfig(
                ModsecurityAnomalyDetectionConfig.newBuilder()
                    .setModsecAllDetection(ModsecurityAllDetectionConfig.getDefaultInstance()))
            .build();

    updateRequest =
        UpdateScopedAnomalyDetectionConfigRequest.newBuilder()
            .setScopedAnomalyDetectionConfig(
                ScopedAnomalyDetectionConfig.newBuilder()
                    .setConfigScope(configScope)
                    .addAllAnomalyDetectionConfigs(
                        List.of(modsecAllDetectionConfig, modsecAllDetectionConfig))
                    .build())
            .build();

    status = validator.validate(updateRequest, mock(RequestContext.class));

    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(
        status
            .getDescription()
            .contains(
                "UpdateScopedAnomalyDetectionConfigRequest should have only one modsecAllDetectionConfig"));

    subRuleConfig1 =
        AnomalySubRuleConfig.newBuilder()
            .setAnomalyRuleAction(AnomalyRuleAction.ANOMALY_RULE_ACTION_BLOCK)
            .setSubRuleId("crs_913100")
            .build();
    anomalyDetectionConfig1 =
        AnomalyDetectionConfig.newBuilder()
            .setModsecurityAnomalyDetectionConfig(
                ModsecurityAnomalyDetectionConfig.newBuilder()
                    .setModsecAnomalyRule(
                        ModsecurityAnomalyRuleConfig.newBuilder()
                            .setAnomalyRuleId("crs_913")
                            .addAllSubRuleConfigs(List.of(subRuleConfig1))
                            .build()))
            .build();
    updateRequest =
        UpdateScopedAnomalyDetectionConfigRequest.newBuilder()
            .setScopedAnomalyDetectionConfig(
                ScopedAnomalyDetectionConfig.newBuilder()
                    .setConfigScope(configScope)
                    .addAnomalyDetectionConfigs(anomalyDetectionConfig1)
                    .build())
            .build();

    when(ruleInfoManager.getModsecAnomalyRuleInfo(any(), any(), any(), anyBoolean()))
        .thenReturn(
            List.of(
                AnomalyRuleInfo.newBuilder()
                    .setRuleId("crs_913")
                    .addSubRuleInfos(
                        AnomalySubRuleInfo.newBuilder()
                            .setRuleId("crs_913100")
                            .addSubRuleTypes(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_REGULAR)
                            .build())
                    .build()));

    status = validator.validate(updateRequest, mock(RequestContext.class));

    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(
        status
            .getDescription()
            .contains("crs_913100 is an aggressive rule which can't be blocked"));

    when(ruleInfoManager.getModsecAnomalyRuleInfo(any(), any(), any(), anyBoolean()))
        .thenReturn(List.of());
    anomalyDetectionConfig1 =
        AnomalyDetectionConfig.newBuilder()
            .setModsecurityAnomalyDetectionConfig(
                ModsecurityAnomalyDetectionConfig.newBuilder()
                    .setModsecAnomalyRule(
                        ModsecurityAnomalyRuleConfig.newBuilder()
                            .setAnomalyRuleId("crs_913")
                            .addSubRuleConfigs(subRuleConfig1)
                            .build()))
            .build();

    subRuleConfig2 = AnomalySubRuleConfig.newBuilder().setSubRuleId("crs_931100").build();
    anomalyDetectionConfig2 =
        AnomalyDetectionConfig.newBuilder()
            .setModsecurityAnomalyDetectionConfig(
                ModsecurityAnomalyDetectionConfig.newBuilder()
                    .setModsecAnomalyRule(
                        ModsecurityAnomalyRuleConfig.newBuilder()
                            .setAnomalyRuleId("crs_931")
                            .addSubRuleConfigs(subRuleConfig2)
                            .build()))
            .build();
    updateRequest =
        UpdateScopedAnomalyDetectionConfigRequest.newBuilder()
            .setScopedAnomalyDetectionConfig(
                ScopedAnomalyDetectionConfig.newBuilder()
                    .setConfigScope(configScope)
                    .addAllAnomalyDetectionConfigs(
                        List.of(anomalyDetectionConfig1, anomalyDetectionConfig2))
                    .build())
            .build();

    status = validator.validate(updateRequest, mock(RequestContext.class));

    assertEquals(Status.OK.getCode(), status.getCode());

    subRuleConfig1 =
        AnomalySubRuleConfig.newBuilder()
            .setSubRuleId("crs_9440900")
            .setBlockingEnabled(true)
            .build();

    anomalyDetectionConfig1 =
        AnomalyDetectionConfig.newBuilder()
            .setModsecurityAnomalyDetectionConfig(
                ModsecurityAnomalyDetectionConfig.newBuilder()
                    .setModsecAnomalyRule(
                        ModsecurityAnomalyRuleConfig.newBuilder()
                            .setAnomalyRuleId("crs_944")
                            .addSubRuleConfigs(subRuleConfig1)
                            .build()))
            .build();

    updateRequest = buildUpdateRequest(List.of(anomalyDetectionConfig1));
    status = validator.validate(updateRequest, mock(RequestContext.class));
    assertEquals(Status.OK.getCode(), status.getCode());
  }

  @Test
  void testStateBasedUpdateValidation() {
    UpdateScopedAnomalyDetectionConfigRequest request;

    AnomalyDetectionConfig detectionConfig1 =
        AnomalyDetectionConfig.newBuilder()
            .setApiStateBasedAnomalyDetectionConfig(
                ApiStateBasedAnomalyDetectionConfig.newBuilder()
                    .setLearntApi(LearntApiAnomalyConfig.newBuilder().build())
                    .build())
            .build();

    AnomalyDetectionConfig detectionConfig2 =
        AnomalyDetectionConfig.newBuilder()
            .setApiStateBasedAnomalyDetectionConfig(
                ApiStateBasedAnomalyDetectionConfig.newBuilder()
                    .setLearntApi(LearntApiAnomalyConfig.newBuilder().build())
                    .build())
            .build();

    request =
        UpdateScopedAnomalyDetectionConfigRequest.newBuilder()
            .setScopedAnomalyDetectionConfig(
                ScopedAnomalyDetectionConfig.newBuilder()
                    .setConfigScope(configScope)
                    .addAllAnomalyDetectionConfigs(List.of(detectionConfig1, detectionConfig2)))
            .build();

    Status status = validator.validate(request, mock(RequestContext.class));
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("Duplicate key LEARNT_API"));

    detectionConfig2 =
        AnomalyDetectionConfig.newBuilder()
            .setApiStateBasedAnomalyDetectionConfig(
                ApiStateBasedAnomalyDetectionConfig.newBuilder()
                    .setUnderDiscoveryApi(UnderDiscoveryApiAnomalyConfig.getDefaultInstance())
                    .build())
            .build();

    request =
        UpdateScopedAnomalyDetectionConfigRequest.newBuilder()
            .setScopedAnomalyDetectionConfig(
                ScopedAnomalyDetectionConfig.newBuilder()
                    .setConfigScope(configScope)
                    .addAllAnomalyDetectionConfigs(List.of(detectionConfig1, detectionConfig2)))
            .build();

    status = validator.validate(request, mock(RequestContext.class));

    assertEquals(Status.OK.getCode(), status.getCode());
  }

  @Test
  void testApiDefinitionUpdateValidation() {
    UpdateScopedAnomalyDetectionConfigRequest request;

    AnomalyDetectionConfig detectionConfig1 =
        AnomalyDetectionConfig.newBuilder()
            .setApiDefinitionMetadataAnomalyDetectionConfig(
                ApiDefinitionMetadataAnomalyDetectionConfig.newBuilder()
                    .setAnomalyRuleId("rule1")
                    .setInteger(IntegerAnomalyConfig.getDefaultInstance()))
            .build();

    request =
        UpdateScopedAnomalyDetectionConfigRequest.newBuilder()
            .setScopedAnomalyDetectionConfig(
                ScopedAnomalyDetectionConfig.newBuilder()
                    .setConfigScope(configScope)
                    .addAllAnomalyDetectionConfigs(List.of(detectionConfig1)))
            .build();

    Status status = validator.validate(request, mock(RequestContext.class));

    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(
        status.getDescription().contains("Invalid api definition detection config ruleId: rule1"));

    detectionConfig1 =
        AnomalyDetectionConfig.newBuilder()
            .setApiDefinitionMetadataAnomalyDetectionConfig(
                ApiDefinitionMetadataAnomalyDetectionConfig.newBuilder()
                    .setAnomalyRuleId("contentSize")
                    .setInteger(IntegerAnomalyConfig.getDefaultInstance()))
            .build();
    request =
        UpdateScopedAnomalyDetectionConfigRequest.newBuilder()
            .setScopedAnomalyDetectionConfig(
                ScopedAnomalyDetectionConfig.newBuilder()
                    .setConfigScope(configScope)
                    .addAllAnomalyDetectionConfigs(List.of(detectionConfig1)))
            .build();

    status = validator.validate(request, mock(RequestContext.class));
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(
        status
            .getDescription()
            .contains("Invalid api definition detection config type for ruleId: contentSize"));

    detectionConfig1 =
        AnomalyDetectionConfig.newBuilder()
            .setApiDefinitionMetadataAnomalyDetectionConfig(
                ApiDefinitionMetadataAnomalyDetectionConfig.newBuilder()
                    .setAnomalyRuleId("integer")
                    .setInteger(IntegerAnomalyConfig.getDefaultInstance()))
            .build();

    request =
        UpdateScopedAnomalyDetectionConfigRequest.newBuilder()
            .setScopedAnomalyDetectionConfig(
                ScopedAnomalyDetectionConfig.newBuilder()
                    .setConfigScope(configScope)
                    .addAllAnomalyDetectionConfigs(List.of(detectionConfig1, detectionConfig1)))
            .build();

    status = validator.validate(request, mock(RequestContext.class));

    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(
        status
            .getDescription()
            .contains("should have only one api definition detection config for ruleId: integer"));

    detectionConfig1 =
        AnomalyDetectionConfig.newBuilder()
            .setApiDefinitionMetadataAnomalyDetectionConfig(
                ApiDefinitionMetadataAnomalyDetectionConfig.newBuilder()
                    .setInteger(IntegerAnomalyConfig.getDefaultInstance()))
            .build();

    request =
        UpdateScopedAnomalyDetectionConfigRequest.newBuilder()
            .setScopedAnomalyDetectionConfig(
                ScopedAnomalyDetectionConfig.newBuilder()
                    .setConfigScope(configScope)
                    .addAllAnomalyDetectionConfigs(List.of(detectionConfig1, detectionConfig1)))
            .build();

    status = validator.validate(request, mock(RequestContext.class));

    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(
        status
            .getDescription()
            .contains(
                "should have only one api definition detection config for configCase: INTEGER"));

    AnomalyDetectionConfig detectionConfig2 =
        AnomalyDetectionConfig.newBuilder()
            .setApiDefinitionMetadataAnomalyDetectionConfig(
                ApiDefinitionMetadataAnomalyDetectionConfig.newBuilder()
                    .setAnomalyRuleId("contentSize")
                    .setContentSize(ContentSizeAnomalyConfig.getDefaultInstance()))
            .build();

    request =
        UpdateScopedAnomalyDetectionConfigRequest.newBuilder()
            .setScopedAnomalyDetectionConfig(
                ScopedAnomalyDetectionConfig.newBuilder()
                    .setConfigScope(configScope)
                    .addAllAnomalyDetectionConfigs(List.of(detectionConfig1, detectionConfig2)))
            .build();

    status = validator.validate(request, mock(RequestContext.class));

    assertEquals(Status.OK.getCode(), status.getCode());

    detectionConfig1 =
        AnomalyDetectionConfig.newBuilder()
            .setApiDefinitionMetadataAnomalyDetectionConfig(
                ApiDefinitionMetadataAnomalyDetectionConfig.getDefaultInstance())
            .build();

    request =
        UpdateScopedAnomalyDetectionConfigRequest.newBuilder()
            .setScopedAnomalyDetectionConfig(
                ScopedAnomalyDetectionConfig.newBuilder()
                    .setConfigScope(configScope)
                    .addAllAnomalyDetectionConfigs(List.of(detectionConfig1)))
            .build();
    status = validator.validate(request, mock(RequestContext.class));

    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("Invalid api definition detection config"));
  }

  @Test
  void testBlockingUpdateValidation() {
    UpdateScopedAnomalyDetectionConfigRequest request;

    AnomalyDetectionConfig detectionConfig1 =
        AnomalyDetectionConfig.newBuilder()
            .setBlockingMetadataAnomalyDetectionConfig(
                BlockingMetadataAnomalyDetectionConfig.newBuilder()
                    .setCustomIp(CustomIpAnomalyConfig.getDefaultInstance())
                    .build())
            .build();

    request =
        UpdateScopedAnomalyDetectionConfigRequest.newBuilder()
            .setScopedAnomalyDetectionConfig(
                ScopedAnomalyDetectionConfig.newBuilder()
                    .setConfigScope(configScope)
                    .addAllAnomalyDetectionConfigs(List.of(detectionConfig1, detectionConfig1)))
            .build();

    Status status = validator.validate(request, mock(RequestContext.class));
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("Duplicate key CUSTOM_IP"));

    AnomalyDetectionConfig detectionConfig2 =
        AnomalyDetectionConfig.newBuilder()
            .setBlockingMetadataAnomalyDetectionConfig(
                BlockingMetadataAnomalyDetectionConfig.newBuilder()
                    .setCustomRegion(CustomRegionAnomalyConfig.getDefaultInstance())
                    .build())
            .build();

    request =
        UpdateScopedAnomalyDetectionConfigRequest.newBuilder()
            .setScopedAnomalyDetectionConfig(
                ScopedAnomalyDetectionConfig.newBuilder()
                    .setConfigScope(configScope)
                    .addAllAnomalyDetectionConfigs(List.of(detectionConfig1, detectionConfig2)))
            .build();

    status = validator.validate(request, mock(RequestContext.class));

    assertEquals(Status.OK.getCode(), status.getCode());
  }

  @Test
  void testSessionDefinitionMetadataUpdateValidation() {
    UpdateScopedAnomalyDetectionConfigRequest request;

    AnomalyDetectionConfig detectionConfig1 =
        AnomalyDetectionConfig.newBuilder()
            .setSessionDefinitionMetadataAnomalyDetectionConfig(
                SessionDefinitionMetadataAnomalyDetectionConfig.newBuilder()
                    .setAnomalyRuleId("rule1")
                    .setObjectBola(ObjectBolaAnomalyConfig.getDefaultInstance()))
            .build();

    request =
        UpdateScopedAnomalyDetectionConfigRequest.newBuilder()
            .setScopedAnomalyDetectionConfig(
                ScopedAnomalyDetectionConfig.newBuilder()
                    .setConfigScope(configScope)
                    .addAllAnomalyDetectionConfigs(List.of(detectionConfig1)))
            .build();

    Status status = validator.validate(request, mock(RequestContext.class));

    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(
        status
            .getDescription()
            .contains("Invalid session definition metadata detection config ruleId: rule1"));

    detectionConfig1 =
        AnomalyDetectionConfig.newBuilder()
            .setSessionDefinitionMetadataAnomalyDetectionConfig(
                SessionDefinitionMetadataAnomalyDetectionConfig.newBuilder()
                    .setObjectBola(ObjectBolaAnomalyConfig.getDefaultInstance()))
            .build();

    request =
        UpdateScopedAnomalyDetectionConfigRequest.newBuilder()
            .setScopedAnomalyDetectionConfig(
                ScopedAnomalyDetectionConfig.newBuilder()
                    .setConfigScope(configScope)
                    .addAllAnomalyDetectionConfigs(List.of(detectionConfig1, detectionConfig1)))
            .build();

    status = validator.validate(request, mock(RequestContext.class));

    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    System.out.println(status.getDescription());
    assertTrue(
        status
            .getDescription()
            .contains(
                "should have only one session definition metadata detection config for configCase: OBJECT_BOLA"));

    AnomalyDetectionConfig detectionConfig2 =
        AnomalyDetectionConfig.newBuilder()
            .setSessionDefinitionMetadataAnomalyDetectionConfig(
                SessionDefinitionMetadataAnomalyDetectionConfig.newBuilder()
                    .setAnomalyRuleId("userIdBola")
                    .setUserIdBola(UserIdBolaAnomalyConfig.getDefaultInstance()))
            .build();

    request =
        UpdateScopedAnomalyDetectionConfigRequest.newBuilder()
            .setScopedAnomalyDetectionConfig(
                ScopedAnomalyDetectionConfig.newBuilder()
                    .setConfigScope(configScope)
                    .addAllAnomalyDetectionConfigs(List.of(detectionConfig1, detectionConfig2)))
            .build();

    status = validator.validate(request, mock(RequestContext.class));

    assertEquals(Status.OK.getCode(), status.getCode());

    detectionConfig1 =
        AnomalyDetectionConfig.newBuilder()
            .setSessionDefinitionMetadataAnomalyDetectionConfig(
                SessionDefinitionMetadataAnomalyDetectionConfig.getDefaultInstance())
            .build();

    request =
        UpdateScopedAnomalyDetectionConfigRequest.newBuilder()
            .setScopedAnomalyDetectionConfig(
                ScopedAnomalyDetectionConfig.newBuilder()
                    .setConfigScope(configScope)
                    .addAllAnomalyDetectionConfigs(List.of(detectionConfig1)))
            .build();
    status = validator.validate(request, mock(RequestContext.class));

    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(
        status.getDescription().contains("Invalid session definition metadata detection config"));
  }

  @Test
  void testCustomRulesUpdateValidation() {
    AnomalyDetectionConfig detectionConfig =
        AnomalyDetectionConfig.newBuilder()
            .setCustomRulesAnomalyDetectionConfig(
                CustomRulesAnomalyDetectionConfig.newBuilder()
                    .setMaliciousSources(
                        MaliciousSourcesRulesAnomalyConfig.newBuilder()
                            .setEmailDomain(EmailDomainAnomalyConfig.newBuilder().setDisabled(true))
                            .setIpType(
                                IpTypeAnomalyConfig.newBuilder()
                                    .setAbuseVelocityMinThresholdValue(90))))
            .build();

    UpdateScopedAnomalyDetectionConfigRequest request =
        UpdateScopedAnomalyDetectionConfigRequest.newBuilder()
            .setScopedAnomalyDetectionConfig(
                ScopedAnomalyDetectionConfig.newBuilder()
                    .setConfigScope(configScope)
                    .addAllAnomalyDetectionConfigs(List.of(detectionConfig)))
            .build();

    assertEquals(
        Status.OK.getCode(), validator.validate(request, mock(RequestContext.class)).getCode());

    detectionConfig =
        AnomalyDetectionConfig.newBuilder()
            .setCustomRulesAnomalyDetectionConfig(
                CustomRulesAnomalyDetectionConfig.newBuilder()
                    .setMaliciousSources(
                        MaliciousSourcesRulesAnomalyConfig.newBuilder()
                            .setEmailDomain(
                                EmailDomainAnomalyConfig.newBuilder()
                                    .setDisabled(true)
                                    .setHighEmailFraudScoreMinThreshold(50)
                                    .setCriticalEmailFraudScoreMinThreshold(70))
                            .setIpType(
                                IpTypeAnomalyConfig.newBuilder()
                                    .setAbuseVelocityMinThresholdValue(90))))
            .build();

    request =
        UpdateScopedAnomalyDetectionConfigRequest.newBuilder()
            .setScopedAnomalyDetectionConfig(
                ScopedAnomalyDetectionConfig.newBuilder()
                    .setConfigScope(configScope)
                    .addAllAnomalyDetectionConfigs(List.of(detectionConfig)))
            .build();

    assertEquals(
        Status.OK.getCode(), validator.validate(request, mock(RequestContext.class)).getCode());

    detectionConfig =
        AnomalyDetectionConfig.newBuilder()
            .setCustomRulesAnomalyDetectionConfig(
                CustomRulesAnomalyDetectionConfig.newBuilder()
                    .setMaliciousSources(
                        MaliciousSourcesRulesAnomalyConfig.newBuilder()
                            .setEmailDomain(
                                EmailDomainAnomalyConfig.newBuilder()
                                    .setDisabled(true)
                                    .setHighEmailFraudScoreMinThreshold(70)
                                    .setCriticalEmailFraudScoreMinThreshold(50))
                            .setIpType(
                                IpTypeAnomalyConfig.newBuilder()
                                    .setAbuseVelocityMinThresholdValue(90))))
            .build();

    request =
        UpdateScopedAnomalyDetectionConfigRequest.newBuilder()
            .setScopedAnomalyDetectionConfig(
                ScopedAnomalyDetectionConfig.newBuilder()
                    .setConfigScope(configScope)
                    .addAllAnomalyDetectionConfigs(List.of(detectionConfig)))
            .build();

    Status status = validator.validate(request, mock(RequestContext.class));
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals(
        "Invalid EmailDomainAnomalyConfig: highEmailFraudScoreMinThreshold > criticalEmailFraudScoreMinThreshold",
        status.getDescription());
  }

  @Test
  void testApiStateBasedAnomalyDetectionConfigRegexValidation() {
    UpdateScopedAnomalyDetectionConfigRequest request;

    AnomalyDetectionConfig detectionConfig =
        AnomalyDetectionConfig.newBuilder()
            .setApiStateBasedAnomalyDetectionConfig(
                ApiStateBasedAnomalyDetectionConfig.newBuilder()
                    .setUnderThresholdLearningApi(
                        UnderThresholdLearningApiAnomalyConfig.newBuilder()
                            .setRejectUrlRegexStrings(
                                StringList.newBuilder().addAllValues(List.of("[")).build())
                            .build())
                    .build())
            .build();
    request =
        UpdateScopedAnomalyDetectionConfigRequest.newBuilder()
            .setScopedAnomalyDetectionConfig(
                ScopedAnomalyDetectionConfig.newBuilder()
                    .setConfigScope(configScope)
                    .addAllAnomalyDetectionConfigs(List.of(detectionConfig)))
            .build();
    Status status = validator.validate(request, mock(RequestContext.class));
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("Invalid Regex pattern: ["));

    AnomalyDetectionConfig detectionConfig1 =
        AnomalyDetectionConfig.newBuilder()
            .setApiStateBasedAnomalyDetectionConfig(
                ApiStateBasedAnomalyDetectionConfig.newBuilder()
                    .setUnderThresholdLearningApi(
                        UnderThresholdLearningApiAnomalyConfig.newBuilder()
                            .setRejectUrlRegexStrings(
                                StringList.newBuilder()
                                    .addAllValues(
                                        List.of(
                                            "/^[(]{0,1}[0-9]{3}[)]{0,1}[-\\s\\.]{0,1}[0-9]{3}[-\\s\\.]{0,1}[0-9]{4}$/\n"))
                                    .build())
                            .build())
                    .build())
            .build();
    request =
        UpdateScopedAnomalyDetectionConfigRequest.newBuilder()
            .setScopedAnomalyDetectionConfig(
                ScopedAnomalyDetectionConfig.newBuilder()
                    .setConfigScope(configScope)
                    .addAllAnomalyDetectionConfigs(List.of(detectionConfig1)))
            .build();
    status = validator.validate(request, mock(RequestContext.class));
    assertEquals(Status.OK.getCode(), status.getCode());

    AnomalyDetectionConfig detectionConfig2 =
        AnomalyDetectionConfig.newBuilder()
            .setApiStateBasedAnomalyDetectionConfig(
                ApiStateBasedAnomalyDetectionConfig.newBuilder()
                    .setUnderDiscoveryApi(
                        UnderDiscoveryApiAnomalyConfig.newBuilder()
                            .setRejectUrlRegexStrings(
                                StringList.newBuilder().addAllValues(List.of("[")).build())
                            .build())
                    .build())
            .build();
    request =
        UpdateScopedAnomalyDetectionConfigRequest.newBuilder()
            .setScopedAnomalyDetectionConfig(
                ScopedAnomalyDetectionConfig.newBuilder()
                    .setConfigScope(configScope)
                    .addAllAnomalyDetectionConfigs(List.of(detectionConfig2)))
            .build();
    status = validator.validate(request, mock(RequestContext.class));
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("Invalid Regex pattern: ["));

    AnomalyDetectionConfig detectionConfig3 =
        AnomalyDetectionConfig.newBuilder()
            .setApiStateBasedAnomalyDetectionConfig(
                ApiStateBasedAnomalyDetectionConfig.newBuilder()
                    .setUnderDiscoveryApi(
                        UnderDiscoveryApiAnomalyConfig.newBuilder()
                            .setRejectUrlRegexStrings(
                                StringList.newBuilder()
                                    .addAllValues(
                                        List.of(
                                            "/^[(]{0,1}[0-9]{3}[)]{0,1}[-\\s\\.]{0,1}[0-9]{3}[-\\s\\.]{0,1}[0-9]{4}$/\n"))
                                    .build())
                            .build())
                    .build())
            .build();
    request =
        UpdateScopedAnomalyDetectionConfigRequest.newBuilder()
            .setScopedAnomalyDetectionConfig(
                ScopedAnomalyDetectionConfig.newBuilder()
                    .setConfigScope(configScope)
                    .addAllAnomalyDetectionConfigs(List.of(detectionConfig3)))
            .build();
    status = validator.validate(request, mock(RequestContext.class));
    assertEquals(Status.OK.getCode(), status.getCode());
  }

  @Test
  void testApiDefinitionRegexValidation() {

    UpdateScopedAnomalyDetectionConfigRequest request;

    AnomalyDetectionConfig detectionConfig =
        AnomalyDetectionConfig.newBuilder()
            .setApiDefinitionMetadataAnomalyDetectionConfig(
                ApiDefinitionMetadataAnomalyDetectionConfig.newBuilder()
                    .setMissingParam(
                        MissingParamAnomalyConfig.newBuilder()
                            .setSevereRegexStrings(
                                StringList.newBuilder().addAllValues(List.of("[")).build())
                            .setAuthRegexStrings(
                                StringList.newBuilder().addAllValues(List.of("[")).build())
                            .build()))
            .build();

    request =
        UpdateScopedAnomalyDetectionConfigRequest.newBuilder()
            .setScopedAnomalyDetectionConfig(
                ScopedAnomalyDetectionConfig.newBuilder()
                    .setConfigScope(configScope)
                    .addAllAnomalyDetectionConfigs(List.of(detectionConfig)))
            .build();
    Status status = validator.validate(request, mock(RequestContext.class));

    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("Invalid Regex pattern: ["));

    AnomalyDetectionConfig detectionConfig1 =
        AnomalyDetectionConfig.newBuilder()
            .setApiDefinitionMetadataAnomalyDetectionConfig(
                ApiDefinitionMetadataAnomalyDetectionConfig.newBuilder()
                    .setUnknownParam(
                        UnknownParamAnomalyConfig.newBuilder()
                            .setSevereRegexStrings(
                                StringList.newBuilder()
                                    .addAllValues(
                                        List.of(
                                            "[",
                                            "/^[(]{0,1}[0-9]{3}[)]{0,1}[-\\s\\.]{0,1}[0-9]{3}[-\\s\\.]{0,1}[0-9]{4}$/"))
                                    .build())
                            .build())
                    .build())
            .build();

    request =
        UpdateScopedAnomalyDetectionConfigRequest.newBuilder()
            .setScopedAnomalyDetectionConfig(
                ScopedAnomalyDetectionConfig.newBuilder()
                    .setConfigScope(configScope)
                    .addAllAnomalyDetectionConfigs(List.of(detectionConfig1)))
            .build();

    status = validator.validate(request, mock(RequestContext.class));

    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("Invalid Regex pattern: ["));

    AnomalyDetectionConfig detectionConfig2 =
        AnomalyDetectionConfig.newBuilder()
            .setApiDefinitionMetadataAnomalyDetectionConfig(
                ApiDefinitionMetadataAnomalyDetectionConfig.newBuilder()
                    .setUnknownParam(
                        UnknownParamAnomalyConfig.newBuilder()
                            .setSevereRegexStrings(
                                StringList.newBuilder()
                                    .addAllValues(
                                        List.of(
                                            "/^[(]{0,1}[0-9]{3}[)]{0,1}[-\\s\\.]{0,1}[0-9]{3}[-\\s\\.]{0,1}[0-9]{4}$/"))
                                    .build())
                            .build())
                    .build())
            .build();

    request =
        UpdateScopedAnomalyDetectionConfigRequest.newBuilder()
            .setScopedAnomalyDetectionConfig(
                ScopedAnomalyDetectionConfig.newBuilder()
                    .setConfigScope(configScope)
                    .addAllAnomalyDetectionConfigs(List.of(detectionConfig2)))
            .build();

    status = validator.validate(request, mock(RequestContext.class));

    assertEquals(Status.OK.getCode(), status.getCode());
  }

  @Test
  void testSessionDefinitionMetadataRegexValidation() {

    UpdateScopedAnomalyDetectionConfigRequest request;

    AnomalyDetectionConfig detectionConfig =
        AnomalyDetectionConfig.newBuilder()
            .setSessionDefinitionMetadataAnomalyDetectionConfig(
                SessionDefinitionMetadataAnomalyDetectionConfig.newBuilder()
                    .setObjectBola(
                        ObjectBolaAnomalyConfig.newBuilder()
                            .setRequestParamValuesNotAllowed(
                                StringList.newBuilder().addAllValues(List.of("params")).build())
                            .setMultiValuedStringParamRules(
                                MultiValuedStringParamRulesList.newBuilder()
                                    .addAllRules(
                                        List.of(
                                            MultiValuedStringParamRule.newBuilder()
                                                .setKeyRegex("[")
                                                .setValueDelimiter("/")
                                                .setValueRegex("]")
                                                .build()))
                                    .build())
                            .build())
                    .build())
            .build();

    request =
        UpdateScopedAnomalyDetectionConfigRequest.newBuilder()
            .setScopedAnomalyDetectionConfig(
                ScopedAnomalyDetectionConfig.newBuilder()
                    .setConfigScope(configScope)
                    .addAllAnomalyDetectionConfigs(List.of(detectionConfig)))
            .build();

    Status status = validator.validate(request, mock(RequestContext.class));
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("Invalid Regex pattern: ["));

    AnomalyDetectionConfig detectionConfig1 =
        AnomalyDetectionConfig.newBuilder()
            .setSessionDefinitionMetadataAnomalyDetectionConfig(
                SessionDefinitionMetadataAnomalyDetectionConfig.newBuilder()
                    .setObjectBola(
                        ObjectBolaAnomalyConfig.newBuilder()
                            .setRequestParamValuesNotAllowed(
                                StringList.newBuilder().addAllValues(List.of("params")).build())
                            .setMultiValuedStringParamRules(
                                MultiValuedStringParamRulesList.newBuilder()
                                    .addAllRules(
                                        List.of(
                                            MultiValuedStringParamRule.newBuilder()
                                                .setKeyRegex(
                                                    "/^[(]{0,1}[0-9]{3}[)]{0,1}[-\\s\\.]{0,1}[0-9]{3}[-\\s\\.]{0,1}[0-9]{4}$/\n")
                                                .setValueDelimiter("/")
                                                .setValueRegex(
                                                    "/^[(]{0,1}[0-9]{3}[)]{0,1}[-\\s\\.]{0,1}[0-9]{3}[-\\s\\.]{0,1}[0-9]{4}$/\n")
                                                .build()))
                                    .build())
                            .build())
                    .build())
            .build();

    request =
        UpdateScopedAnomalyDetectionConfigRequest.newBuilder()
            .setScopedAnomalyDetectionConfig(
                ScopedAnomalyDetectionConfig.newBuilder()
                    .setConfigScope(configScope)
                    .addAllAnomalyDetectionConfigs(List.of(detectionConfig1)))
            .build();

    status = validator.validate(request, mock(RequestContext.class));
    assertEquals(Status.OK.getCode(), status.getCode());
  }

  private AnomalyDetectionConfig buildModSecConfig(String ruleId, Optional<String> subRuleId) {
    ModsecurityAnomalyRuleConfig.Builder builder =
        ModsecurityAnomalyRuleConfig.newBuilder().setAnomalyRuleId(ruleId);
    subRuleId.ifPresent(
        id -> builder.addSubRuleConfigs(AnomalySubRuleConfig.newBuilder().setSubRuleId(id)));
    return AnomalyDetectionConfig.newBuilder()
        .setModsecurityAnomalyDetectionConfig(
            ModsecurityAnomalyDetectionConfig.newBuilder().setModsecAnomalyRule(builder))
        .build();
  }

  private UpdateScopedAnomalyDetectionConfigRequest buildUpdateRequest(
      List<AnomalyDetectionConfig> anomalyDetectionConfigs) {
    return UpdateScopedAnomalyDetectionConfigRequest.newBuilder()
        .setScopedAnomalyDetectionConfig(
            ScopedAnomalyDetectionConfig.newBuilder()
                .setConfigScope(configScope)
                .addAllAnomalyDetectionConfigs(anomalyDetectionConfigs)
                .build())
        .build();
  }
}
