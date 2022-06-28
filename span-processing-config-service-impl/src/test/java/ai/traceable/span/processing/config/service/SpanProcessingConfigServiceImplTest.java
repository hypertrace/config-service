package ai.traceable.span.processing.config.service;

import static ai.traceable.licensestatus.config.service.v1.LicenseLimit.LICENSE_LIMIT_AVAILABLE;
import static ai.traceable.licensestatus.config.service.v1.LicenseLimit.LICENSE_LIMIT_EXHAUSTED;
import static ai.traceable.span.processing.config.service.SpanProcessingConfigServiceImplTestUtils.buildMockConfig;
import static ai.traceable.span.processing.config.service.SpanProcessingConfigServiceImplTestUtils.buildProtectionSpanRuleInfo;
import static ai.traceable.span.processing.config.service.SpanProcessingConfigServiceImplTestUtils.buildSamplingConfig;
import static ai.traceable.span.processing.config.service.SpanProcessingConfigServiceImplTestUtils.buildSamplingConfigInfo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.licensestatus.config.service.v1.GetLicenseStatusResponse;
import ai.traceable.licensestatus.config.service.v1.LicenseStatus;
import ai.traceable.licensestatus.config.service.v1.LicenseStatusConfigServiceGrpc;
import ai.traceable.span.processing.config.service.licensestatus.DefaultLicenseStatusConfigManager;
import ai.traceable.span.processing.config.service.licensestatus.LicenseStatusConfigManager;
import ai.traceable.span.processing.config.service.protectionspanrules.DefaultProtectionSpanRulesManager;
import ai.traceable.span.processing.config.service.protectionspanrules.ProtectionSpanRulesManager;
import ai.traceable.span.processing.config.service.samplingconfigs.DefaultSamplingConfigManager;
import ai.traceable.span.processing.config.service.samplingconfigs.RateLimitConfigManager;
import ai.traceable.span.processing.config.service.samplingconfigs.SamplingConfigManager;
import ai.traceable.span.processing.config.service.store.ProtectionSpanRulesConfigStore;
import ai.traceable.span.processing.config.service.store.SamplingConfigsConfigStore;
import ai.traceable.span.processing.config.service.utils.TimestampConverter;
import ai.traceable.span.processing.config.service.v1.CreateProtectionSpanRuleRequest;
import ai.traceable.span.processing.config.service.v1.CreateSamplingConfigRequest;
import ai.traceable.span.processing.config.service.v1.DeleteProtectionSpanRuleRequest;
import ai.traceable.span.processing.config.service.v1.DeleteSamplingConfigRequest;
import ai.traceable.span.processing.config.service.v1.Field;
import ai.traceable.span.processing.config.service.v1.GetAllProtectionSpanRulesRequest;
import ai.traceable.span.processing.config.service.v1.GetAllResolvedProtectionSpanRulesRequest;
import ai.traceable.span.processing.config.service.v1.GetAllResolvedSamplingConfigsRequest;
import ai.traceable.span.processing.config.service.v1.GetAllSamplingConfigsRequest;
import ai.traceable.span.processing.config.service.v1.ProtectionSpanRule;
import ai.traceable.span.processing.config.service.v1.ProtectionSpanRuleDetails;
import ai.traceable.span.processing.config.service.v1.ProtectionSpanRuleInfo;
import ai.traceable.span.processing.config.service.v1.RateLimit;
import ai.traceable.span.processing.config.service.v1.RateLimitConfig;
import ai.traceable.span.processing.config.service.v1.RelationalOperator;
import ai.traceable.span.processing.config.service.v1.RelationalSpanFilterExpression;
import ai.traceable.span.processing.config.service.v1.SamplingConfig;
import ai.traceable.span.processing.config.service.v1.SamplingConfigDetails;
import ai.traceable.span.processing.config.service.v1.SamplingConfigInfo;
import ai.traceable.span.processing.config.service.v1.SpanFilter;
import ai.traceable.span.processing.config.service.v1.SpanFilterValue;
import ai.traceable.span.processing.config.service.v1.UpdateProtectionSpanRule;
import ai.traceable.span.processing.config.service.v1.UpdateProtectionSpanRuleRequest;
import ai.traceable.span.processing.config.service.v1.UpdateSamplingConfig;
import ai.traceable.span.processing.config.service.v1.UpdateSamplingConfigRequest;
import ai.traceable.span.processing.config.service.v1.WindowedRateLimit;
import ai.traceable.span.processing.config.service.validation.SpanProcessingConfigRequestValidator;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import java.util.List;
import java.util.stream.Collectors;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SpanProcessingConfigServiceImplTest {

  private MockGenericConfigService mockGenericConfigService;
  private LicenseStatusConfigServiceGrpc.LicenseStatusConfigServiceBlockingStub
      licenseStatusConfigServiceBlockingStub;
  private ai.traceable.span.processing.config.service.v1.SpanProcessingConfigServiceGrpc
          .SpanProcessingConfigServiceBlockingStub
      spanProcessingConfigServiceStub;
  private TimestampConverter timestampConverter;

  @BeforeEach
  void beforeEach() {
    this.mockGenericConfigService =
        new MockGenericConfigService()
            .mockUpsert()
            .mockGet()
            .mockGetAll()
            .mockDelete()
            .mockUpsertAll();

    licenseStatusConfigServiceBlockingStub =
        mock(LicenseStatusConfigServiceGrpc.LicenseStatusConfigServiceBlockingStub.class);

    RateLimitConfigManager rateLimitConfigManager = new RateLimitConfigManager(buildMockConfig());
    LicenseStatusConfigManager licenseStatusConfigManager =
        new DefaultLicenseStatusConfigManager(licenseStatusConfigServiceBlockingStub);

    ConfigServiceGrpc.ConfigServiceBlockingStub genericStub =
        ConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
    timestampConverter = mock(TimestampConverter.class);

    SamplingConfigsConfigStore samplingConfigsConfigStore =
        new SamplingConfigsConfigStore(genericStub, timestampConverter);
    ProtectionSpanRulesConfigStore protectionSpanRulesConfigStore =
        new ProtectionSpanRulesConfigStore(genericStub, timestampConverter);
    SamplingConfigManager samplingConfigManager =
        new DefaultSamplingConfigManager(
            samplingConfigsConfigStore,
            timestampConverter,
            rateLimitConfigManager,
            licenseStatusConfigManager);
    ProtectionSpanRulesManager protectionSpanRulesManager =
        new DefaultProtectionSpanRulesManager(
            protectionSpanRulesConfigStore, timestampConverter, licenseStatusConfigManager);

    this.mockGenericConfigService
        .addService(
            new SpanProcessingConfigServiceImpl(
                samplingConfigManager,
                protectionSpanRulesManager,
                new SpanProcessingConfigRequestValidator()))
        .start();

    this.spanProcessingConfigServiceStub =
        ai.traceable.span.processing.config.service.v1.SpanProcessingConfigServiceGrpc
            .newBlockingStub(this.mockGenericConfigService.channel());

    when(timestampConverter.convert(any()))
        .thenReturn(Timestamp.newBuilder().setSeconds(100).build());
  }

  @AfterEach
  void afterEach() {
    this.mockGenericConfigService.shutdown();
  }

  @Test
  void testGetAllResolvedSamplingConfigs_licenseLimitExhausted() {
    when(licenseStatusConfigServiceBlockingStub.getLicenseStatus(any()))
        .thenReturn(
            GetLicenseStatusResponse.newBuilder()
                .setLicenseStatus(
                    LicenseStatus.newBuilder()
                        .setTracesLicenseLimit(LICENSE_LIMIT_EXHAUSTED)
                        .build())
                .build());

    List<SamplingConfig> samplingConfigs =
        this.spanProcessingConfigServiceStub
            .getAllResolvedSamplingConfigs(
                GetAllResolvedSamplingConfigsRequest.newBuilder().build())
            .getSamplingConfigsList();

    assertEquals(1, samplingConfigs.size());
    // get zero sampling config
    assertEquals(buildSamplingConfig("zero-sampling-config-id", 0), samplingConfigs.get(0));
  }

  @Test
  void testGetAllResolvedSamplingConfigs_licenseLimitAvailable() {
    SamplingConfigDetails samplingConfigDetails =
        this.spanProcessingConfigServiceStub
            .createSamplingConfig(
                CreateSamplingConfigRequest.newBuilder()
                    .setSamplingConfigInfo(buildSamplingConfigInfo(1000))
                    .build())
            .getSamplingConfigDetails();
    SamplingConfig samplingConfig = samplingConfigDetails.getSamplingConfig();

    when(licenseStatusConfigServiceBlockingStub.getLicenseStatus(any()))
        .thenReturn(
            GetLicenseStatusResponse.newBuilder()
                .setLicenseStatus(
                    LicenseStatus.newBuilder()
                        .setTracesLicenseLimit(LICENSE_LIMIT_AVAILABLE)
                        .build())
                .build());

    List<SamplingConfig> samplingConfigs =
        this.spanProcessingConfigServiceStub
            .getAllResolvedSamplingConfigs(
                GetAllResolvedSamplingConfigsRequest.newBuilder().build())
            .getSamplingConfigsList();

    assertEquals(2, samplingConfigs.size());
    assertEquals(samplingConfig, samplingConfigs.get(0));
    // default sampling config
    assertEquals(buildSamplingConfig("default-sampling-config-id", 100), samplingConfigs.get(1));
  }

  @Test
  void testGetAllResolvedProtectionSpanRules_licenseLimitExhausted() {
    when(licenseStatusConfigServiceBlockingStub.getLicenseStatus(any()))
        .thenReturn(
            GetLicenseStatusResponse.newBuilder()
                .setLicenseStatus(
                    LicenseStatus.newBuilder()
                        .setTracesLicenseLimit(LICENSE_LIMIT_EXHAUSTED)
                        .setProtectionLicense(LicenseStatus.License.newBuilder().build())
                        .build())
                .build());

    List<ProtectionSpanRule> protectionSpanRules =
        spanProcessingConfigServiceStub
            .getAllResolvedProtectionSpanRules(
                GetAllResolvedProtectionSpanRulesRequest.newBuilder().build())
            .getRulesList();

    assertTrue(protectionSpanRules.isEmpty());
  }

  @Test
  void testGetAllResolvedProtectionSpanRules_licenseLimitAvailable_noProtectionLicense() {
    when(licenseStatusConfigServiceBlockingStub.getLicenseStatus(any()))
        .thenReturn(
            GetLicenseStatusResponse.newBuilder()
                .setLicenseStatus(
                    LicenseStatus.newBuilder()
                        .setTracesLicenseLimit(LICENSE_LIMIT_AVAILABLE)
                        .build())
                .build());

    List<ProtectionSpanRule> protectionSpanRules =
        spanProcessingConfigServiceStub
            .getAllResolvedProtectionSpanRules(
                GetAllResolvedProtectionSpanRulesRequest.newBuilder().build())
            .getRulesList();

    assertTrue(protectionSpanRules.isEmpty());
  }

  @Test
  void testGetAllResolvedProtectionSpanRules_licenseLimitAvailable() {
    ProtectionSpanRuleDetails protectionSpanRuleDetails =
        this.spanProcessingConfigServiceStub
            .createProtectionSpanRule(
                CreateProtectionSpanRuleRequest.newBuilder()
                    .setRuleInfo(buildProtectionSpanRuleInfo())
                    .build())
            .getRuleDetails();
    ProtectionSpanRule protectionSpanRule = protectionSpanRuleDetails.getRule();

    when(licenseStatusConfigServiceBlockingStub.getLicenseStatus(any()))
        .thenReturn(
            GetLicenseStatusResponse.newBuilder()
                .setLicenseStatus(
                    LicenseStatus.newBuilder()
                        .setTracesLicenseLimit(LICENSE_LIMIT_AVAILABLE)
                        .setProtectionLicense(LicenseStatus.License.newBuilder().build())
                        .build())
                .build());

    List<ProtectionSpanRule> protectionSpanRules =
        spanProcessingConfigServiceStub
            .getAllResolvedProtectionSpanRules(
                GetAllResolvedProtectionSpanRulesRequest.newBuilder().build())
            .getRulesList();

    assertEquals(1, protectionSpanRules.size());
    assertEquals(protectionSpanRule, protectionSpanRules.get(0));
  }

  @Test
  void testProtectionSpanRulesCrud() {
    ProtectionSpanRuleDetails firstCreatedProtectionSpanRuleDetails =
        this.spanProcessingConfigServiceStub
            .createProtectionSpanRule(
                CreateProtectionSpanRuleRequest.newBuilder()
                    .setRuleInfo(
                        ProtectionSpanRuleInfo.newBuilder()
                            .setName("ruleName1")
                            .setDisabled(true)
                            .setFilter(
                                SpanFilter.newBuilder()
                                    .setRelationalSpanFilter(
                                        RelationalSpanFilterExpression.newBuilder()
                                            .setField(Field.FIELD_SERVICE_NAME)
                                            .setOperator(
                                                RelationalOperator.RELATIONAL_OPERATOR_CONTAINS)
                                            .setRightOperand(
                                                SpanFilterValue.newBuilder().setStringValue("a")))))
                    .build())
            .getRuleDetails();
    ProtectionSpanRule firstCreatedProtectionSpanRule =
        firstCreatedProtectionSpanRuleDetails.getRule();
    Timestamp expectedTimestamp = Timestamp.newBuilder().setSeconds(100).build();
    assertEquals(
        expectedTimestamp,
        firstCreatedProtectionSpanRuleDetails.getMetadata().getCreationTimestamp());
    assertEquals(
        expectedTimestamp,
        firstCreatedProtectionSpanRuleDetails.getMetadata().getLastUpdatedTimestamp());

    ProtectionSpanRule secondCreatedProtectionSpanRule =
        this.spanProcessingConfigServiceStub
            .createProtectionSpanRule(
                CreateProtectionSpanRuleRequest.newBuilder()
                    .setRuleInfo(
                        ProtectionSpanRuleInfo.newBuilder()
                            .setName("ruleName2")
                            .setDisabled(true)
                            .setFilter(
                                SpanFilter.newBuilder()
                                    .setRelationalSpanFilter(
                                        RelationalSpanFilterExpression.newBuilder()
                                            .setField(Field.FIELD_SERVICE_NAME)
                                            .setOperator(
                                                RelationalOperator.RELATIONAL_OPERATOR_CONTAINS)
                                            .setRightOperand(
                                                SpanFilterValue.newBuilder().setStringValue("a")))))
                    .build())
            .getRuleDetails()
            .getRule();

    List<ProtectionSpanRule> protectionSpanRules =
        this.spanProcessingConfigServiceStub
            .getAllProtectionSpanRules(GetAllProtectionSpanRulesRequest.newBuilder().build())
            .getRuleDetailsList()
            .stream()
            .map(ProtectionSpanRuleDetails::getRule)
            .collect(Collectors.toUnmodifiableList());
    assertEquals(2, protectionSpanRules.size());
    assertTrue(protectionSpanRules.contains(firstCreatedProtectionSpanRule));
    assertTrue(protectionSpanRules.contains(secondCreatedProtectionSpanRule));

    ProtectionSpanRule updatedFirstProtectionSpanRule =
        this.spanProcessingConfigServiceStub
            .updateProtectionSpanRule(
                UpdateProtectionSpanRuleRequest.newBuilder()
                    .setRule(
                        UpdateProtectionSpanRule.newBuilder()
                            .setId(firstCreatedProtectionSpanRule.getId())
                            .setName("updatedRuleName1")
                            .setDisabled(false)
                            .setFilter(
                                SpanFilter.newBuilder()
                                    .setRelationalSpanFilter(
                                        RelationalSpanFilterExpression.newBuilder()
                                            .setField(Field.FIELD_SERVICE_NAME)
                                            .setOperator(
                                                RelationalOperator.RELATIONAL_OPERATOR_CONTAINS)
                                            .setRightOperand(
                                                SpanFilterValue.newBuilder().setStringValue("a")))))
                    .build())
            .getRuleDetails()
            .getRule();
    assertEquals("updatedRuleName1", updatedFirstProtectionSpanRule.getRuleInfo().getName());
    assertFalse(updatedFirstProtectionSpanRule.getRuleInfo().getDisabled());

    protectionSpanRules =
        this.spanProcessingConfigServiceStub
            .getAllProtectionSpanRules(GetAllProtectionSpanRulesRequest.newBuilder().build())
            .getRuleDetailsList()
            .stream()
            .map(ProtectionSpanRuleDetails::getRule)
            .collect(Collectors.toUnmodifiableList());
    assertEquals(2, protectionSpanRules.size());
    assertTrue(protectionSpanRules.contains(updatedFirstProtectionSpanRule));

    this.spanProcessingConfigServiceStub.deleteProtectionSpanRule(
        DeleteProtectionSpanRuleRequest.newBuilder()
            .setId(firstCreatedProtectionSpanRule.getId())
            .build());

    protectionSpanRules =
        this.spanProcessingConfigServiceStub
            .getAllProtectionSpanRules(GetAllProtectionSpanRulesRequest.newBuilder().build())
            .getRuleDetailsList()
            .stream()
            .map(ProtectionSpanRuleDetails::getRule)
            .collect(Collectors.toUnmodifiableList());
    assertEquals(1, protectionSpanRules.size());
    assertEquals(secondCreatedProtectionSpanRule, protectionSpanRules.get(0));
  }

  @Test
  void testSamplingConfigsCrud() {
    SamplingConfigDetails firstCreatedSamplingConfigDetails =
        this.spanProcessingConfigServiceStub
            .createSamplingConfig(
                CreateSamplingConfigRequest.newBuilder()
                    .setSamplingConfigInfo(
                        SamplingConfigInfo.newBuilder()
                            .setRateLimitConfig(
                                RateLimitConfig.newBuilder()
                                    .setTraceLimitGlobal(
                                        RateLimit.newBuilder()
                                            .setFixedWindowLimit(
                                                WindowedRateLimit.newBuilder()
                                                    .setQuantityAllowed(100)
                                                    .setWindowDuration(
                                                        Duration.newBuilder()
                                                            .setSeconds(60)
                                                            .build())
                                                    .build())
                                            .build())
                                    .setTraceLimitPerEndpoint(
                                        RateLimit.newBuilder()
                                            .setFixedWindowLimit(
                                                WindowedRateLimit.newBuilder()
                                                    .setQuantityAllowed(100)
                                                    .setWindowDuration(
                                                        Duration.newBuilder()
                                                            .setSeconds(60)
                                                            .build())
                                                    .build())
                                            .build())
                                    .setApiEndpointCacheDuration(
                                        Duration.newBuilder().setSeconds(100).setNanos(100).build())
                                    .build())
                            .setFilter(
                                SpanFilter.newBuilder()
                                    .setRelationalSpanFilter(
                                        RelationalSpanFilterExpression.newBuilder()
                                            .setField(Field.FIELD_SERVICE_NAME)
                                            .setOperator(
                                                RelationalOperator.RELATIONAL_OPERATOR_CONTAINS)
                                            .setRightOperand(
                                                SpanFilterValue.newBuilder().setStringValue("a")))))
                    .build())
            .getSamplingConfigDetails();
    SamplingConfig firstCreatedSamplingConfig =
        firstCreatedSamplingConfigDetails.getSamplingConfig();
    Timestamp expectedTimestamp = Timestamp.newBuilder().setSeconds(100).build();
    assertEquals(
        expectedTimestamp, firstCreatedSamplingConfigDetails.getMetadata().getCreationTimestamp());
    assertEquals(
        expectedTimestamp,
        firstCreatedSamplingConfigDetails.getMetadata().getLastUpdatedTimestamp());

    SamplingConfig secondCreatedSamplingConfig =
        this.spanProcessingConfigServiceStub
            .createSamplingConfig(
                CreateSamplingConfigRequest.newBuilder()
                    .setSamplingConfigInfo(
                        SamplingConfigInfo.newBuilder()
                            .setRateLimitConfig(
                                RateLimitConfig.newBuilder()
                                    .setTraceLimitGlobal(
                                        RateLimit.newBuilder()
                                            .setFixedWindowLimit(
                                                WindowedRateLimit.newBuilder()
                                                    .setQuantityAllowed(200)
                                                    .setWindowDuration(
                                                        Duration.newBuilder()
                                                            .setSeconds(60)
                                                            .build())
                                                    .build())
                                            .build())
                                    .setTraceLimitPerEndpoint(
                                        RateLimit.newBuilder()
                                            .setFixedWindowLimit(
                                                WindowedRateLimit.newBuilder()
                                                    .setQuantityAllowed(200)
                                                    .setWindowDuration(
                                                        Duration.newBuilder()
                                                            .setSeconds(60)
                                                            .build())
                                                    .build())
                                            .build())
                                    .setApiEndpointCacheDuration(
                                        Duration.newBuilder().setSeconds(100).setNanos(100).build())
                                    .build())
                            .setFilter(
                                SpanFilter.newBuilder()
                                    .setRelationalSpanFilter(
                                        RelationalSpanFilterExpression.newBuilder()
                                            .setField(Field.FIELD_SERVICE_NAME)
                                            .setOperator(
                                                RelationalOperator.RELATIONAL_OPERATOR_CONTAINS)
                                            .setRightOperand(
                                                SpanFilterValue.newBuilder().setStringValue("b")))))
                    .build())
            .getSamplingConfigDetails()
            .getSamplingConfig();

    List<SamplingConfig> samplingConfigs =
        this.spanProcessingConfigServiceStub
            .getAllSamplingConfigs(GetAllSamplingConfigsRequest.newBuilder().build())
            .getSamplingConfigDetailsList()
            .stream()
            .map(SamplingConfigDetails::getSamplingConfig)
            .collect(Collectors.toUnmodifiableList());
    assertEquals(2, samplingConfigs.size());
    assertTrue(samplingConfigs.contains(firstCreatedSamplingConfig));
    assertTrue(samplingConfigs.contains(secondCreatedSamplingConfig));

    SamplingConfig updatedFirstSamplingConfig =
        this.spanProcessingConfigServiceStub
            .updateSamplingConfig(
                UpdateSamplingConfigRequest.newBuilder()
                    .setSamplingConfig(
                        UpdateSamplingConfig.newBuilder()
                            .setId(firstCreatedSamplingConfig.getId())
                            .setRateLimitConfig(
                                RateLimitConfig.newBuilder()
                                    .setTraceLimitGlobal(
                                        RateLimit.newBuilder()
                                            .setFixedWindowLimit(
                                                WindowedRateLimit.newBuilder()
                                                    .setQuantityAllowed(300)
                                                    .setWindowDuration(
                                                        Duration.newBuilder()
                                                            .setSeconds(60)
                                                            .build())
                                                    .build())
                                            .build())
                                    .setTraceLimitPerEndpoint(
                                        RateLimit.newBuilder()
                                            .setFixedWindowLimit(
                                                WindowedRateLimit.newBuilder()
                                                    .setQuantityAllowed(100)
                                                    .setWindowDuration(
                                                        Duration.newBuilder()
                                                            .setSeconds(60)
                                                            .build())
                                                    .build())
                                            .build())
                                    .setApiEndpointCacheDuration(
                                        Duration.newBuilder().setSeconds(100).setNanos(100).build())
                                    .build())
                            .setFilter(
                                SpanFilter.newBuilder()
                                    .setRelationalSpanFilter(
                                        RelationalSpanFilterExpression.newBuilder()
                                            .setField(Field.FIELD_SERVICE_NAME)
                                            .setOperator(
                                                RelationalOperator.RELATIONAL_OPERATOR_CONTAINS)
                                            .setRightOperand(
                                                SpanFilterValue.newBuilder().setStringValue("a")))))
                    .build())
            .getSamplingConfigDetails()
            .getSamplingConfig();
    assertEquals(
        RateLimitConfig.newBuilder()
            .setTraceLimitGlobal(
                RateLimit.newBuilder()
                    .setFixedWindowLimit(
                        WindowedRateLimit.newBuilder()
                            .setQuantityAllowed(300)
                            .setWindowDuration(Duration.newBuilder().setSeconds(60).build())
                            .build())
                    .build())
            .setTraceLimitPerEndpoint(
                RateLimit.newBuilder()
                    .setFixedWindowLimit(
                        WindowedRateLimit.newBuilder()
                            .setQuantityAllowed(100)
                            .setWindowDuration(Duration.newBuilder().setSeconds(60).build())
                            .build())
                    .build())
            .setApiEndpointCacheDuration(
                Duration.newBuilder().setSeconds(100).setNanos(100).build())
            .build(),
        updatedFirstSamplingConfig.getSamplingConfigInfo().getRateLimitConfig());

    samplingConfigs =
        this.spanProcessingConfigServiceStub
            .getAllSamplingConfigs(GetAllSamplingConfigsRequest.newBuilder().build())
            .getSamplingConfigDetailsList()
            .stream()
            .map(SamplingConfigDetails::getSamplingConfig)
            .collect(Collectors.toUnmodifiableList());
    assertEquals(2, samplingConfigs.size());
    assertTrue(samplingConfigs.contains(updatedFirstSamplingConfig));

    this.spanProcessingConfigServiceStub.deleteSamplingConfig(
        DeleteSamplingConfigRequest.newBuilder().setId(firstCreatedSamplingConfig.getId()).build());

    samplingConfigs =
        this.spanProcessingConfigServiceStub
            .getAllSamplingConfigs(GetAllSamplingConfigsRequest.newBuilder().build())
            .getSamplingConfigDetailsList()
            .stream()
            .map(SamplingConfigDetails::getSamplingConfig)
            .collect(Collectors.toUnmodifiableList());
    assertEquals(1, samplingConfigs.size());
    assertEquals(secondCreatedSamplingConfig, samplingConfigs.get(0));
  }
}
