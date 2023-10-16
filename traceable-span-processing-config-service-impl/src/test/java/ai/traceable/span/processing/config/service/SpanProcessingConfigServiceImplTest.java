package ai.traceable.span.processing.config.service;

import static ai.traceable.licensestatus.config.service.v1.LicenseLimit.LICENSE_LIMIT_AVAILABLE;
import static ai.traceable.licensestatus.config.service.v1.LicenseLimit.LICENSE_LIMIT_EXHAUSTED;
import static ai.traceable.span.processing.config.service.SpanProcessingConfigServiceImplTestUtils.buildProtectionSpanRuleInfo;
import static ai.traceable.span.processing.config.service.SpanProcessingConfigServiceImplTestUtils.buildSamplingConfig;
import static ai.traceable.span.processing.config.service.SpanProcessingConfigServiceImplTestUtils.buildSamplingConfigInfo;
import static ai.traceable.span.processing.config.service.v1.RateLimitStrategy.RATE_LIMIT_STRATEGY_BARESPAN;
import static ai.traceable.span.processing.config.service.v1.RateLimitStrategy.RATE_LIMIT_STRATEGY_DO_NOT_PERSIST;
import static ai.traceable.span.processing.config.service.v1.RateLimitStrategy.RATE_LIMIT_STRATEGY_DROP;
import static ai.traceable.span.processing.config.service.v1.RelationalOperator.RELATIONAL_OPERATOR_CONTAINS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.config.utils.TimestampConverter;
import ai.traceable.licensestatus.config.service.v1.GetLicenseStatusResponse;
import ai.traceable.licensestatus.config.service.v1.LicenseStatus;
import ai.traceable.licensestatus.config.service.v1.LicenseStatusConfigServiceGrpc;
import ai.traceable.span.processing.config.service.apinamingrules.ApiNamingRulesManager;
import ai.traceable.span.processing.config.service.apinamingrules.DefaultApiNamingRulesManager;
import ai.traceable.span.processing.config.service.licensestatus.DefaultLicenseStatusConfigManager;
import ai.traceable.span.processing.config.service.licensestatus.LicenseStatusConfigManager;
import ai.traceable.span.processing.config.service.protectionspanrules.DefaultProtectionSpanRulesManager;
import ai.traceable.span.processing.config.service.protectionspanrules.ProtectionSpanRulesManager;
import ai.traceable.span.processing.config.service.samplingconfigs.DefaultSamplingConfigManager;
import ai.traceable.span.processing.config.service.samplingconfigs.SamplingConfigManager;
import ai.traceable.span.processing.config.service.servicenaming.ServiceNamingRulesManager;
import ai.traceable.span.processing.config.service.spaningestionrules.SpanIngestionRulesManager;
import ai.traceable.span.processing.config.service.store.ApiNamingRulesConfigStore;
import ai.traceable.span.processing.config.service.store.DefaultProtectionSpanRuleEvaluationStatusConfigStore;
import ai.traceable.span.processing.config.service.store.ProtectionSpanRulesConfigStore;
import ai.traceable.span.processing.config.service.store.SamplingConfigsConfigStore;
import ai.traceable.span.processing.config.service.v1.ApiNamingRule;
import ai.traceable.span.processing.config.service.v1.ApiNamingRuleConfig;
import ai.traceable.span.processing.config.service.v1.ApiNamingRuleDetails;
import ai.traceable.span.processing.config.service.v1.ApiNamingRuleInfo;
import ai.traceable.span.processing.config.service.v1.ApiSpecBasedConfig;
import ai.traceable.span.processing.config.service.v1.AstScanBasedConfig;
import ai.traceable.span.processing.config.service.v1.CreateApiNamingRuleRequest;
import ai.traceable.span.processing.config.service.v1.CreateApiNamingRulesRequest;
import ai.traceable.span.processing.config.service.v1.CreateProtectionSpanRuleRequest;
import ai.traceable.span.processing.config.service.v1.CreateSamplingConfigRequest;
import ai.traceable.span.processing.config.service.v1.DefaultProtectionSpanRuleEvaluationStatus;
import ai.traceable.span.processing.config.service.v1.DeleteApiNamingRuleRequest;
import ai.traceable.span.processing.config.service.v1.DeleteApiNamingRulesRequest;
import ai.traceable.span.processing.config.service.v1.DeleteProtectionSpanRuleRequest;
import ai.traceable.span.processing.config.service.v1.DeleteSamplingConfigRequest;
import ai.traceable.span.processing.config.service.v1.Field;
import ai.traceable.span.processing.config.service.v1.GetAllApiNamingRulesRequest;
import ai.traceable.span.processing.config.service.v1.GetAllProtectionSpanRulesRequest;
import ai.traceable.span.processing.config.service.v1.GetAllResolvedProtectionSpanRulesRequest;
import ai.traceable.span.processing.config.service.v1.GetAllResolvedSamplingConfigsRequest;
import ai.traceable.span.processing.config.service.v1.GetAllSamplingConfigsRequest;
import ai.traceable.span.processing.config.service.v1.GetDefaultProtectionSpanRuleEvaluationStatusRequest;
import ai.traceable.span.processing.config.service.v1.ProtectionSpanRule;
import ai.traceable.span.processing.config.service.v1.ProtectionSpanRuleDetails;
import ai.traceable.span.processing.config.service.v1.ProtectionSpanRuleInfo;
import ai.traceable.span.processing.config.service.v1.RateLimit;
import ai.traceable.span.processing.config.service.v1.RateLimitConfig;
import ai.traceable.span.processing.config.service.v1.RelationalSpanFilterExpression;
import ai.traceable.span.processing.config.service.v1.SamplingConfig;
import ai.traceable.span.processing.config.service.v1.SamplingConfigDetails;
import ai.traceable.span.processing.config.service.v1.SamplingConfigInfo;
import ai.traceable.span.processing.config.service.v1.SegmentMatchingBasedConfig;
import ai.traceable.span.processing.config.service.v1.SpanFilter;
import ai.traceable.span.processing.config.service.v1.SpanFilterValue;
import ai.traceable.span.processing.config.service.v1.UpdateApiNamingRule;
import ai.traceable.span.processing.config.service.v1.UpdateApiNamingRuleRequest;
import ai.traceable.span.processing.config.service.v1.UpdateApiNamingRulesRequest;
import ai.traceable.span.processing.config.service.v1.UpdateDefaultProtectionSpanRuleEvaluationStatusRequest;
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
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
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

  @BeforeEach
  void beforeEach() {
    this.mockGenericConfigService =
        new MockGenericConfigService()
            .mockUpsert()
            .mockGet()
            .mockGetAll()
            .mockDelete()
            .mockDeleteAll()
            .mockUpsertAll();

    licenseStatusConfigServiceBlockingStub =
        mock(LicenseStatusConfigServiceGrpc.LicenseStatusConfigServiceBlockingStub.class);

    ConfigChangeEventGenerator configChangeEventGenerator = mock(ConfigChangeEventGenerator.class);
    LicenseStatusConfigManager licenseStatusConfigManager =
        new DefaultLicenseStatusConfigManager(licenseStatusConfigServiceBlockingStub);

    ConfigServiceGrpc.ConfigServiceBlockingStub genericStub =
        ConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
    TimestampConverter timestampConverter = mock(TimestampConverter.class);

    SamplingConfigsConfigStore samplingConfigsConfigStore =
        new SamplingConfigsConfigStore(genericStub, timestampConverter, configChangeEventGenerator);
    ApiNamingRulesConfigStore apiNamingRulesConfigStore =
        new ApiNamingRulesConfigStore(genericStub, timestampConverter, configChangeEventGenerator);
    ProtectionSpanRulesConfigStore protectionSpanRulesConfigStore =
        new ProtectionSpanRulesConfigStore(genericStub, timestampConverter);
    SamplingConfigManager samplingConfigManager =
        new DefaultSamplingConfigManager(
            samplingConfigsConfigStore, timestampConverter, licenseStatusConfigManager);
    ApiNamingRulesManager apiNamingRulesManager =
        new DefaultApiNamingRulesManager(apiNamingRulesConfigStore, timestampConverter);
    ProtectionSpanRulesManager protectionSpanRulesManager =
        new DefaultProtectionSpanRulesManager(
            protectionSpanRulesConfigStore, timestampConverter, licenseStatusConfigManager);
    DefaultProtectionSpanRuleEvaluationStatusConfigStore
        defaultProtectionSpanRuleEvaluationStatusConfigStore =
            new DefaultProtectionSpanRuleEvaluationStatusConfigStore(genericStub);
    ServiceNamingRulesManager mockServiceNamingManager = mock(ServiceNamingRulesManager.class);
    SpanIngestionRulesManager mockSpanIngestionRuleManager = mock(SpanIngestionRulesManager.class);

    this.mockGenericConfigService
        .addService(
            new SpanProcessingConfigServiceImpl(
                new SpanProcessingConfigRequestValidator(),
                samplingConfigManager,
                apiNamingRulesManager,
                protectionSpanRulesManager,
                defaultProtectionSpanRuleEvaluationStatusConfigStore,
                mockServiceNamingManager,
                mockSpanIngestionRuleManager))
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

    assertEquals(1, samplingConfigs.size());
    assertEquals(samplingConfig, samplingConfigs.get(0));
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
                                            .setOperator(RELATIONAL_OPERATOR_CONTAINS)
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
                                            .setOperator(RELATIONAL_OPERATOR_CONTAINS)
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
                                            .setOperator(RELATIONAL_OPERATOR_CONTAINS)
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
                                    .setRateLimitStrategy(RATE_LIMIT_STRATEGY_DROP)
                                    .build())
                            .setFilter(
                                SpanFilter.newBuilder()
                                    .setRelationalSpanFilter(
                                        RelationalSpanFilterExpression.newBuilder()
                                            .setField(Field.FIELD_SERVICE_NAME)
                                            .setOperator(RELATIONAL_OPERATOR_CONTAINS)
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
                                    .setRateLimitStrategy(RATE_LIMIT_STRATEGY_BARESPAN)
                                    .build())
                            .setFilter(
                                SpanFilter.newBuilder()
                                    .setRelationalSpanFilter(
                                        RelationalSpanFilterExpression.newBuilder()
                                            .setField(Field.FIELD_SERVICE_NAME)
                                            .setOperator(RELATIONAL_OPERATOR_CONTAINS)
                                            .setRightOperand(
                                                SpanFilterValue.newBuilder().setStringValue("b")))))
                    .build())
            .getSamplingConfigDetails()
            .getSamplingConfig();

    SamplingConfig thirdCreatedSamplingConfig =
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
                                    .setRateLimitStrategy(RATE_LIMIT_STRATEGY_DO_NOT_PERSIST)
                                    .build())
                            .setFilter(
                                SpanFilter.newBuilder()
                                    .setRelationalSpanFilter(
                                        RelationalSpanFilterExpression.newBuilder()
                                            .setField(Field.FIELD_SERVICE_NAME)
                                            .setOperator(RELATIONAL_OPERATOR_CONTAINS)
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
    assertEquals(3, samplingConfigs.size());
    assertTrue(samplingConfigs.contains(firstCreatedSamplingConfig));
    assertTrue(samplingConfigs.contains(secondCreatedSamplingConfig));
    assertTrue(samplingConfigs.contains(thirdCreatedSamplingConfig));

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
                                    .setRateLimitStrategy(RATE_LIMIT_STRATEGY_BARESPAN)
                                    .build())
                            .setFilter(
                                SpanFilter.newBuilder()
                                    .setRelationalSpanFilter(
                                        RelationalSpanFilterExpression.newBuilder()
                                            .setField(Field.FIELD_SERVICE_NAME)
                                            .setOperator(RELATIONAL_OPERATOR_CONTAINS)
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
            .setRateLimitStrategy(RATE_LIMIT_STRATEGY_BARESPAN)
            .build(),
        updatedFirstSamplingConfig.getSamplingConfigInfo().getRateLimitConfig());

    samplingConfigs =
        this.spanProcessingConfigServiceStub
            .getAllSamplingConfigs(GetAllSamplingConfigsRequest.newBuilder().build())
            .getSamplingConfigDetailsList()
            .stream()
            .map(SamplingConfigDetails::getSamplingConfig)
            .collect(Collectors.toUnmodifiableList());
    assertEquals(3, samplingConfigs.size());
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
    assertEquals(2, samplingConfigs.size());
    assertTrue(samplingConfigs.contains(secondCreatedSamplingConfig));
    assertTrue(samplingConfigs.contains(thirdCreatedSamplingConfig));
  }

  @Test
  void testDefaultProtectionSpanRuleEvaluationStatus() {
    DefaultProtectionSpanRuleEvaluationStatus defaultProtectionSpanRuleEvaluationStatus =
        getDefaultProtectionSpanRuleEvaluationStatus();
    assertFalse(defaultProtectionSpanRuleEvaluationStatus.getEnabled());

    defaultProtectionSpanRuleEvaluationStatus =
        updateDefaultProtectionSpanRuleEvaluationStatus(true);
    assertTrue(defaultProtectionSpanRuleEvaluationStatus.getEnabled());
    defaultProtectionSpanRuleEvaluationStatus = getDefaultProtectionSpanRuleEvaluationStatus();
    assertTrue(defaultProtectionSpanRuleEvaluationStatus.getEnabled());

    defaultProtectionSpanRuleEvaluationStatus =
        updateDefaultProtectionSpanRuleEvaluationStatus(false);
    assertFalse(defaultProtectionSpanRuleEvaluationStatus.getEnabled());
    defaultProtectionSpanRuleEvaluationStatus = getDefaultProtectionSpanRuleEvaluationStatus();
    assertFalse(defaultProtectionSpanRuleEvaluationStatus.getEnabled());
  }

  private DefaultProtectionSpanRuleEvaluationStatus getDefaultProtectionSpanRuleEvaluationStatus() {
    return this.spanProcessingConfigServiceStub
        .getDefaultProtectionSpanRuleEvaluationStatus(
            GetDefaultProtectionSpanRuleEvaluationStatusRequest.getDefaultInstance())
        .getStatus();
  }

  private DefaultProtectionSpanRuleEvaluationStatus updateDefaultProtectionSpanRuleEvaluationStatus(
      boolean enabled) {
    return this.spanProcessingConfigServiceStub
        .updateDefaultProtectionSpanRuleEvaluationStatus(
            UpdateDefaultProtectionSpanRuleEvaluationStatusRequest.newBuilder()
                .setStatus(
                    DefaultProtectionSpanRuleEvaluationStatus.newBuilder()
                        .setEnabled(enabled)
                        .build())
                .build())
        .getStatus();
  }

  @Test
  void testSegmentBasedApiNamingRules() {
    ApiNamingRuleDetails firstCreatedApiNamingRuleDetails =
        this.spanProcessingConfigServiceStub
            .createApiNamingRule(
                CreateApiNamingRuleRequest.newBuilder()
                    .setRuleInfo(
                        ApiNamingRuleInfo.newBuilder()
                            .setName("ruleName1")
                            .setDisabled(true)
                            .setRuleConfig(
                                buildSegmentMatchingBasedConfig(List.of("regex"), List.of("value")))
                            .setFilter(buildTestFilter()))
                    .build())
            .getRuleDetails();
    ApiNamingRule firstCreatedApiNamingRule = firstCreatedApiNamingRuleDetails.getRule();
    Timestamp expectedTimestamp = Timestamp.newBuilder().setSeconds(100).build();
    assertEquals(
        expectedTimestamp, firstCreatedApiNamingRuleDetails.getMetadata().getCreationTimestamp());
    assertEquals(
        expectedTimestamp,
        firstCreatedApiNamingRuleDetails.getMetadata().getLastUpdatedTimestamp());

    ApiNamingRule secondCreatedApiNamingRule =
        this.spanProcessingConfigServiceStub
            .createApiNamingRule(
                CreateApiNamingRuleRequest.newBuilder()
                    .setRuleInfo(
                        ApiNamingRuleInfo.newBuilder()
                            .setName("ruleName2")
                            .setDisabled(true)
                            .setRuleConfig(
                                buildSegmentMatchingBasedConfig(List.of("regex"), List.of("value")))
                            .setFilter(buildTestFilter()))
                    .build())
            .getRuleDetails()
            .getRule();

    List<ApiNamingRule> apiNamingRules = getAllApiNamingRules();
    assertEquals(2, apiNamingRules.size());
    assertTrue(apiNamingRules.contains(firstCreatedApiNamingRule));
    assertTrue(apiNamingRules.contains(secondCreatedApiNamingRule));

    ApiNamingRule updatedFirstApiNamingRule =
        this.spanProcessingConfigServiceStub
            .updateApiNamingRule(
                UpdateApiNamingRuleRequest.newBuilder()
                    .setRule(
                        UpdateApiNamingRule.newBuilder()
                            .setId(firstCreatedApiNamingRule.getId())
                            .setName("updatedRuleName1")
                            .setDisabled(false)
                            .setRuleConfig(
                                buildSegmentMatchingBasedConfig(List.of("regex"), List.of("value")))
                            .setFilter(buildTestFilter()))
                    .build())
            .getRuleDetails()
            .getRule();
    assertEquals("updatedRuleName1", updatedFirstApiNamingRule.getRuleInfo().getName());
    assertFalse(updatedFirstApiNamingRule.getRuleInfo().getDisabled());

    apiNamingRules = getAllApiNamingRules();
    assertEquals(2, apiNamingRules.size());
    assertTrue(apiNamingRules.contains(updatedFirstApiNamingRule));

    this.spanProcessingConfigServiceStub.deleteApiNamingRule(
        DeleteApiNamingRuleRequest.newBuilder().setId(firstCreatedApiNamingRule.getId()).build());

    apiNamingRules = getAllApiNamingRules();
    assertEquals(1, apiNamingRules.size());
    assertEquals(secondCreatedApiNamingRule, apiNamingRules.get(0));
  }

  // TODO: remove this test after migration of upstream services and configs
  @Test
  void testApiSpecBasedApiNamingRules() {
    List<ApiNamingRuleDetails> firstTwoCreatedApiNamingRuleDetails =
        this.spanProcessingConfigServiceStub
            .createApiNamingRules(
                CreateApiNamingRulesRequest.newBuilder()
                    .addAllRulesInfo(
                        List.of(
                            ApiNamingRuleInfo.newBuilder()
                                .setName("ruleName1")
                                .setDisabled(true)
                                .setRuleConfig(
                                    buildApiSpecBasedConfig(
                                        "id1", List.of("regex"), List.of("value")))
                                .setFilter(buildTestFilter())
                                .build(),
                            ApiNamingRuleInfo.newBuilder()
                                .setName("ruleName2")
                                .setDisabled(true)
                                .setRuleConfig(
                                    buildApiSpecBasedConfig(
                                        "id2", List.of("regex"), List.of("value")))
                                .setFilter(buildTestFilter())
                                .build()))
                    .build())
            .getRulesDetailsList();

    ApiNamingRuleDetails firstCreatedApiNamingRuleDetails =
        firstTwoCreatedApiNamingRuleDetails.get(1);
    ApiNamingRule firstCreatedApiNamingRule = firstTwoCreatedApiNamingRuleDetails.get(1).getRule();
    ApiNamingRule secondCreatedApiNamingRule = firstTwoCreatedApiNamingRuleDetails.get(0).getRule();

    List<ApiNamingRule> apiNamingRules = getAllApiNamingRules();
    assertEquals(2, apiNamingRules.size());
    assertTrue(apiNamingRules.contains(firstCreatedApiNamingRule));
    assertTrue(apiNamingRules.contains(secondCreatedApiNamingRule));

    Timestamp expectedTimestamp = Timestamp.newBuilder().setSeconds(100).build();
    assertEquals(
        expectedTimestamp, firstCreatedApiNamingRuleDetails.getMetadata().getCreationTimestamp());
    assertEquals(
        expectedTimestamp,
        firstCreatedApiNamingRuleDetails.getMetadata().getLastUpdatedTimestamp());

    ApiNamingRule thirdCreatedApiNamingRule =
        this.spanProcessingConfigServiceStub
            .createApiNamingRule(
                CreateApiNamingRuleRequest.newBuilder()
                    .setRuleInfo(
                        ApiNamingRuleInfo.newBuilder()
                            .setName("ruleName3")
                            .setDisabled(true)
                            .setRuleConfig(
                                buildApiSpecBasedConfig("id3", List.of("regex"), List.of("value")))
                            .setFilter(buildTestFilter()))
                    .build())
            .getRuleDetails()
            .getRule();

    apiNamingRules = getAllApiNamingRules();
    assertEquals(3, apiNamingRules.size());
    assertTrue(apiNamingRules.contains(firstCreatedApiNamingRule));
    assertTrue(apiNamingRules.contains(secondCreatedApiNamingRule));
    assertTrue(apiNamingRules.contains(thirdCreatedApiNamingRule));

    ApiNamingRule updatedFirstApiNamingRule =
        this.spanProcessingConfigServiceStub
            .updateApiNamingRule(
                UpdateApiNamingRuleRequest.newBuilder()
                    .setRule(
                        UpdateApiNamingRule.newBuilder()
                            .setId(firstCreatedApiNamingRule.getId())
                            .setName("updatedRuleName1")
                            .setDisabled(false)
                            .setRuleConfig(
                                buildApiSpecBasedConfig("id1", List.of("regex"), List.of("value")))
                            .setFilter(buildTestFilter()))
                    .build())
            .getRuleDetails()
            .getRule();
    assertEquals("updatedRuleName1", updatedFirstApiNamingRule.getRuleInfo().getName());
    assertFalse(updatedFirstApiNamingRule.getRuleInfo().getDisabled());

    apiNamingRules = getAllApiNamingRules();
    assertEquals(3, apiNamingRules.size());
    assertTrue(apiNamingRules.contains(updatedFirstApiNamingRule));

    List<ApiNamingRule> updatedSecondAndThirdApiNamingRules =
        this.spanProcessingConfigServiceStub
            .updateApiNamingRules(
                UpdateApiNamingRulesRequest.newBuilder()
                    .addAllRules(
                        List.of(
                            UpdateApiNamingRule.newBuilder()
                                .setId(secondCreatedApiNamingRule.getId())
                                .setName("updatedRuleName2")
                                .setDisabled(false)
                                .setRuleConfig(
                                    buildApiSpecBasedConfig(
                                        "id2", List.of("regex"), List.of("value")))
                                .setFilter(buildTestFilter())
                                .build(),
                            UpdateApiNamingRule.newBuilder()
                                .setId(thirdCreatedApiNamingRule.getId())
                                .setName("updatedRuleName3")
                                .setDisabled(false)
                                .setRuleConfig(
                                    buildApiSpecBasedConfig(
                                        "id3", List.of("regex"), List.of("value")))
                                .setFilter(buildTestFilter())
                                .build()))
                    .build())
            .getRulesDetailsList()
            .stream()
            .map(ApiNamingRuleDetails::getRule)
            .collect(Collectors.toUnmodifiableList());

    apiNamingRules = getAllApiNamingRules();
    assertEquals(3, apiNamingRules.size());
    assertTrue(apiNamingRules.contains(updatedSecondAndThirdApiNamingRules.get(0)));
    assertTrue(apiNamingRules.contains(updatedSecondAndThirdApiNamingRules.get(1)));

    this.spanProcessingConfigServiceStub.deleteApiNamingRule(
        DeleteApiNamingRuleRequest.newBuilder().setId(firstCreatedApiNamingRule.getId()).build());

    apiNamingRules = getAllApiNamingRules();
    assertEquals(2, apiNamingRules.size());
    assertTrue(apiNamingRules.contains(updatedSecondAndThirdApiNamingRules.get(0)));
    assertTrue(apiNamingRules.contains(updatedSecondAndThirdApiNamingRules.get(1)));

    this.spanProcessingConfigServiceStub.deleteApiNamingRules(
        DeleteApiNamingRulesRequest.newBuilder()
            .addAllIds(
                List.of(secondCreatedApiNamingRule.getId(), thirdCreatedApiNamingRule.getId()))
            .build());

    apiNamingRules = getAllApiNamingRules();
    assertTrue(apiNamingRules.isEmpty());
  }

  @Test
  void testAstScanBasedApiNamingRules() {
    List<ApiNamingRuleDetails> firstTwoCreatedApiNamingRuleDetails =
        this.spanProcessingConfigServiceStub
            .createApiNamingRules(
                CreateApiNamingRulesRequest.newBuilder()
                    .addAllRulesInfo(
                        List.of(
                            ApiNamingRuleInfo.newBuilder()
                                .setName("ruleName1")
                                .setDisabled(true)
                                .setRuleConfig(
                                    buildAstScanBasedConfig(
                                        "scanId1",
                                        "apiSpecId1",
                                        List.of("regex"),
                                        List.of("value")))
                                .setFilter(buildTestFilter())
                                .build(),
                            ApiNamingRuleInfo.newBuilder()
                                .setName("ruleName2")
                                .setDisabled(true)
                                .setRuleConfig(
                                    buildAstScanBasedConfig(
                                        "scanId2",
                                        "apiSpecId2",
                                        List.of("regex"),
                                        List.of("value")))
                                .setFilter(buildTestFilter())
                                .build()))
                    .build())
            .getRulesDetailsList();

    ApiNamingRuleDetails firstCreatedApiNamingRuleDetails =
        firstTwoCreatedApiNamingRuleDetails.get(1);
    ApiNamingRule firstCreatedApiNamingRule = firstTwoCreatedApiNamingRuleDetails.get(1).getRule();
    ApiNamingRule secondCreatedApiNamingRule = firstTwoCreatedApiNamingRuleDetails.get(0).getRule();

    List<ApiNamingRule> apiNamingRules = getAllApiNamingRules();
    assertEquals(2, apiNamingRules.size());
    assertTrue(apiNamingRules.contains(firstCreatedApiNamingRule));
    assertTrue(apiNamingRules.contains(secondCreatedApiNamingRule));

    Timestamp expectedTimestamp = Timestamp.newBuilder().setSeconds(100).build();
    assertEquals(
        expectedTimestamp, firstCreatedApiNamingRuleDetails.getMetadata().getCreationTimestamp());
    assertEquals(
        expectedTimestamp,
        firstCreatedApiNamingRuleDetails.getMetadata().getLastUpdatedTimestamp());

    ApiNamingRule thirdCreatedApiNamingRule =
        this.spanProcessingConfigServiceStub
            .createApiNamingRule(
                CreateApiNamingRuleRequest.newBuilder()
                    .setRuleInfo(
                        ApiNamingRuleInfo.newBuilder()
                            .setName("ruleName3")
                            .setDisabled(true)
                            .setRuleConfig(
                                buildAstScanBasedConfig(
                                    "scanId3", "apiSpecId3", List.of("regex"), List.of("value")))
                            .setFilter(buildTestFilter()))
                    .build())
            .getRuleDetails()
            .getRule();

    apiNamingRules = getAllApiNamingRules();
    assertEquals(3, apiNamingRules.size());
    assertTrue(apiNamingRules.contains(firstCreatedApiNamingRule));
    assertTrue(apiNamingRules.contains(secondCreatedApiNamingRule));
    assertTrue(apiNamingRules.contains(thirdCreatedApiNamingRule));

    ApiNamingRule updatedFirstApiNamingRule =
        this.spanProcessingConfigServiceStub
            .updateApiNamingRule(
                UpdateApiNamingRuleRequest.newBuilder()
                    .setRule(
                        UpdateApiNamingRule.newBuilder()
                            .setId(firstCreatedApiNamingRule.getId())
                            .setName("updatedRuleName1")
                            .setDisabled(false)
                            .setRuleConfig(
                                buildAstScanBasedConfig(
                                    "scanId1", "apiSpecId1", List.of("regex"), List.of("value")))
                            .setFilter(buildTestFilter()))
                    .build())
            .getRuleDetails()
            .getRule();
    assertEquals("updatedRuleName1", updatedFirstApiNamingRule.getRuleInfo().getName());
    assertFalse(updatedFirstApiNamingRule.getRuleInfo().getDisabled());

    apiNamingRules = getAllApiNamingRules();
    assertEquals(3, apiNamingRules.size());
    assertTrue(apiNamingRules.contains(updatedFirstApiNamingRule));

    List<ApiNamingRule> updatedSecondAndThirdApiNamingRules =
        this.spanProcessingConfigServiceStub
            .updateApiNamingRules(
                UpdateApiNamingRulesRequest.newBuilder()
                    .addAllRules(
                        List.of(
                            UpdateApiNamingRule.newBuilder()
                                .setId(secondCreatedApiNamingRule.getId())
                                .setName("updatedRuleName2")
                                .setDisabled(false)
                                .setRuleConfig(
                                    buildAstScanBasedConfig(
                                        "scanId2",
                                        "apiSpecId2",
                                        List.of("regex"),
                                        List.of("value")))
                                .setFilter(buildTestFilter())
                                .build(),
                            UpdateApiNamingRule.newBuilder()
                                .setId(thirdCreatedApiNamingRule.getId())
                                .setName("updatedRuleName3")
                                .setDisabled(false)
                                .setRuleConfig(
                                    buildAstScanBasedConfig(
                                        "scanId3",
                                        "apiSpecId3",
                                        List.of("regex"),
                                        List.of("value")))
                                .setFilter(buildTestFilter())
                                .build()))
                    .build())
            .getRulesDetailsList()
            .stream()
            .map(ApiNamingRuleDetails::getRule)
            .collect(Collectors.toUnmodifiableList());

    apiNamingRules = getAllApiNamingRules();
    assertEquals(3, apiNamingRules.size());
    assertTrue(apiNamingRules.contains(updatedSecondAndThirdApiNamingRules.get(0)));
    assertTrue(apiNamingRules.contains(updatedSecondAndThirdApiNamingRules.get(1)));

    this.spanProcessingConfigServiceStub.deleteApiNamingRule(
        DeleteApiNamingRuleRequest.newBuilder().setId(firstCreatedApiNamingRule.getId()).build());

    apiNamingRules = getAllApiNamingRules();
    assertEquals(2, apiNamingRules.size());
    assertTrue(apiNamingRules.contains(updatedSecondAndThirdApiNamingRules.get(0)));
    assertTrue(apiNamingRules.contains(updatedSecondAndThirdApiNamingRules.get(1)));

    this.spanProcessingConfigServiceStub.deleteApiNamingRules(
        DeleteApiNamingRulesRequest.newBuilder()
            .addAllIds(
                List.of(secondCreatedApiNamingRule.getId(), thirdCreatedApiNamingRule.getId()))
            .build());

    apiNamingRules = getAllApiNamingRules();
    assertTrue(apiNamingRules.isEmpty());
  }

  @Test
  void testApiSpecBasedApiNamingRules_handleDuplication() {
    List<ApiNamingRuleDetails> firstTwoCreatedApiNamingRuleDetails =
        this.spanProcessingConfigServiceStub
            .createApiNamingRules(
                CreateApiNamingRulesRequest.newBuilder()
                    .addAllRulesInfo(
                        List.of(
                            ApiNamingRuleInfo.newBuilder()
                                .setName("ruleName1")
                                .setDisabled(true)
                                .setRuleConfig(
                                    buildApiSpecBasedConfig(
                                        List.of("id1"), List.of("regex"), List.of("value")))
                                .setFilter(buildTestFilter())
                                .build(),
                            ApiNamingRuleInfo.newBuilder()
                                .setName("ruleName2")
                                .setDisabled(true)
                                .setRuleConfig(
                                    buildApiSpecBasedConfig(
                                        List.of("id2"), List.of("regex1"), List.of("value1")))
                                .setFilter(buildTestFilter())
                                .build()))
                    .build())
            .getRulesDetailsList();

    ApiNamingRuleDetails firstCreatedApiNamingRuleDetails =
        firstTwoCreatedApiNamingRuleDetails.get(1);
    ApiNamingRule firstCreatedApiNamingRule = firstTwoCreatedApiNamingRuleDetails.get(1).getRule();
    ApiNamingRule secondCreatedApiNamingRule = firstTwoCreatedApiNamingRuleDetails.get(0).getRule();

    List<ApiNamingRule> apiNamingRules = getAllApiNamingRules();
    assertEquals(2, apiNamingRules.size());
    assertTrue(apiNamingRules.contains(firstCreatedApiNamingRule));
    assertTrue(apiNamingRules.contains(secondCreatedApiNamingRule));

    Timestamp expectedTimestamp = Timestamp.newBuilder().setSeconds(100).build();
    assertEquals(
        expectedTimestamp, firstCreatedApiNamingRuleDetails.getMetadata().getCreationTimestamp());
    assertEquals(
        expectedTimestamp,
        firstCreatedApiNamingRuleDetails.getMetadata().getLastUpdatedTimestamp());

    ApiNamingRule thirdCreatedApiNamingRule =
        this.spanProcessingConfigServiceStub
            .createApiNamingRules(
                CreateApiNamingRulesRequest.newBuilder()
                    .addAllRulesInfo(
                        List.of(
                            ApiNamingRuleInfo.newBuilder()
                                .setName("ruleName3")
                                .setDisabled(true)
                                .setRuleConfig(
                                    buildApiSpecBasedConfig(
                                        List.of("id3"), List.of("regex"), List.of("value")))
                                .setFilter(buildTestFilter())
                                .build()))
                    .build())
            .getRulesDetailsList()
            .get(0)
            .getRule();

    apiNamingRules = getAllApiNamingRules();
    assertEquals(2, apiNamingRules.size());
    assertTrue(apiNamingRules.contains(firstCreatedApiNamingRule));
    assertTrue(
        apiNamingRules.contains(
            ApiNamingRule.newBuilder()
                .setId(secondCreatedApiNamingRule.getId())
                .setRuleInfo(
                    ApiNamingRuleInfo.newBuilder()
                        .setName("ruleName1")
                        .setDisabled(true)
                        .setRuleConfig(
                            buildApiSpecBasedConfig(
                                List.of("id1", "id3"), List.of("regex"), List.of("value")))
                        .setFilter(buildTestFilter())
                        .build())
                .build()));

    ApiNamingRule updatedFirstApiNamingRule =
        this.spanProcessingConfigServiceStub
            .updateApiNamingRule(
                UpdateApiNamingRuleRequest.newBuilder()
                    .setRule(
                        UpdateApiNamingRule.newBuilder()
                            .setId(firstCreatedApiNamingRule.getId())
                            .setName("updatedRuleName1")
                            .setDisabled(false)
                            .setRuleConfig(
                                buildApiSpecBasedConfig(
                                    List.of("id1", "id4"), List.of("regex"), List.of("value")))
                            .setFilter(buildTestFilter()))
                    .build())
            .getRuleDetails()
            .getRule();
    assertEquals("updatedRuleName1", updatedFirstApiNamingRule.getRuleInfo().getName());
    assertFalse(updatedFirstApiNamingRule.getRuleInfo().getDisabled());

    apiNamingRules = getAllApiNamingRules();
    assertEquals(2, apiNamingRules.size());
    assertTrue(apiNamingRules.contains(updatedFirstApiNamingRule));

    List<ApiNamingRule> updatedFirstAndSecondApiNamingRules =
        this.spanProcessingConfigServiceStub
            .updateApiNamingRules(
                UpdateApiNamingRulesRequest.newBuilder()
                    .addAllRules(
                        List.of(
                            UpdateApiNamingRule.newBuilder()
                                .setId(firstCreatedApiNamingRule.getId())
                                .setName("updatedRuleName1")
                                .setDisabled(false)
                                .setRuleConfig(
                                    buildApiSpecBasedConfig(
                                        List.of("id1", "id4", "id5"),
                                        List.of("regex"),
                                        List.of("value")))
                                .setFilter(buildTestFilter())
                                .build(),
                            UpdateApiNamingRule.newBuilder()
                                .setId(secondCreatedApiNamingRule.getId())
                                .setName("updatedRuleName2")
                                .setDisabled(false)
                                .setRuleConfig(
                                    buildApiSpecBasedConfig(
                                        "id3", List.of("regex1"), List.of("value1")))
                                .setFilter(buildTestFilter())
                                .build()))
                    .build())
            .getRulesDetailsList()
            .stream()
            .map(ApiNamingRuleDetails::getRule)
            .collect(Collectors.toUnmodifiableList());

    apiNamingRules = getAllApiNamingRules();
    assertEquals(2, apiNamingRules.size());
    assertTrue(apiNamingRules.contains(updatedFirstAndSecondApiNamingRules.get(0)));
    assertTrue(apiNamingRules.contains(updatedFirstAndSecondApiNamingRules.get(1)));

    this.spanProcessingConfigServiceStub.deleteApiNamingRule(
        DeleteApiNamingRuleRequest.newBuilder().setId(firstCreatedApiNamingRule.getId()).build());

    apiNamingRules = getAllApiNamingRules();
    assertEquals(1, apiNamingRules.size());
    assertTrue(apiNamingRules.contains(updatedFirstAndSecondApiNamingRules.get(1)));

    this.spanProcessingConfigServiceStub.deleteApiNamingRules(
        DeleteApiNamingRulesRequest.newBuilder()
            .addAllIds(List.of(secondCreatedApiNamingRule.getId()))
            .build());

    apiNamingRules = getAllApiNamingRules();
    assertTrue(apiNamingRules.isEmpty());
  }

  // TODO: remove this test after migration of upstream services and configs
  @Test
  void testApiSpecBasedApiNamingRules_checkNamingRuleMatchWithOlderFormat() {
    // shouldn't match with a naming rule of older format even if identification regexes,replacement
    // values and filter are same
    ApiNamingRuleDetails firstCreatedApiNamingRuleDetails =
        this.spanProcessingConfigServiceStub
            .createApiNamingRule(
                CreateApiNamingRuleRequest.newBuilder()
                    .setRuleInfo(
                        ApiNamingRuleInfo.newBuilder()
                            .setName("ruleName1")
                            .setDisabled(true)
                            .setRuleConfig(
                                buildApiSpecBasedConfig("id1", List.of("regex"), List.of("value")))
                            .setFilter(buildTestFilter())
                            .build())
                    .build())
            .getRuleDetails();

    ApiNamingRule firstCreatedApiNamingRule = firstCreatedApiNamingRuleDetails.getRule();

    List<ApiNamingRule> apiNamingRules = getAllApiNamingRules();
    assertEquals(1, apiNamingRules.size());
    assertTrue(apiNamingRules.contains(firstCreatedApiNamingRule));

    Timestamp expectedTimestamp = Timestamp.newBuilder().setSeconds(100).build();
    assertEquals(
        expectedTimestamp, firstCreatedApiNamingRuleDetails.getMetadata().getCreationTimestamp());
    assertEquals(
        expectedTimestamp,
        firstCreatedApiNamingRuleDetails.getMetadata().getLastUpdatedTimestamp());

    ApiNamingRule secondCreatedApiNamingRule =
        this.spanProcessingConfigServiceStub
            .createApiNamingRules(
                CreateApiNamingRulesRequest.newBuilder()
                    .addAllRulesInfo(
                        List.of(
                            ApiNamingRuleInfo.newBuilder()
                                .setName("ruleName2")
                                .setDisabled(true)
                                .setRuleConfig(
                                    buildApiSpecBasedConfig(
                                        List.of("id3"), List.of("regex"), List.of("value")))
                                .setFilter(buildTestFilter())
                                .build()))
                    .build())
            .getRulesDetailsList()
            .get(0)
            .getRule();

    apiNamingRules = getAllApiNamingRules();
    assertEquals(2, apiNamingRules.size());
    assertTrue(apiNamingRules.contains(firstCreatedApiNamingRule));
    assertTrue(apiNamingRules.contains(secondCreatedApiNamingRule));
  }

  private List<ApiNamingRule> getAllApiNamingRules() {
    return this.spanProcessingConfigServiceStub
        .getAllApiNamingRules(GetAllApiNamingRulesRequest.newBuilder().build())
        .getRuleDetailsList()
        .stream()
        .map(ApiNamingRuleDetails::getRule)
        .collect(Collectors.toUnmodifiableList());
  }

  private SpanFilter buildTestFilter() {
    return SpanFilter.newBuilder()
        .setRelationalSpanFilter(
            RelationalSpanFilterExpression.newBuilder()
                .setField(Field.FIELD_SERVICE_NAME)
                .setOperator(RELATIONAL_OPERATOR_CONTAINS)
                .setRightOperand(SpanFilterValue.newBuilder().setStringValue("a")))
        .build();
  }

  private ApiNamingRuleConfig buildSegmentMatchingBasedConfig(
      List<String> regexes, List<String> values) {
    return ApiNamingRuleConfig.newBuilder()
        .setSegmentMatchingBasedConfig(
            SegmentMatchingBasedConfig.newBuilder().addAllRegexes(regexes).addAllValues(values))
        .build();
  }

  private ApiNamingRuleConfig buildAstScanBasedConfig(
      String scanId, String apiSpecId, List<String> regexes, List<String> values) {
    return ApiNamingRuleConfig.newBuilder()
        .setAstScanBasedConfig(
            AstScanBasedConfig.newBuilder()
                .setScanId(scanId)
                .setApiSpecId(apiSpecId)
                .addAllRegexes(regexes)
                .addAllValues(values))
        .build();
  }

  private ApiNamingRuleConfig buildApiSpecBasedConfig(
      String id, List<String> regexes, List<String> values) {
    return ApiNamingRuleConfig.newBuilder()
        .setApiSpecBasedConfig(
            ApiSpecBasedConfig.newBuilder()
                .setApiSpecId(id)
                .addAllRegexes(regexes)
                .addAllValues(values))
        .build();
  }

  private ApiNamingRuleConfig buildApiSpecBasedConfig(
      List<String> id, List<String> regexes, List<String> values) {
    return ApiNamingRuleConfig.newBuilder()
        .setApiSpecBasedConfig(
            ApiSpecBasedConfig.newBuilder()
                .addAllApiSpecIds(id)
                .addAllRegexes(regexes)
                .addAllValues(values))
        .build();
  }
}
