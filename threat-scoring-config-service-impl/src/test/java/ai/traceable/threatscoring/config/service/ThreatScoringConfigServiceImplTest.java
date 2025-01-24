package ai.traceable.threatscoring.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.threatscoring.config.service.v1.AnomalousEventConfidenceConfig;
import ai.traceable.threatscoring.config.service.v1.DeleteEventConfidenceScoringConfigOverridesRequest;
import ai.traceable.threatscoring.config.service.v1.DeleteEventConfidenceScoringConfigOverridesResponse;
import ai.traceable.threatscoring.config.service.v1.DeleteThreatActivityConfidenceScoringConfigOverridesRequest;
import ai.traceable.threatscoring.config.service.v1.DeleteThreatActivityConfidenceScoringConfigOverridesResponse;
import ai.traceable.threatscoring.config.service.v1.EnvironmentScope;
import ai.traceable.threatscoring.config.service.v1.EventConfidenceScoringConfig;
import ai.traceable.threatscoring.config.service.v1.GetScopedThreatScoringConfigsRequest;
import ai.traceable.threatscoring.config.service.v1.OverrideEventConfidenceScoringConfigRequest;
import ai.traceable.threatscoring.config.service.v1.OverrideEventConfidenceScoringConfigResponse;
import ai.traceable.threatscoring.config.service.v1.OverrideThreatActivityConfidenceScoringConfigRequest;
import ai.traceable.threatscoring.config.service.v1.OverrideThreatActivityConfidenceScoringConfigResponse;
import ai.traceable.threatscoring.config.service.v1.ScopedThreatScoringConfigs;
import ai.traceable.threatscoring.config.service.v1.ScoringLevelConfig;
import ai.traceable.threatscoring.config.service.v1.ThreatActivityConfidenceScoringConfig;
import ai.traceable.threatscoring.config.service.v1.ThreatScoringConfigScope;
import ai.traceable.threatscoring.config.service.v1.ThreatScoringConfigServiceGrpc;
import ai.traceable.threatscoring.config.service.v1.ThreatScoringConfigs;
import ai.traceable.threatscoring.config.service.validation.ThreatScoringConfigRequestValidator;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ThreatScoringConfigServiceImplTest {
  private ThreatScoringConfigServiceGrpc.ThreatScoringConfigServiceBlockingStub
      threatScoringConfigServiceStub;
  private MockGenericConfigService mockGenericConfigService;
  private EventConfidenceScoringConfigManager eventConfidenceScoringConfigManager;
  private ThreatActivityConfidenceScoringConfigManager threatActivityConfidenceScoringConfigManager;

  @BeforeEach
  void beforeEach() {
    this.mockGenericConfigService =
        new MockGenericConfigService()
            .mockUpsert()
            .mockGet()
            .mockGetAll()
            .mockDelete()
            .mockUpsertAll();

    ConfigServiceGrpc.ConfigServiceBlockingStub genericStub =
        ConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());

    ConfigChangeEventGenerator configChangeEventGenerator = mock(ConfigChangeEventGenerator.class);
    this.threatScoringConfigServiceStub =
        ThreatScoringConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
    eventConfidenceScoringConfigManager = mock(EventConfidenceScoringConfigManager.class);
    threatActivityConfidenceScoringConfigManager =
        mock(ThreatActivityConfidenceScoringConfigManager.class);
    this.mockGenericConfigService
        .addService(
            new ThreatScoringConfigServiceImpl(
                eventConfidenceScoringConfigManager,
                threatActivityConfidenceScoringConfigManager,
                new ThreatScoringConfigRequestValidator()))
        .start();
  }

  @AfterEach
  void afterEach() {
    this.mockGenericConfigService.shutdown();
  }

  @Test
  void test_get() {
    GetScopedThreatScoringConfigsRequest request =
        GetScopedThreatScoringConfigsRequest.newBuilder()
            .setConfigScope(
                ThreatScoringConfigScope.newBuilder()
                    .setEnvironmentScope(
                        EnvironmentScope.newBuilder().setEnvironmentId("env").build())
                    .build())
            .build();
    EventConfidenceScoringConfig eventConfidenceScoringConfig =
        EventConfidenceScoringConfig.newBuilder()
            .setAnomalousEventConfidenceConfig(
                AnomalousEventConfidenceConfig.newBuilder()
                    .setLevelConfig(
                        ScoringLevelConfig.newBuilder().setHighLevelMinScore(10).build())
                    .build())
            .build();
    when(eventConfidenceScoringConfigManager.getResolvedConfig(eq(request), any()))
        .thenReturn(eventConfidenceScoringConfig);
    when(threatActivityConfidenceScoringConfigManager.getResolvedConfig(eq(request), any()))
        .thenReturn(ThreatActivityConfidenceScoringConfig.getDefaultInstance());
    assertEquals(
        ScopedThreatScoringConfigs.newBuilder()
            .setConfigScope(request.getConfigScope())
            .setConfigs(
                ThreatScoringConfigs.newBuilder()
                    .setThreatActivityConfidenceScoringConfig(
                        ThreatActivityConfidenceScoringConfig.newBuilder().build())
                    .setEventConfidenceScoringConfig(eventConfidenceScoringConfig))
            .build(),
        threatScoringConfigServiceStub
            .getScopedThreatScoringConfigs(request)
            .getScopedThreatScoringConfigs());

    request = GetScopedThreatScoringConfigsRequest.getDefaultInstance();
    when(eventConfidenceScoringConfigManager.getResolvedConfig(eq(request), any()))
        .thenReturn(eventConfidenceScoringConfig);
    when(threatActivityConfidenceScoringConfigManager.getResolvedConfig(eq(request), any()))
        .thenReturn(ThreatActivityConfidenceScoringConfig.getDefaultInstance());
    assertEquals(
        ScopedThreatScoringConfigs.newBuilder()
            .setConfigScope(request.getConfigScope())
            .setConfigs(
                ThreatScoringConfigs.newBuilder()
                    .setThreatActivityConfidenceScoringConfig(
                        ThreatActivityConfidenceScoringConfig.newBuilder().build())
                    .setEventConfidenceScoringConfig(eventConfidenceScoringConfig))
            .build(),
        threatScoringConfigServiceStub
            .getScopedThreatScoringConfigs(request)
            .getScopedThreatScoringConfigs());
  }

  @Test
  void test_overrideEventConfidenceScoringConfig() {
    EventConfidenceScoringConfig eventConfidenceScoringConfig =
        EventConfidenceScoringConfig.newBuilder()
            .setAnomalousEventConfidenceConfig(
                AnomalousEventConfidenceConfig.newBuilder()
                    .setLevelConfig(
                        ScoringLevelConfig.newBuilder().setHighLevelMinScore(10).build())
                    .build())
            .build();

    OverrideEventConfidenceScoringConfigRequest request =
        OverrideEventConfidenceScoringConfigRequest.newBuilder()
            .setEventConfidenceScoringConfig(eventConfidenceScoringConfig)
            .build();
    when(eventConfidenceScoringConfigManager.getDefaultEventConfidenceScoringConfig())
        .thenReturn(EventConfidenceScoringConfig.getDefaultInstance());
    when(eventConfidenceScoringConfigManager.overrideConfig(eq(request), any()))
        .thenReturn(eventConfidenceScoringConfig);
    assertEquals(
        OverrideEventConfidenceScoringConfigResponse.newBuilder()
            .setDefaultEventConfidenceScoringConfig(
                EventConfidenceScoringConfig.getDefaultInstance())
            .setResolvedEventConfidenceScoringConfig(eventConfidenceScoringConfig)
            .build(),
        threatScoringConfigServiceStub.overrideEventConfidenceScoringConfig(request));
  }

  @Test
  void test_overrideThreatActivityConfidenceScoringConfig() {
    ThreatActivityConfidenceScoringConfig threatActivityConfidenceScoringConfig =
        ThreatActivityConfidenceScoringConfig.newBuilder()
            .setPreviousActivityEventCountLevelConfig(
                ScoringLevelConfig.newBuilder().setMediumLevelMinScore(20))
            .build();

    OverrideThreatActivityConfidenceScoringConfigRequest request =
        OverrideThreatActivityConfidenceScoringConfigRequest.newBuilder()
            .setThreatActivityConfidenceScoringConfig(threatActivityConfidenceScoringConfig)
            .build();
    when(threatActivityConfidenceScoringConfigManager
            .getDefaultThreatActivityConfidenceScoringConfig())
        .thenReturn(ThreatActivityConfidenceScoringConfig.getDefaultInstance());
    when(threatActivityConfidenceScoringConfigManager.overrideConfig(eq(request), any()))
        .thenReturn(threatActivityConfidenceScoringConfig);
    assertEquals(
        OverrideThreatActivityConfidenceScoringConfigResponse.newBuilder()
            .setDefaultThreatActivityConfidenceScoringConfig(
                ThreatActivityConfidenceScoringConfig.getDefaultInstance())
            .setResolvedThreatActivityConfidenceScoringConfig(threatActivityConfidenceScoringConfig)
            .build(),
        threatScoringConfigServiceStub.overrideThreatActivityConfidenceScoringConfig(request));
  }

  @Test
  void test_deleteEventConfidenceScoringConfigOverrides() {
    DeleteEventConfidenceScoringConfigOverridesRequest request =
        DeleteEventConfidenceScoringConfigOverridesRequest.newBuilder().build();
    when(eventConfidenceScoringConfigManager.getDefaultEventConfidenceScoringConfig())
        .thenReturn(EventConfidenceScoringConfig.getDefaultInstance());
    assertEquals(
        DeleteEventConfidenceScoringConfigOverridesResponse.newBuilder()
            .setDefaultEventConfidenceScoringConfig(
                EventConfidenceScoringConfig.getDefaultInstance())
            .build(),
        threatScoringConfigServiceStub.deleteEventConfidenceScoringConfigOverrides(request));
  }

  @Test
  void test_deleteThreatActivityConfidenceScoringConfigOverrides() {
    DeleteThreatActivityConfidenceScoringConfigOverridesRequest request =
        DeleteThreatActivityConfidenceScoringConfigOverridesRequest.newBuilder().build();
    when(threatActivityConfidenceScoringConfigManager
            .getDefaultThreatActivityConfidenceScoringConfig())
        .thenReturn(ThreatActivityConfidenceScoringConfig.getDefaultInstance());
    assertEquals(
        DeleteThreatActivityConfidenceScoringConfigOverridesResponse.newBuilder()
            .setDefaultThreatActivityConfidenceScoringConfig(
                ThreatActivityConfidenceScoringConfig.getDefaultInstance())
            .build(),
        threatScoringConfigServiceStub.deleteThreatActivityConfidenceScoringConfigOverrides(
            request));
  }
}
