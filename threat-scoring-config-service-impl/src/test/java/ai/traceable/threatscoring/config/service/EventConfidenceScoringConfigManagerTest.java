package ai.traceable.threatscoring.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.threatscoring.config.service.store.ScopedThreatScoringConfigsStore;
import ai.traceable.threatscoring.config.service.v1.AnomalousEventConfidenceConfig;
import ai.traceable.threatscoring.config.service.v1.EventConfidenceScoringConfig;
import ai.traceable.threatscoring.config.service.v1.GetScopedThreatScoringConfigsRequest;
import ai.traceable.threatscoring.config.service.v1.MaliciousSpanConfidenceScoring;
import ai.traceable.threatscoring.config.service.v1.OverrideEventConfidenceScoringConfigRequest;
import ai.traceable.threatscoring.config.service.v1.ScopedThreatScoringConfigs;
import ai.traceable.threatscoring.config.service.v1.ScoringLevelConfig;
import ai.traceable.threatscoring.config.service.v1.ThreatScoringConfigs;
import java.time.Instant;
import java.util.List;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class EventConfidenceScoringConfigManagerTest {
  @Mock private ScopedThreatScoringConfigsStore scopedThreatScoringConfigsStore;
  private EventConfidenceScoringConfigManager eventConfidenceScoringConfigManager;
  private ScopedThreatScoringConfigs defaultScopedThreatScoringConfigs;

  @BeforeEach
  void setup() {
    defaultScopedThreatScoringConfigs =
        ScopedThreatScoringConfigs.newBuilder()
            .setConfigs(
                ThreatScoringConfigs.newBuilder()
                    .setEventConfidenceScoringConfig(
                        EventConfidenceScoringConfig.newBuilder()
                            .setMaliciousSpanConfidenceScoring(
                                MaliciousSpanConfidenceScoring.newBuilder()
                                    .setLevelConfig(
                                        ScoringLevelConfig.newBuilder()
                                            .setMediumLevelMinScore(9)
                                            .setHighLevelMinScore(10)
                                            .build())
                                    .build()))
                    .build())
            .build();
    eventConfidenceScoringConfigManager =
        new EventConfidenceScoringConfigManager(
            new ThreatScoringConfigScopeUtils(),
            scopedThreatScoringConfigsStore,
            defaultScopedThreatScoringConfigs);
  }

  @Test
  void test_get() {

    when(scopedThreatScoringConfigsStore.fetchConfigsInContextOrder(any(), any()))
        .thenReturn(
            List.of(
                ScopedThreatScoringConfigs.newBuilder()
                    .setConfigs(
                        ThreatScoringConfigs.newBuilder()
                            .setEventConfidenceScoringConfig(
                                EventConfidenceScoringConfig.newBuilder()
                                    .setAnomalousEventConfidenceConfig(
                                        AnomalousEventConfidenceConfig.newBuilder()
                                            .setMinNumberOfUniqueUnexpectedCharacters(20))
                                    .setMaliciousSpanConfidenceScoring(
                                        MaliciousSpanConfidenceScoring.newBuilder()
                                            .setLevelConfig(
                                                ScoringLevelConfig.newBuilder()
                                                    .setHighLevelMinScore(5)
                                                    .build())
                                            .build()))
                            .build())
                    .build()));
    assertEquals(
        EventConfidenceScoringConfig.newBuilder()
            .setAnomalousEventConfidenceConfig(
                AnomalousEventConfidenceConfig.newBuilder()
                    .setMinNumberOfUniqueUnexpectedCharacters(20))
            .setMaliciousSpanConfidenceScoring(
                MaliciousSpanConfidenceScoring.newBuilder()
                    .setLevelConfig(
                        ScoringLevelConfig.newBuilder()
                            .setMediumLevelMinScore(9)
                            .setHighLevelMinScore(5)
                            .build())
                    .build())
            .build(),
        eventConfidenceScoringConfigManager.getResolvedConfig(
            GetScopedThreatScoringConfigsRequest.newBuilder().build(), mock(RequestContext.class)));
  }

  @Test
  void test_getDefaultEventConfidenceScoringConfig() {
    assertEquals(
        defaultScopedThreatScoringConfigs.getConfigs().getEventConfidenceScoringConfig(),
        eventConfidenceScoringConfigManager.getDefaultEventConfidenceScoringConfig());
  }

  @Test
  void test_overrideConfig() {
    ScopedThreatScoringConfigs config =
        ScopedThreatScoringConfigs.newBuilder()
            .setConfigs(
                ThreatScoringConfigs.newBuilder()
                    .setEventConfidenceScoringConfig(
                        EventConfidenceScoringConfig.newBuilder()
                            .setMaliciousSpanConfidenceScoring(
                                MaliciousSpanConfidenceScoring.newBuilder()
                                    .setLevelConfig(
                                        ScoringLevelConfig.newBuilder()
                                            .setMediumLevelMinScore(9)
                                            .setHighLevelMinScore(10)
                                            .build())
                                    .build()))
                    .build())
            .build();
    when(scopedThreatScoringConfigsStore.upsertObject(any(), any()))
        .thenReturn(new ContextualConfigObjectImpl(config));
    assertEquals(
        config.getConfigs().getEventConfidenceScoringConfig(),
        eventConfidenceScoringConfigManager.overrideConfig(
            OverrideEventConfidenceScoringConfigRequest.newBuilder()
                .setEventConfidenceScoringConfig(
                    config.getConfigs().getEventConfidenceScoringConfig())
                .build(),
            mock(RequestContext.class)));
  }

  static class ContextualConfigObjectImpl
      implements ContextualConfigObject<ScopedThreatScoringConfigs> {
    private ScopedThreatScoringConfigs data;

    private final Instant creationTimestamp;
    private final String createdByEmail;
    private final Instant lastUserUpdateTimestamp;
    private final String lastUserUpdateEmail;
    private final Instant lastUpdatedTimestamp;
    private final String lastUpdateEmail;

    ContextualConfigObjectImpl(ScopedThreatScoringConfigs data) {
      this.data = data;
      this.creationTimestamp = Instant.EPOCH;
      this.createdByEmail = "system";
      this.lastUserUpdateTimestamp = Instant.EPOCH;
      this.lastUserUpdateEmail = "system";
      this.lastUpdatedTimestamp = Instant.EPOCH;
      this.lastUpdateEmail = "system";
    }

    @Override
    public ScopedThreatScoringConfigs getData() {
      return data;
    }

    @Override
    public Instant getCreationTimestamp() {
      return creationTimestamp;
    }

    @Override
    public String getCreatedByEmail() {
      return createdByEmail;
    }

    @Override
    public Instant getLastUserUpdateTimestamp() {
      return lastUserUpdateTimestamp;
    }

    @Override
    public String getLastUserUpdateEmail() {
      return lastUserUpdateEmail;
    }

    @Override
    public Instant getLastUpdatedTimestamp() {
      return lastUpdatedTimestamp;
    }

    @Override
    public String getLastUpdateEmail() {
      return lastUpdateEmail;
    }

    @Override
    public String getContext() {
      return null;
    }
  }
}
