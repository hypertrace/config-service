package ai.traceable.threatscoring.config.service;

import ai.traceable.threatscoring.config.service.v1.ActorMaliciousConfidenceConfig;
import ai.traceable.threatscoring.config.service.v1.AnomalousEventConfidenceConfig;
import ai.traceable.threatscoring.config.service.v1.ApiTrendConfidenceConfig;
import ai.traceable.threatscoring.config.service.v1.EventConfidenceScoringConfig;
import ai.traceable.threatscoring.config.service.v1.IpReputationFactorMapping;
import ai.traceable.threatscoring.config.service.v1.MaliciousSpanConfidenceScoring;
import ai.traceable.threatscoring.config.service.v1.ResponseConfidenceConfig;
import ai.traceable.threatscoring.config.service.v1.ScoringLevelConfig;
import ai.traceable.threatscoring.config.service.v1.ThreatScoringConfigs;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class DefaultThreatScoringConfigTest {
  private final DefaultThreatScoringConfig defaultThreatScoringConfig =
      new DefaultThreatScoringConfig();

  @Test
  void test_event_confidence_mapping() {
    // Assert the expected count
    Assertions.assertEquals(
        564,
        defaultThreatScoringConfig
            .getDefaultConfig()
            .getConfigs()
            .getEventConfidenceScoringConfig()
            .getEventConfidenceMatrixConfig()
            .getEventConfidenceMappingCount());

    // Fetch the actual mapping
    var mappingList =
        defaultThreatScoringConfig
            .getDefaultConfig()
            .getConfigs()
            .getEventConfidenceScoringConfig()
            .getEventConfidenceMatrixConfig()
            .getEventConfidenceMappingList();
    Set<String> uniqueSubConfidences =
        mappingList.stream()
            .map(
                mapping ->
                    String.join(
                        ",",
                        mapping.getActorConfidence().toString(),
                        mapping.getMaliciousSpanConfidence().toString(),
                        mapping.getAnomalousConfidence().toString(),
                        mapping.getTrendConfidence().toString(),
                        mapping.getResponseConfidence().toString()))
            .collect(Collectors.toSet());
    Assertions.assertEquals(
        mappingList.size(),
        uniqueSubConfidences.size(),
        "Duplicate entries found for the confidence fields (BadActor, Malicious, Anomalous, Trend, Response)");
  }

  @Test
  void test_threat_activity_confidence_mapping() {
    Assertions.assertEquals(
        84,
        defaultThreatScoringConfig
            .getDefaultConfig()
            .getConfigs()
            .getThreatActivityConfidenceScoringConfig()
            .getThreatActivityConfidenceMatrixConfig()
            .getThreatActivityConfidenceMappingCount());
  }

  @Test
  void test_threat_scoring_config() {
    Assertions.assertEquals(
        ThreatScoringConfigs.newBuilder()
            .setEventConfidenceScoringConfig(
                EventConfidenceScoringConfig.newBuilder()
                    .setAnomalousEventConfidenceConfig(
                        AnomalousEventConfidenceConfig.newBuilder()
                            .setLevelConfig(
                                ScoringLevelConfig.newBuilder()
                                    .setMediumLevelMinScore(30)
                                    .setHighLevelMinScore(50)
                                    .setMaxNormalizedScore(10)
                                    .build())
                            .setMinNumberOfUniqueUnexpectedCharacters(3)
                            .setMinNumberOfUnlearntParamsInApi(25)
                            .build())
                    .setMaliciousSpanConfidenceScoring(
                        MaliciousSpanConfidenceScoring.newBuilder()
                            .setLevelConfig(
                                ScoringLevelConfig.newBuilder()
                                    .setMediumLevelMinScore(40)
                                    .setHighLevelMinScore(70)
                                    .setMaxNormalizedScore(10)
                                    .build())
                            .build())
                    .setActorMaliciousConfidenceConfig(
                        ActorMaliciousConfidenceConfig.newBuilder()
                            .setActorTrendFactor(1)
                            .setIpReputationFactorMapping(
                                IpReputationFactorMapping.newBuilder()
                                    .setLowIpReputationFactor(0.25)
                                    .setHighIpReputationFactor(0.5)
                                    .setCriticalIpReputationFactor(0.75)
                                    .build())
                            .setLevelConfig(
                                ScoringLevelConfig.newBuilder()
                                    .setMediumLevelMinScore(40)
                                    .setHighLevelMinScore(70)
                                    .setMaxNormalizedScore(7)
                                    .build())
                            .build())
                    .setResponseConfidenceConfig(
                        ResponseConfidenceConfig.newBuilder()
                            .setLevelConfig(
                                ScoringLevelConfig.newBuilder()
                                    .setMediumLevelMinScore(50)
                                    .setHighLevelMinScore(75)
                                    .setMaxNormalizedScore(4)
                                    .build())
                            .build())
                    .setApiTrendConfidenceConfig(
                        ApiTrendConfidenceConfig.newBuilder()
                            .setLevelConfig(
                                ScoringLevelConfig.newBuilder()
                                    .setMediumLevelMinScore(1)
                                    .setHighLevelMinScore(51)
                                    .setMaxNormalizedScore(2)
                                    .build())
                            .build()))
            .build(),
        DefaultThreatScoringConfig.loadDefaultThreatScoringConfigs());
  }
}
