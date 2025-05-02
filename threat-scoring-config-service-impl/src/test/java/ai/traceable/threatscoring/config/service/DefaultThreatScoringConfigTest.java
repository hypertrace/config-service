package ai.traceable.threatscoring.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.threatscoring.config.service.v1.ActorMaliciousConfidenceConfig;
import ai.traceable.threatscoring.config.service.v1.AnomalousEventConfidenceConfig;
import ai.traceable.threatscoring.config.service.v1.ApiTrendConfidenceConfig;
import ai.traceable.threatscoring.config.service.v1.EventConfidenceScoringConfig;
import ai.traceable.threatscoring.config.service.v1.IpReputationFactorMapping;
import ai.traceable.threatscoring.config.service.v1.MaliciousSpanConfidenceScoring;
import ai.traceable.threatscoring.config.service.v1.ResponseConfidenceConfig;
import ai.traceable.threatscoring.config.service.v1.ScoringLevelConfig;
import ai.traceable.threatscoring.config.service.v1.ThreatScoringConfigs;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
    assertEquals(
        mappingList.size(),
        uniqueSubConfidences.size(),
        "Duplicate entries found for the confidence fields (BadActor, Malicious, Anomalous, Trend, Response)");
  }

  @Test
  void test_threat_activity_confidence_mapping_complete_and_unique() {
    Map<String, String> labelToEnum =
        Map.of(
            "HIGH", "SCORING_LEVEL_HIGH",
            "MEDIUM", "SCORING_LEVEL_MEDIUM",
            "LOW", "SCORING_LEVEL_LOW",
            "NA", "SCORING_LEVEL_UNSPECIFIED");

    List<String> possibleScore = List.of("HIGH", "MEDIUM", "LOW");
    List<String> possibleScoreWithNA = List.of("HIGH", "MEDIUM", "LOW", "NA");

    Set<String> expectedPermutations = new HashSet<>();
    for (String currentEvent : possibleScore) {
      for (String previousActivity : possibleScoreWithNA) {
        for (String eventCount : possibleScore) {
          for (String uniqueParams : possibleScore) {
            expectedPermutations.add(
                String.join(
                    ",",
                    labelToEnum.get(currentEvent),
                    labelToEnum.get(previousActivity),
                    labelToEnum.get(eventCount),
                    labelToEnum.get(uniqueParams)));
          }
        }
      }
    }

    assertEquals(
        108,
        expectedPermutations.size(),
        "Expected 108 unique permutations of the confidence fields (CurrentEvent, PreviousActivity, EventCount, UniqueParams)");

    var mappingList =
        defaultThreatScoringConfig
            .getDefaultConfig()
            .getConfigs()
            .getThreatActivityConfidenceScoringConfig()
            .getThreatActivityConfidenceMatrixConfig()
            .getThreatActivityConfidenceMappingList();

    List<String> actualPermutations =
        mappingList.stream()
            .map(
                mapping ->
                    String.join(
                        ",",
                        mapping.getCurrentEventConfidence().toString(),
                        mapping.getPreviousActivityConfidence().toString(),
                        mapping.getPreviousActivityEventCount().toString(),
                        mapping.getPreviousUniqueParamsCount().toString()))
            .collect(Collectors.toList());

    Set<String> uniquePermutations = new HashSet<>(actualPermutations);
    assertEquals(
        actualPermutations.size(),
        uniquePermutations.size(),
        "Duplicate entries found for the confidence fields (CurrentEvent, PreviousActivity, EventCount, UniqueParams)");

    assertEquals(
        expectedPermutations.size(),
        uniquePermutations.size(),
        "Expected permutations do not match the actual permutations");

    if (!expectedPermutations.equals(uniquePermutations)) {
      Set<String> missing = new HashSet<>(expectedPermutations);
      missing.removeAll(uniquePermutations);
      System.err.println("Missing permutations:");
      missing.forEach(System.err::println);
      Assertions.fail("Some expected permutations are missing");
    }
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
