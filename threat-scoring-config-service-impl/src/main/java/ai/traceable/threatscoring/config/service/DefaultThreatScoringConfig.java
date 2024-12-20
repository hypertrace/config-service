package ai.traceable.threatscoring.config.service;

import ai.traceable.threatscoring.config.service.v1.EventConfidenceMapping;
import ai.traceable.threatscoring.config.service.v1.EventConfidenceMatrixConfig;
import ai.traceable.threatscoring.config.service.v1.EventConfidenceScoringConfig;
import ai.traceable.threatscoring.config.service.v1.ScopedThreatScoringConfigs;
import ai.traceable.threatscoring.config.service.v1.ScoringLevel;
import ai.traceable.threatscoring.config.service.v1.ThreatScoringConfigs;
import com.google.inject.Inject;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DefaultThreatScoringConfig {
  private static final String EVENT_CONFIDENCE_MAPPING_FILE_NAME = "event_confidence_mapping.csv";
  private static final Map<String, ScoringLevel> SCORING_LEVEL_MAP =
      Map.of(
          "LOW", ScoringLevel.SCORING_LEVEL_LOW,
          "MEDIUM", ScoringLevel.SCORING_LEVEL_MEDIUM,
          "HIGH", ScoringLevel.SCORING_LEVEL_HIGH);
  private static final String DELIMITER = ",";
  private final ScopedThreatScoringConfigs defaultConfig;

  @Inject
  public DefaultThreatScoringConfig() {
    defaultConfig =
        ScopedThreatScoringConfigs.newBuilder()
            .setConfigs(
                ThreatScoringConfigs.newBuilder()
                    .setEventConfidenceScoringConfig(
                        EventConfidenceScoringConfig.newBuilder()
                            .setEventConfidenceMatrixConfig(
                                EventConfidenceMatrixConfig.newBuilder()
                                    .addAllEventConfidenceMapping(loadEventConfidenceMapping())))
                    .build())
            .build();
  }

  public ScopedThreatScoringConfigs getDefaultConfig() {
    return this.defaultConfig;
  }

  private static List<EventConfidenceMapping> loadEventConfidenceMapping() {
    URL resource =
        DefaultThreatScoringConfig.class
            .getClassLoader()
            .getResource(EVENT_CONFIDENCE_MAPPING_FILE_NAME);

    if (resource == null) {
      throw new RuntimeException(EVENT_CONFIDENCE_MAPPING_FILE_NAME + " file not found!");
    }

    String line;
    List<EventConfidenceMapping> eventConfidenceMappingList = new ArrayList<>();
    try (InputStream inputStream = resource.openStream();
        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {
      br.readLine(); // reads the column name
      while ((line = br.readLine()) != null) {
        String[] values = line.split(DELIMITER);
        if (values.length < 6) {
          continue;
        }
        eventConfidenceMappingList.add(
            EventConfidenceMapping.newBuilder()
                .setActorConfidence(
                    SCORING_LEVEL_MAP.getOrDefault(
                        values[0], ScoringLevel.SCORING_LEVEL_UNSPECIFIED))
                .setMaliciousSpanConfidence(
                    SCORING_LEVEL_MAP.getOrDefault(
                        values[1], ScoringLevel.SCORING_LEVEL_UNSPECIFIED))
                .setAnomalousConfidence(
                    SCORING_LEVEL_MAP.getOrDefault(
                        values[2], ScoringLevel.SCORING_LEVEL_UNSPECIFIED))
                .setTrendConfidence(
                    SCORING_LEVEL_MAP.getOrDefault(
                        values[3], ScoringLevel.SCORING_LEVEL_UNSPECIFIED))
                .setResponseConfidence(
                    SCORING_LEVEL_MAP.getOrDefault(
                        values[4], ScoringLevel.SCORING_LEVEL_UNSPECIFIED))
                .setEventConfidence(
                    SCORING_LEVEL_MAP.getOrDefault(
                        values[5], ScoringLevel.SCORING_LEVEL_UNSPECIFIED))
                .build());
      }

    } catch (IOException e) {
      log.error("Unable to parse default event confidence mapping", e.getMessage());
    }
    return eventConfidenceMappingList;
  }
}
