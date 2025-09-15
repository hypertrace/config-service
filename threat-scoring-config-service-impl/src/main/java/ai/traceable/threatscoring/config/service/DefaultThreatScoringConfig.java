package ai.traceable.threatscoring.config.service;

import ai.traceable.threatscoring.config.service.v1.EventConfidenceMapping;
import ai.traceable.threatscoring.config.service.v1.EventConfidenceMatrixConfig;
import ai.traceable.threatscoring.config.service.v1.ScopedThreatScoringConfigs;
import ai.traceable.threatscoring.config.service.v1.ScoringLevel;
import ai.traceable.threatscoring.config.service.v1.ThreatActivityConfidenceMapping;
import ai.traceable.threatscoring.config.service.v1.ThreatActivityConfidenceMatrixConfig;
import ai.traceable.threatscoring.config.service.v1.ThreatActivityConfidenceScoringConfig;
import ai.traceable.threatscoring.config.service.v1.ThreatScoringConfigs;
import com.google.inject.Inject;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DefaultThreatScoringConfig {
  private static final String EVENT_CONFIDENCE_MAPPING_FILE_NAME = "event_confidence_mapping.csv";
  private static final String THREAT_ACTIVITY_CONFIDENCE_MAPPING_FILE_NAME =
      "threat_activity_confidence_mapping.csv";
  private static final String DEFAULT_THREAT_SCORING_CONFIG_FILE_NAME =
      "default-threat-scoring-config.conf";
  private static final JsonFormat.Parser JSON_PARSER = JsonFormat.parser().ignoringUnknownFields();
  private static final ConfigRenderOptions CONFIG_RENDER_CONCISE = ConfigRenderOptions.concise();

  private static final Map<String, ScoringLevel> SCORING_LEVEL_MAP =
      Map.of(
          "LOW", ScoringLevel.SCORING_LEVEL_LOW,
          "MEDIUM", ScoringLevel.SCORING_LEVEL_MEDIUM,
          "HIGH", ScoringLevel.SCORING_LEVEL_HIGH);
  private static final String DELIMITER = ",";
  private final ScopedThreatScoringConfigs defaultConfig;

  @Inject
  public DefaultThreatScoringConfig() {
    ThreatScoringConfigs defaultThreatScoringConfigs = loadDefaultThreatScoringConfigs();
    defaultConfig = mergeEventAndThreatActivityConfidenceMappings(defaultThreatScoringConfigs);
  }

  public ScopedThreatScoringConfigs getDefaultConfig() {
    return this.defaultConfig;
  }

  static ThreatScoringConfigs loadDefaultThreatScoringConfigs() {
    ThreatScoringConfigs.Builder builder = ThreatScoringConfigs.newBuilder();
    mergeFromConfig(ConfigFactory.parseResources(DEFAULT_THREAT_SCORING_CONFIG_FILE_NAME), builder);
    return builder.build();
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

  private static List<ThreatActivityConfidenceMapping> loadThreatActivityConfidenceMapping() {
    URL resource =
        DefaultThreatScoringConfig.class
            .getClassLoader()
            .getResource(THREAT_ACTIVITY_CONFIDENCE_MAPPING_FILE_NAME);

    if (resource == null) {
      throw new RuntimeException(THREAT_ACTIVITY_CONFIDENCE_MAPPING_FILE_NAME + " file not found!");
    }

    String line;
    List<ThreatActivityConfidenceMapping> threatActivityConfidenceMappingList = new ArrayList<>();
    try (InputStream inputStream = resource.openStream();
        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {
      br.readLine(); // reads the column name
      while ((line = br.readLine()) != null) {
        String[] values = line.split(DELIMITER);
        if (values.length < 5) {
          continue;
        }
        threatActivityConfidenceMappingList.add(
            ThreatActivityConfidenceMapping.newBuilder()
                .setCurrentEventConfidence(
                    SCORING_LEVEL_MAP.getOrDefault(
                        values[0], ScoringLevel.SCORING_LEVEL_UNSPECIFIED))
                .setPreviousActivityConfidence(
                    SCORING_LEVEL_MAP.getOrDefault(
                        values[1], ScoringLevel.SCORING_LEVEL_UNSPECIFIED))
                .setPreviousActivityEventCount(
                    SCORING_LEVEL_MAP.getOrDefault(
                        values[2], ScoringLevel.SCORING_LEVEL_UNSPECIFIED))
                .setPreviousUniqueParamsCount(
                    SCORING_LEVEL_MAP.getOrDefault(
                        values[3], ScoringLevel.SCORING_LEVEL_UNSPECIFIED))
                .setNewActivityConfidence(
                    SCORING_LEVEL_MAP.getOrDefault(
                        values[4], ScoringLevel.SCORING_LEVEL_UNSPECIFIED))
                .setResponseVariationCount(ScoringLevel.SCORING_LEVEL_UNSPECIFIED)
                .build());
      }

    } catch (IOException e) {
      log.error("Unable to parse default event confidence mapping", e.getMessage());
    }
    return threatActivityConfidenceMappingList;
  }

  private static ScopedThreatScoringConfigs mergeEventAndThreatActivityConfidenceMappings(
      ThreatScoringConfigs defaultThreatScoringConfigs) {
    return ScopedThreatScoringConfigs.newBuilder()
        .setConfigs(
            ThreatScoringConfigs.newBuilder()
                .setThreatActivityConfidenceScoringConfig(
                    ThreatActivityConfidenceScoringConfig.newBuilder()
                        .setThreatActivityConfidenceMatrixConfig(
                            ThreatActivityConfidenceMatrixConfig.newBuilder()
                                .addAllThreatActivityConfidenceMapping(
                                    loadThreatActivityConfidenceMapping()))
                        .build())
                .setEventConfidenceScoringConfig(
                    defaultThreatScoringConfigs.getEventConfidenceScoringConfig().toBuilder()
                        .setEventConfidenceMatrixConfig(
                            EventConfidenceMatrixConfig.newBuilder()
                                .addAllEventConfidenceMapping(loadEventConfidenceMapping()))))
        .build();
  }

  @SneakyThrows
  private static void mergeFromConfig(Config config, Message.Builder builder) {
    JSON_PARSER.merge(config.root().render(CONFIG_RENDER_CONCISE), builder);
  }
}
