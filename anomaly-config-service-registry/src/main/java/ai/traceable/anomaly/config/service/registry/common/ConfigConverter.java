package ai.traceable.anomaly.config.service.registry.common;

import ai.traceable.anomaly.config.service.v1.AnomalyEventFamily;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleCategory;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.EnumExtension;
import ai.traceable.anomaly.config.service.v1.aggregator.AggregationConfig;
import ai.traceable.anomaly.config.service.v1.aggregator.EventAggregationGlobalConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigRenderOptions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConfigConverter {
  private static final JsonFormat.Parser JSON_PARSER = JsonFormat.parser().ignoringUnknownFields();
  private static final ObjectMapper YAML_OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());
  private static final ObjectMapper JSON_OBJECT_MAPPER = new ObjectMapper();
  private static final ConfigRenderOptions CONFIG_RENDER_CONCISE = ConfigRenderOptions.concise();

  public Map<String, AnomalyRuleInfo> getAnomalyRuleInfos(
      String fileName, AnomalyEventFamily eventFamily) {
    try {
      JsonNode jsonNode =
          YAML_OBJECT_MAPPER.readValue(
              getClass().getClassLoader().getResourceAsStream(fileName), JsonNode.class);
      Iterator<JsonNode> iterator = jsonNode.elements();
      List<AnomalyRuleInfo> rules = new ArrayList<>();
      while (iterator.hasNext()) {
        String ruleJson = JSON_OBJECT_MAPPER.writeValueAsString(iterator.next());
        AnomalyRuleInfo.Builder builder = AnomalyRuleInfo.newBuilder().setEventFamily(eventFamily);
        JSON_PARSER.merge(ruleJson, builder);
        rules.add(builder.build());
      }
      return rules.stream()
          .collect(Collectors.toUnmodifiableMap(AnomalyRuleInfo::getRuleId, Function.identity()));
    } catch (Exception e) {
      log.error("Error in reading anomaly rule infos from file: {}", fileName, e);
      return Collections.emptyMap();
    }
  }

  public Map<String, AnomalyRuleInfo> convertAnomalyRuleInfos(
      List<? extends Config> configsList, AnomalyEventFamily eventFamily) {
    return configsList.stream()
        .map(
            config -> {
              AnomalyRuleInfo.Builder builder = AnomalyRuleInfo.newBuilder();
              mergeFromConfig(config, builder);
              if (builder.getAnomalyRuleCategory()
                  == AnomalyRuleCategory.ANOMALY_RULE_CATEGORY_UNSPECIFIED) {
                throw new IllegalArgumentException(
                    String.format(
                        "Anomaly rule with id - %s does not have a valid anomaly rule category",
                        builder.getRuleId()));
              }
              return builder
                  .setEventFamily(eventFamily)
                  .setRuleCategory(
                      builder
                          .getAnomalyRuleCategory()
                          .getValueDescriptor()
                          .getOptions()
                          .getExtension(EnumExtension.stringValue))
                  .build();
            })
        .collect(Collectors.toUnmodifiableMap(AnomalyRuleInfo::getRuleId, config -> config));
  }

  public List<AnomalyDetectionConfig> convertToAnomalyDetectionConfigs(
      List<? extends Config> configsList) {
    return configsList.stream()
        .map(
            config -> {
              AnomalyDetectionConfig.Builder builder = AnomalyDetectionConfig.newBuilder();
              mergeFromConfig(config, builder);
              return builder.build();
            })
        .collect(Collectors.toList());
  }

  public Optional<AggregationConfig> getAggregationConfig(String configPath, Config config) {
    if (config.hasPath(configPath)) {
      AggregationConfig.Builder builder = AggregationConfig.newBuilder();
      mergeFromConfig(config.getConfig(configPath), builder);
      return Optional.of(builder.build());
    }
    return Optional.empty();
  }

  public Optional<EventAggregationGlobalConfig> getGlobalAggregationConfig(
      String configPath, Config config) {
    if (config.hasPath(configPath)) {
      EventAggregationGlobalConfig.Builder builder = EventAggregationGlobalConfig.newBuilder();
      mergeFromConfig(config.getConfig(configPath), builder);
      return Optional.of(builder.build());
    }
    return Optional.empty();
  }

  public List<TrainingConfig> convertToTrainingConfigs(List<? extends Config> configList) {
    return configList.stream()
        .map(
            config -> {
              TrainingConfig.Builder builder = TrainingConfig.newBuilder();
              mergeFromConfig(config, builder);
              return builder.build();
            })
        .collect(Collectors.toList());
  }

  @SneakyThrows
  private static void mergeFromConfig(Config config, Message.Builder builder) {
    JSON_PARSER.merge(config.root().render(CONFIG_RENDER_CONCISE), builder);
  }
}
