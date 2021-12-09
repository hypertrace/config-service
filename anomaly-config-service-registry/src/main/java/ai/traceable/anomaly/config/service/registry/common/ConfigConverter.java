package ai.traceable.anomaly.config.service.registry.common;

import ai.traceable.anomaly.config.service.v1.AnomalyEventFamily;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleCategory;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.EnumExtension;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigRenderOptions;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.SneakyThrows;

public class ConfigConverter {
  private static final JsonFormat.Parser JSON_PARSER = JsonFormat.parser().ignoringUnknownFields();
  private static final ConfigRenderOptions CONFIG_RENDER_CONCISE = ConfigRenderOptions.concise();

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

  @SneakyThrows
  private static void mergeFromConfig(Config config, Message.Builder builder) {
    JSON_PARSER.merge(config.root().render(CONFIG_RENDER_CONCISE), builder);
  }
}
