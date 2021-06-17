package ai.traceable.anomaly.config.service.registry.common;

import ai.traceable.anomaly.config.service.v1.AnomalyEventFamily;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
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
              return builder.setEventFamily(eventFamily).build();
            })
        .collect(Collectors.toUnmodifiableMap(AnomalyRuleInfo::getRuleId, config -> config));
  }

  @SneakyThrows
  private static void mergeFromConfig(Config config, Message.Builder builder) {
    JSON_PARSER.merge(config.root().render(CONFIG_RENDER_CONCISE), builder);
  }
}
