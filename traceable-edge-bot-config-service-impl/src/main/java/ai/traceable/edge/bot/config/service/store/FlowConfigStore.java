package ai.traceable.edge.bot.config.service.store;

import ai.traceable.edge.bot.config.service.v1.FlowConfig;
import com.google.protobuf.Value;
import java.util.Optional;
import javax.inject.Inject;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;

public class FlowConfigStore extends IdentifiedObjectStore<FlowConfig> {
  public static final String BOT_FLOW_CONFIG_NAMESPACE = "bot-flow-config-resource-namespace";

  public static final String BOT_FLOW_CONFIG_RESOURCE = "bot-flow-config-resource";

  @Inject
  public FlowConfigStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        BOT_FLOW_CONFIG_NAMESPACE,
        BOT_FLOW_CONFIG_RESOURCE,
        configChangeEventGenerator);
  }

  @SneakyThrows
  @Override
  protected Optional<FlowConfig> buildDataFromValue(Value value) {
    FlowConfig.Builder builder = FlowConfig.newBuilder();
    ConfigProtoConverter.mergeFromValue(value, builder);
    return Optional.of(builder.build());
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(FlowConfig config) {
    return ConfigProtoConverter.convertToValue(config);
  }

  @Override
  protected String getContextFromData(FlowConfig config) {
    return config.getId();
  }
}
