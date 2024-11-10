package ai.traceable.edge.bot.config.service.store;

import ai.traceable.edge.bot.config.service.v1.CaptchaSiteKeyConfig;
import com.google.protobuf.Value;
import java.util.Optional;
import javax.inject.Inject;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;

public class CaptchaSiteKeyConfigStore extends IdentifiedObjectStore<CaptchaSiteKeyConfig> {
  public static final String EDGE_BOT_CONFIG_NAMESPACE = "edge-bot-config-resource-namespace";

  public static final String EDGE_BOT_CONFIG_RESOURCE = "edge-bot-config-resource";

  @Inject
  public CaptchaSiteKeyConfigStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        EDGE_BOT_CONFIG_NAMESPACE,
        EDGE_BOT_CONFIG_RESOURCE,
        configChangeEventGenerator);
  }

  @SneakyThrows
  @Override
  protected Optional<CaptchaSiteKeyConfig> buildDataFromValue(Value value) {
    CaptchaSiteKeyConfig.Builder builder = CaptchaSiteKeyConfig.newBuilder();
    ConfigProtoConverter.mergeFromValue(value, builder);
    return Optional.of(builder.build());
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(CaptchaSiteKeyConfig captchaSiteKeyConfig) {
    return ConfigProtoConverter.convertToValue(captchaSiteKeyConfig);
  }

  @Override
  protected String getContextFromData(CaptchaSiteKeyConfig captchaSiteKeyConfig) {
    return captchaSiteKeyConfig.getId();
  }
}
