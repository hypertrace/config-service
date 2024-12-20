package ai.traceable.edge.bot.config.service.store;

import ai.traceable.edge.bot.config.service.v1.CaptchaSiteKeyConfig;
import com.google.protobuf.Value;
import jakarta.inject.Inject;
import java.util.Optional;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;

public class CaptchaSiteKeyConfigStore extends IdentifiedObjectStore<CaptchaSiteKeyConfig> {
  public static final String BOT_CAPTCHA_SITE_KEY_CONFIG_RESOURCE_NAMESPACE =
      "bot-captcha-site-key-config-resource-namespace";

  public static final String BOT_CAPTCHA_SITE_KEY_CONFIG_RESOURCE =
      "bot-captcha-site-key-config-resource";

  @Inject
  public CaptchaSiteKeyConfigStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        BOT_CAPTCHA_SITE_KEY_CONFIG_RESOURCE_NAMESPACE,
        BOT_CAPTCHA_SITE_KEY_CONFIG_RESOURCE,
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
