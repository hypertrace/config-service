package ai.traceable.ast.config.service.rules;

import static ai.traceable.ast.config.service.constants.AstConfigConstants.AST_CONFIG_NAMESPACE;

import ai.traceable.ast.config.service.v1.CustomTestPlugin;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;

@Slf4j
public class CustomTestPluginStore extends IdentifiedObjectStore<CustomTestPlugin> {
  private static final String CUSTOM_TEST_PLUGIN_RESOURCE_NAME = "custom-test-plugin";

  @Inject
  public CustomTestPluginStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        AST_CONFIG_NAMESPACE,
        CUSTOM_TEST_PLUGIN_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @SneakyThrows
  @Override
  protected Optional<CustomTestPlugin> buildDataFromValue(Value value) {
    CustomTestPlugin.Builder configBuilder = CustomTestPlugin.newBuilder();
    ConfigProtoConverter.mergeFromValue(value, configBuilder);
    return Optional.of(configBuilder.build());
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(CustomTestPlugin customTestPlugin) {
    return ConfigProtoConverter.convertToValue(customTestPlugin);
  }

  @SneakyThrows
  @Override
  protected String getContextFromData(CustomTestPlugin customTestPlugin) {
    return customTestPlugin.getId();
  }
}
