package ai.traceable.ast.hooks.config.service.store;

import static ai.traceable.ast.hooks.config.service.store.AstHookConfigConstants.AST_HOOKS_CONFIG_RESOURCE_NAMESPACE;

import ai.traceable.ast.hooks.config.service.v1.AstHook;
import com.google.protobuf.Value;
import java.util.Optional;
import javax.inject.Inject;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;

public class AstHooksConfigStore extends IdentifiedObjectStore<AstHook> {
  private static final String AST_HOOKS_CONFIG_RESOURCE_NAME = "ast-hooks-config";

  @Inject
  public AstHooksConfigStore(ConfigServiceBlockingStub configServiceBlockingStub) {
    super(
        configServiceBlockingStub,
        AST_HOOKS_CONFIG_RESOURCE_NAMESPACE,
        AST_HOOKS_CONFIG_RESOURCE_NAME);
  }

  @SneakyThrows
  @Override
  protected Optional<AstHook> buildDataFromValue(Value value) {
    AstHook.Builder configBuilder = AstHook.newBuilder();
    ConfigProtoConverter.mergeFromValue(value, configBuilder);
    return Optional.of(configBuilder.build());
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(AstHook astHook) {
    return ConfigProtoConverter.convertToValue(astHook);
  }

  @Override
  protected String getContextFromData(AstHook astHook) {
    return astHook.getId();
  }
}
