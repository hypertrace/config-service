package ai.traceable.ast.hooks.config.service.store;

import static ai.traceable.ast.hooks.config.service.store.AstHookConfigConstants.AST_HOOKS_CONFIG_RESOURCE_NAMESPACE;

import ai.traceable.ast.hooks.config.service.v1.AstHook;
import ai.traceable.ast.hooks.config.service.v1.GetAstHookFilter;
import com.google.protobuf.Value;
import io.grpc.Status;
import java.util.Optional;
import javax.inject.Inject;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class AstHooksConfigStore
    extends IdentifiedObjectStoreWithFilter<AstHook, GetAstHookFilter> {

  private static final String AST_HOOKS_CONFIG_RESOURCE_NAME = "ast-hooks-config";

  @Inject
  public AstHooksConfigStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        AST_HOOKS_CONFIG_RESOURCE_NAMESPACE,
        AST_HOOKS_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
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

  @Override
  protected Optional<AstHook> filterConfigData(AstHook astHook, GetAstHookFilter filter) {
    return Optional.of(astHook)
        .filter(
            hook -> filter.getIdList().isEmpty() || filter.getIdList().contains(astHook.getId()))
        .filter(
            hook ->
                filter.getNameList().isEmpty()
                    || filter.getNameList().contains(astHook.getHookDetails().getName()));
  }

  public AstHook getAstHook(RequestContext requestContext, String id) {
    return getData(requestContext, id)
        .orElseThrow(() -> Status.NOT_FOUND.asRuntimeException(requestContext.buildTrailers()));
  }
}
