package ai.traceable.ast.hooks.config.service.store;

import static ai.traceable.ast.hooks.config.service.store.AstHookConfigConstants.AST_HOOKS_CONFIG_RESOURCE_NAMESPACE;

import ai.traceable.ast.hooks.config.service.v1.AstHook;
import ai.traceable.ast.hooks.config.service.v1.Filter;
import ai.traceable.ast.hooks.config.service.v1.GetAstHookFilter;
import ai.traceable.ast.hooks.config.service.v1.HookScope;
import com.google.protobuf.Value;
import io.grpc.Status;
import jakarta.inject.Inject;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
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

  public List<AstHook> getAllConfigDataWithScopeFilter(
      RequestContext requestContext, HookScope requestScope, Filter filter) {
    List<AstHook> allHooks = getAllConfigData(requestContext);

    Set<String> allowedEnvironmentIds =
        Set.copyOf(requestScope.getEnvironmentScope().getEnvironmentIdsList());

    // Filter hooks to only include those the user has environment access to
    return allHooks.stream()
        .filter(hook -> isHookAccessibleToUser(hook, allowedEnvironmentIds))
        .filter(hook -> isHookRequestedInFilter(hook, filter))
        .collect(Collectors.toUnmodifiableList());
  }

  private boolean isHookAccessibleToUser(AstHook hook, Set<String> userAllowedEnvironments) {

    if (userAllowedEnvironments.isEmpty()) {
      return true;
    }

    // Deny access to hooks without proper environment scope configuration
    if (!hook.getHookDetails().hasScope()
        || !hook.getHookDetails().getScope().hasEnvironmentScope()) {
      return false;
    }

    Set<String> hookEnvironmentIds =
        Set.copyOf(hook.getHookDetails().getScope().getEnvironmentScope().getEnvironmentIdsList());

    // Deny access to hooks with empty environment scope (invalid configuration )
    if (hookEnvironmentIds.isEmpty()) {
      return false;
    }

    // Grant access only if user has permission to all environment required by the hook
    return userAllowedEnvironments.containsAll(hookEnvironmentIds);
  }

  private boolean isHookRequestedInFilter(AstHook hook, Filter filter) {
    // If no filter is provided, include all hooks
    if (filter == null || filter.getEnvironmentIdsList().isEmpty()) {
      return true;
    }

    // Check if hook has proper environment scope configuration -> All Env Scoped
    if (!hook.getHookDetails().hasScope()
        || !hook.getHookDetails().getScope().hasEnvironmentScope()) {
      return true;
    }

    // Include hook if any of its environments match the filter environments
    return !Collections.disjoint(
        hook.getHookDetails().getScope().getEnvironmentScope().getEnvironmentIdsList(),
        filter.getEnvironmentIdsList());
  }
}
