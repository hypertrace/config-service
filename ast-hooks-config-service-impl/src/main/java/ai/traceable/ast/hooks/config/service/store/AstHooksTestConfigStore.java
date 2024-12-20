package ai.traceable.ast.hooks.config.service.store;

import static ai.traceable.ast.hooks.config.service.store.AstHookConfigConstants.AST_HOOKS_CONFIG_RESOURCE_NAMESPACE;

import ai.traceable.ast.hooks.config.service.v1.AstHookTest;
import ai.traceable.ast.hooks.config.service.v1.AstHookTestFilter;
import com.google.protobuf.Value;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class AstHooksTestConfigStore extends IdentifiedObjectStore<AstHookTest> {

  private static final String AST_HOOKS_TESTS_CONFIG_RESOURCE_NAME = "ast-hooks-test-config";

  @Inject
  public AstHooksTestConfigStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        AST_HOOKS_CONFIG_RESOURCE_NAMESPACE,
        AST_HOOKS_TESTS_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  public List<AstHookTest> getAllFilteredAstHookTests(
      final RequestContext requestContext, final List<AstHookTestFilter> filters) {
    return this.getAllConfigData(requestContext).stream()
        .filter(astHookTest -> applyFilters(astHookTest, filters))
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  @SneakyThrows
  protected Optional<AstHookTest> buildDataFromValue(Value value) {
    AstHookTest.Builder astHookTestBuilder = AstHookTest.newBuilder();
    ConfigProtoConverter.mergeFromValue(value, astHookTestBuilder);
    return Optional.of(astHookTestBuilder.build());
  }

  @Override
  @SneakyThrows
  protected Value buildValueFromData(AstHookTest astHookTest) {
    return ConfigProtoConverter.convertToValue(astHookTest);
  }

  @Override
  protected String getContextFromData(AstHookTest astHookTest) {
    return astHookTest.getId();
  }

  private boolean applyFilters(
      final AstHookTest astHookTest, final List<AstHookTestFilter> filters) {
    return filters.stream()
        .allMatch(astHookTestFilter -> filterBasedOnAstHookFilter(astHookTest, astHookTestFilter));
  }

  private boolean filterBasedOnAstHookFilter(
      final AstHookTest astHookTest, final AstHookTestFilter filter) {
    switch (filter.getFilterCase()) {
      case TEST_STATUS_FILTER:
        return filter.getTestStatusFilter().getStatusesList().contains(astHookTest.getTestStatus());
      case FILTER_NOT_SET:
        return true;
      default:
        log.error("Encountered unknown ast hook filter case: {}", filter);
        return false;
    }
  }
}
