package ai.traceable.ast.hooks.config.service.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import ai.traceable.ast.hooks.config.service.v1.AstHook;
import ai.traceable.ast.hooks.config.service.v1.AstHookDetails;
import ai.traceable.ast.hooks.config.service.v1.EnvironmentScope;
import ai.traceable.ast.hooks.config.service.v1.Filter;
import ai.traceable.ast.hooks.config.service.v1.HookScope;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class AstHooksConfigStoreTest {

  MockGenericConfigService mockGenericConfigService;
  @Mock ConfigChangeEventGenerator mockConfigChangeEventGenerator;
  @Mock RequestContext mockRequestContext;
  @Mock HookScope mockRequestScope;
  AstHooksConfigStore spyAstHooksConfigStore;

  @BeforeEach
  void setUp() {
    mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDeleteAll();
    ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());

    spyAstHooksConfigStore =
        spy(new AstHooksConfigStore(configServiceBlockingStub, mockConfigChangeEventGenerator));

    mockGenericConfigService.start();
  }

  @Test
  void testGetAllConfigDataWithScopeFilter_WithUserScopeAndFilter() {
    AstHook hookEnv1 = createTestHook("hook1", "env1");
    AstHook hookEnv2 = createTestHook("hook2", "env2");
    AstHook hookEnv1And2 = createTestHook("hook3", "env1", "env2");
    AstHook hookEnv3 = createTestHook("hook4", "env3");
    Filter filter = Filter.newBuilder().addEnvironmentIds("env2").build();

    doReturn(List.of(hookEnv1, hookEnv2, hookEnv1And2, hookEnv3))
        .when(spyAstHooksConfigStore)
        .getAllConfigData(mockRequestContext);

    EnvironmentScope envScope =
        EnvironmentScope.newBuilder().addAllEnvironmentIds(List.of("env1", "env2")).build();

    when(mockRequestScope.hasEnvironmentScope()).thenReturn(true);
    when(mockRequestScope.getEnvironmentScope()).thenReturn(envScope);

    List<AstHook> result =
        spyAstHooksConfigStore.getAllConfigDataWithScopeFilter(
            mockRequestContext, mockRequestScope, filter);

    // Assert: Should return hook2, hook3 (user has access to env1,env2 but filter only matches
    // env2)
    assertEquals(2, result.size());
    List<String> resultHookIds =
        result.stream().map(AstHook::getId).sorted().collect(Collectors.toList());
    assertEquals(List.of("hook2", "hook3"), resultHookIds);
  }

  @Test
  void testGetAllConfigDataWithScopeFilter_AdminAccess() {
    AstHook hookEnv1 = createTestHook("hook1", "env1");
    AstHook hookEnv2 = createTestHook("hook2", "env2");
    AstHook hookEnv3 = createTestHook("hook3", "env3");
    Filter filter = Filter.newBuilder().addEnvironmentIds("env1").build();

    doReturn(List.of(hookEnv1, hookEnv2, hookEnv3))
        .when(spyAstHooksConfigStore)
        .getAllConfigData(mockRequestContext);

    // Admin user with empty environment scope (access to all environments)
    EnvironmentScope envScope = EnvironmentScope.newBuilder().build();

    when(mockRequestScope.hasEnvironmentScope()).thenReturn(true);
    when(mockRequestScope.getEnvironmentScope()).thenReturn(envScope);

    List<AstHook> result =
        spyAstHooksConfigStore.getAllConfigDataWithScopeFilter(
            mockRequestContext, mockRequestScope, filter);

    // Assert: Admin should only get hooks matching the filter (hook1)
    assertEquals(1, result.size());
    assertEquals("hook1", result.get(0).getId());
  }

  @Test
  void testGetAllConfigDataWithScopeFilter_UserMissingEnvironmentAccess() {
    AstHook hookMultiEnv = createTestHook("hook1", "env1", "env3");
    AstHook hookSingleEnv = createTestHook("hook2", "env2");
    Filter filter = Filter.newBuilder().build(); // Empty filter

    doReturn(List.of(hookMultiEnv, hookSingleEnv))
        .when(spyAstHooksConfigStore)
        .getAllConfigData(mockRequestContext);

    // User only has access to env1, env2 but not env3
    EnvironmentScope envScope =
        EnvironmentScope.newBuilder().addAllEnvironmentIds(List.of("env1", "env2")).build();

    when(mockRequestScope.hasEnvironmentScope()).thenReturn(true);
    when(mockRequestScope.getEnvironmentScope()).thenReturn(envScope);

    List<AstHook> result =
        spyAstHooksConfigStore.getAllConfigDataWithScopeFilter(
            mockRequestContext, mockRequestScope, filter);

    // Assert: Should return both hooks (user has access to env1 from hook1 and env2 from hook2)
    assertEquals(2, result.size());
    Set<String> hookIds = result.stream().map(AstHook::getId).collect(Collectors.toSet());
    assertTrue(hookIds.contains("hook1"));
    assertTrue(hookIds.contains("hook2"));
  }

  @Test
  void testGetAllConfigDataWithScopeFilter_HooksWithInvalidScope() {
    AstHook validHook = createTestHook("valid-hook", "env1");
    AstHook hookWithoutScope =
        AstHook.newBuilder()
            .setId("no-scope-hook")
            .setHookDetails(AstHookDetails.newBuilder().build())
            .build();
    Filter filter = Filter.newBuilder().build(); // Empty filter

    doReturn(List.of(validHook, hookWithoutScope))
        .when(spyAstHooksConfigStore)
        .getAllConfigData(mockRequestContext);

    EnvironmentScope envScope =
        EnvironmentScope.newBuilder().addAllEnvironmentIds(List.of("env1")).build();

    when(mockRequestScope.hasEnvironmentScope()).thenReturn(true);
    when(mockRequestScope.getEnvironmentScope()).thenReturn(envScope);

    List<AstHook> result =
        spyAstHooksConfigStore.getAllConfigDataWithScopeFilter(
            mockRequestContext, mockRequestScope, filter);

    // Assert: Should return both hooks (empty scope hooks are allowed)
    assertEquals(2, result.size());
    Set<String> hookIds = result.stream().map(AstHook::getId).collect(Collectors.toSet());
    assertTrue(hookIds.contains("valid-hook"));
    assertTrue(hookIds.contains("no-scope-hook"));
  }

  // Helper method for creating test hooks
  private AstHook createTestHook(String id, String... envIds) {
    return AstHook.newBuilder()
        .setId(id)
        .setHookDetails(
            AstHookDetails.newBuilder()
                .setScope(
                    HookScope.newBuilder()
                        .setEnvironmentScope(
                            EnvironmentScope.newBuilder()
                                .addAllEnvironmentIds(List.of(envIds))
                                .build())
                        .build())
                .build())
        .build();
  }

  @AfterEach
  void afterEach() {
    mockGenericConfigService.shutdown();
  }
}
