package ai.traceable.ipresolutionstrategy.config.service.v1.store;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import ai.traceable.ipresolutionstrategy.config.service.v1.EnvironmentScope;
import ai.traceable.ipresolutionstrategy.config.service.v1.IpResolutionStrategy;
import ai.traceable.ipresolutionstrategy.config.service.v1.IpResolutionStrategyConfig;
import ai.traceable.ipresolutionstrategy.config.service.v1.IpResolutionStrategyConfigData;
import ai.traceable.ipresolutionstrategy.config.service.v1.IpResolutionStrategyConfigServiceConfig;
import ai.traceable.ipresolutionstrategy.config.service.v1.IpResolutionStrategyFilter;
import ai.traceable.ipresolutionstrategy.config.service.v1.Scope;
import ai.traceable.ipresolutionstrategy.config.service.v1.ServiceScope;
import com.google.protobuf.Value;
import java.util.List;
import java.util.Optional;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class IpResolutionStrategyStoreTest {
  private IpResolutionStrategyStore store;

  @BeforeEach
  void setup() {
    IpResolutionStrategyConfigServiceConfig config =
        mock(IpResolutionStrategyConfigServiceConfig.class);
    when(config.getDefaultIpResolutionStrategyConfigs()).thenReturn(List.of());

    store =
        new IpResolutionStrategyStore(
            mock(ConfigServiceGrpc.ConfigServiceBlockingStub.class),
            mock(ConfigChangeEventGenerator.class),
            config);
  }

  @Test
  @DisplayName("buildValueFromData and buildDataFromValue round-trip")
  void roundTripConversion() {
    IpResolutionStrategyConfig config = buildConfig("id1", false, List.of("prod"), List.of("svc1"));

    Value value = store.buildValueFromData(config);
    Optional<IpResolutionStrategyConfig> decoded = store.buildDataFromValue(value);

    assertTrue(decoded.isPresent());
    assertEquals(config, decoded.get());
  }

  @Test
  @DisplayName("getContextFromData returns id")
  void getContext() {
    IpResolutionStrategyConfig config = buildConfig("ctx-id", false, List.of(), List.of());
    assertEquals("ctx-id", store.getContextFromData(config));
  }

  @Test
  @DisplayName("filterConfigData filters by disabled flag, ids, env and service scope")
  void filterConfigDataCases() {
    IpResolutionStrategyConfig enabledScopedCfg =
        buildConfig("cfg1", false, List.of("prod"), List.of("svc1"));

    // 1) Disabled flag mismatch
    IpResolutionStrategyFilter filterDisabledFalse =
        IpResolutionStrategyFilter.newBuilder().setDisabled(false).build();
    IpResolutionStrategyConfig disabledCfg =
        buildConfig("cfgD", true, List.of("prod"), List.of("svc1"));
    assertTrue(store.filterConfigData(disabledCfg, filterDisabledFalse).isEmpty());

    // 2) Ids filter mismatch
    IpResolutionStrategyFilter filterDifferentId =
        IpResolutionStrategyFilter.newBuilder().addIds("other").build();
    assertTrue(store.filterConfigData(enabledScopedCfg, filterDifferentId).isEmpty());

    // 3) Env filter: config has empty env -> applies to all, keep
    IpResolutionStrategyConfig envWideCfg = buildConfig("cfg2", false, List.of(), List.of("svc1"));
    IpResolutionStrategyFilter filterEnv =
        IpResolutionStrategyFilter.newBuilder().addEnvironmentNames("prod").build();
    assertTrue(store.filterConfigData(envWideCfg, filterEnv).isPresent());

    // 4) Env filter: disjoint -> filtered out
    assertTrue(
        store
            .filterConfigData(
                enabledScopedCfg,
                filterEnv.toBuilder()
                    .clearEnvironmentNames()
                    .addEnvironmentNames("staging")
                    .build())
            .isEmpty());

    // 5) Service filter: config has empty service list -> applies to all, keep
    IpResolutionStrategyConfig serviceWideCfg =
        buildConfig("cfg3", false, List.of("prod"), List.of());
    IpResolutionStrategyFilter filterService =
        IpResolutionStrategyFilter.newBuilder().addServiceNames("svc1").build();
    assertTrue(store.filterConfigData(serviceWideCfg, filterService).isPresent());

    // 6) Service filter: disjoint -> filtered out
    assertTrue(
        store
            .filterConfigData(
                enabledScopedCfg,
                filterService.toBuilder().clearServiceNames().addServiceNames("other").build())
            .isEmpty());

    // 7) All filters match -> present
    IpResolutionStrategyFilter matchAll =
        IpResolutionStrategyFilter.newBuilder()
            .setDisabled(false)
            .addEnvironmentNames("prod")
            .addServiceNames("svc1")
            .addIds("cfg1")
            .build();
    assertEquals(enabledScopedCfg, store.filterConfigData(enabledScopedCfg, matchAll).get());
  }

  @Test
  void testDefaultRule() {
    IpResolutionStrategyConfigServiceConfig config =
        mock(IpResolutionStrategyConfigServiceConfig.class);
    IpResolutionStrategyConfig defaultStrategy =
        IpResolutionStrategyConfig.newBuilder()
            .setId("default-id")
            .setData(
                IpResolutionStrategyConfigData.newBuilder()
                    .setStrategy(
                        IpResolutionStrategy.newBuilder().setEvaluateAllIpsForBlocking(true)))
            .build();
    IpResolutionStrategyConfig randomStrategy =
        IpResolutionStrategyConfig.newBuilder()
            .setId("random-id")
            .setData(
                IpResolutionStrategyConfigData.newBuilder()
                    .setStrategy(
                        IpResolutionStrategy.newBuilder().setEvaluateAllIpsForBlocking(true)))
            .build();

    when(config.getDefaultIpResolutionStrategyConfigs()).thenReturn(List.of(defaultStrategy));
    store =
        spy(
            new IpResolutionStrategyStore(
                mock(ConfigServiceGrpc.ConfigServiceBlockingStub.class),
                mock(ConfigChangeEventGenerator.class),
                config));

    doReturn(List.of(new MockContextualConfigObject(randomStrategy)))
        .when(store)
        .getAllObjects(any(RequestContext.class), any(IpResolutionStrategyFilter.class));
    RequestContext ctx = RequestContext.forTenantId("test-tenant");
    assertEquals(
        List.of(defaultStrategy, randomStrategy),
        store.getAllConfigData(ctx, IpResolutionStrategyFilter.getDefaultInstance()));
  }

  @Test
  void testOverrideOfDefaultRule() {
    IpResolutionStrategyConfigServiceConfig config =
        mock(IpResolutionStrategyConfigServiceConfig.class);
    IpResolutionStrategyConfig defaultStrategy =
        IpResolutionStrategyConfig.newBuilder()
            .setId("default-id")
            .setData(
                IpResolutionStrategyConfigData.newBuilder()
                    .setStrategy(
                        IpResolutionStrategy.newBuilder().setEvaluateAllIpsForBlocking(true)))
            .build();
    IpResolutionStrategyConfig overriddenStrategy =
        IpResolutionStrategyConfig.newBuilder()
            .setId("default-id")
            .setData(
                IpResolutionStrategyConfigData.newBuilder()
                    .setStrategy(
                        IpResolutionStrategy.newBuilder().setEvaluateAllIpsForBlocking(false)))
            .build();
    IpResolutionStrategyConfig randomStrategy =
        IpResolutionStrategyConfig.newBuilder()
            .setId("random-id")
            .setData(
                IpResolutionStrategyConfigData.newBuilder()
                    .setStrategy(
                        IpResolutionStrategy.newBuilder().setEvaluateAllIpsForBlocking(true)))
            .build();

    when(config.getDefaultIpResolutionStrategyConfigs()).thenReturn(List.of(defaultStrategy));
    store =
        spy(
            new IpResolutionStrategyStore(
                mock(ConfigServiceGrpc.ConfigServiceBlockingStub.class),
                mock(ConfigChangeEventGenerator.class),
                config));

    doReturn(
            List.of(
                new MockContextualConfigObject(randomStrategy),
                new MockContextualConfigObject(overriddenStrategy)))
        .when(store)
        .getAllObjects(any(RequestContext.class), any(IpResolutionStrategyFilter.class));
    RequestContext ctx = RequestContext.forTenantId("test-tenant");
    assertEquals(
        List.of(overriddenStrategy, randomStrategy),
        store.getAllConfigData(ctx, IpResolutionStrategyFilter.getDefaultInstance()));
  }

  private static IpResolutionStrategyConfig buildConfig(
      String id, boolean disabled, List<String> envNames, List<String> serviceNames) {
    IpResolutionStrategy strategy =
        IpResolutionStrategy.newBuilder().setEvaluateAllIpsForBlocking(true).build();
    IpResolutionStrategyConfigData data =
        IpResolutionStrategyConfigData.newBuilder()
            .setStrategy(strategy)
            .setDisabled(disabled)
            .setScope(
                Scope.newBuilder()
                    .setEnvironmentScope(
                        EnvironmentScope.newBuilder().addAllEnvironmentNames(envNames))
                    .setServiceScope(ServiceScope.newBuilder().addAllServiceNames(serviceNames)))
            .build();
    return IpResolutionStrategyConfig.newBuilder().setId(id).setData(data).build();
  }
}
