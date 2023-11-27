package ai.traceable.runner.logs.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.runner.logs.config.service.v1.AstScanLogsPersistenceConfig;
import ai.traceable.runner.logs.config.service.v1.AstScanLogsPersistenceConfigUpdate;
import ai.traceable.runner.logs.config.service.v1.AstScanLogsPurgeConfig;
import ai.traceable.runner.logs.config.service.v1.AstScanLogsPurgeConfigUpdate;
import ai.traceable.runner.logs.config.service.v1.RunnerLogsPersistenceConfig;
import ai.traceable.runner.logs.config.service.v1.RunnerLogsPersistenceConfigUpdate;
import ai.traceable.runner.logs.config.service.v1.RunnerLogsPurgeConfig;
import ai.traceable.runner.logs.config.service.v1.RunnerLogsPurgeConfigUpdate;
import ai.traceable.runner.logs.config.service.v1.UpdateRunnerLogsPersistenceConfigRequest;
import ai.traceable.runner.logs.config.service.v1.UpdateRunnerLogsPurgeConfigRequest;
import com.google.protobuf.Duration;
import java.util.Optional;
import org.hypertrace.config.objectstore.ConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RunnerLogsConfigManagerTest {

  @Mock RequestContext requestContext;
  @Mock RunnerLogsPurgeConfigStore runnerLogsPurgeConfigStore;
  @Mock RunnerLogsPersistenceConfigStore runnerLogsPersistenceConfigStore;
  @Mock RunnerLogsConfigServiceConfig runnerLogsConfigServiceConfig;
  @InjectMocks RunnerLogsConfigManager runnerLogsConfigManager;

  @Test
  void testGetRunnerLogsPurgeConfig_noPersistedConfig() {
    final RunnerLogsPurgeConfig defaultRunnerLogsPurgeConfig = buildDefaultRunnerLogsPurgeConfig();
    when(runnerLogsPurgeConfigStore.getData(any(RequestContext.class)))
        .thenReturn(Optional.empty());
    when(runnerLogsConfigServiceConfig.getDefaultRunnerLogsPurgeConfig())
        .thenReturn(defaultRunnerLogsPurgeConfig);

    assertEquals(
        defaultRunnerLogsPurgeConfig,
        runnerLogsConfigManager.getRunnerLogsPurgeConfig(requestContext));
  }

  @Test
  void testGetRunnerLogsPurgeConfig_persistedConfig() {
    final RunnerLogsPurgeConfig persistedRunnerLogsPurgeConfig =
        RunnerLogsPurgeConfig.newBuilder()
            .setAstScanLogsPurgeConfig(
                AstScanLogsPurgeConfig.newBuilder()
                    .setEnabled(true)
                    .setRetentionDuration(Duration.newBuilder().setSeconds(3000L)))
            .build();
    final RunnerLogsPurgeConfig defaultRunnerLogsPurgeConfig = buildDefaultRunnerLogsPurgeConfig();
    when(runnerLogsPurgeConfigStore.getData(any(RequestContext.class)))
        .thenReturn(Optional.of(persistedRunnerLogsPurgeConfig));
    when(runnerLogsConfigServiceConfig.getDefaultRunnerLogsPurgeConfig())
        .thenReturn(defaultRunnerLogsPurgeConfig);

    assertEquals(
        persistedRunnerLogsPurgeConfig,
        runnerLogsConfigManager.getRunnerLogsPurgeConfig(requestContext));
  }

  private RunnerLogsPurgeConfig buildDefaultRunnerLogsPurgeConfig() {
    return RunnerLogsPurgeConfig.newBuilder()
        .setAstScanLogsPurgeConfig(
            AstScanLogsPurgeConfig.newBuilder()
                .setEnabled(true)
                .setRetentionDuration(Duration.newBuilder().setSeconds(10000L)))
        .build();
  }

  @Test
  void testGetRunnerLogsPersistenceConfig_noPersistedConfig() {
    final RunnerLogsPersistenceConfig defaultRunnerLogsPersistenceConfig =
        buildDefaultRunnerLogsPersistenceConfig();
    when(runnerLogsPersistenceConfigStore.getData(any(RequestContext.class)))
        .thenReturn(Optional.empty());
    when(runnerLogsConfigServiceConfig.getDefaultRunnerLogsPersistenceConfig())
        .thenReturn(defaultRunnerLogsPersistenceConfig);

    assertEquals(
        defaultRunnerLogsPersistenceConfig,
        runnerLogsConfigManager.getRunnerLogsPersistenceConfig(requestContext));
  }

  @Test
  void testGetRunnerLogsPersistedConfig_persistedConfig() {
    final RunnerLogsPersistenceConfig persistedRunnerLogsPersistenceConfig =
        RunnerLogsPersistenceConfig.newBuilder()
            .setAstScanLogsPersistenceConfig(
                AstScanLogsPersistenceConfig.newBuilder().setMaxPersistLogLines(3000))
            .build();
    final RunnerLogsPersistenceConfig defaultRunnerLogsPersistenceConfig =
        buildDefaultRunnerLogsPersistenceConfig();
    when(runnerLogsPersistenceConfigStore.getData(any(RequestContext.class)))
        .thenReturn(Optional.of(persistedRunnerLogsPersistenceConfig));
    when(runnerLogsConfigServiceConfig.getDefaultRunnerLogsPersistenceConfig())
        .thenReturn(defaultRunnerLogsPersistenceConfig);

    assertEquals(
        persistedRunnerLogsPersistenceConfig,
        runnerLogsConfigManager.getRunnerLogsPersistenceConfig(requestContext));
  }

  private RunnerLogsPersistenceConfig buildDefaultRunnerLogsPersistenceConfig() {
    return RunnerLogsPersistenceConfig.newBuilder()
        .setAstScanLogsPersistenceConfig(
            AstScanLogsPersistenceConfig.newBuilder().setMaxPersistLogLines(1000))
        .build();
  }

  @Test
  @SuppressWarnings("unchecked")
  void testUpdateRunnerLogsPurgeConfig() {
    final RunnerLogsPurgeConfig runnerLogsPurgeConfig =
        RunnerLogsPurgeConfig.newBuilder()
            .setAstScanLogsPurgeConfig(
                AstScanLogsPurgeConfig.newBuilder()
                    .setEnabled(true)
                    .setRetentionDuration(Duration.newBuilder().setSeconds(3000L)))
            .build();
    final ConfigObject<RunnerLogsPurgeConfig> mockRunnerLogsPurgeConfigConfigObject =
        mock(ConfigObject.class);
    when(runnerLogsPurgeConfigStore.upsertObject(
            any(RequestContext.class), eq(runnerLogsPurgeConfig)))
        .thenReturn(mockRunnerLogsPurgeConfigConfigObject);
    when(mockRunnerLogsPurgeConfigConfigObject.getData()).thenReturn(runnerLogsPurgeConfig);
    when(runnerLogsConfigServiceConfig.getDefaultRunnerLogsPurgeConfig())
        .thenReturn(buildDefaultRunnerLogsPurgeConfig());

    assertEquals(
        runnerLogsPurgeConfig,
        runnerLogsConfigManager.updateAndGetRunnerLogsPurgeConfig(
            requestContext,
            UpdateRunnerLogsPurgeConfigRequest.newBuilder()
                .setRunnerLogsPurgeConfigUpdate(
                    RunnerLogsPurgeConfigUpdate.newBuilder()
                        .setAstScanLogsPurgeConfig(
                            AstScanLogsPurgeConfigUpdate.newBuilder()
                                .setEnabled(true)
                                .setRetentionDuration(Duration.newBuilder().setSeconds(3000L))))
                .build()));
  }

  @Test
  @SuppressWarnings("unchecked")
  void testUpdateRunnerLogsPersistenceConfig() {
    final RunnerLogsPersistenceConfig runnerLogsPersistenceConfig =
        RunnerLogsPersistenceConfig.newBuilder()
            .setAstScanLogsPersistenceConfig(
                AstScanLogsPersistenceConfig.newBuilder().setMaxPersistLogLines(3000))
            .build();
    final ConfigObject<RunnerLogsPersistenceConfig> mockRunnerLogsPersistenceConfigConfigObject =
        mock(ConfigObject.class);
    when(runnerLogsPersistenceConfigStore.upsertObject(
            any(RequestContext.class), eq(runnerLogsPersistenceConfig)))
        .thenReturn(mockRunnerLogsPersistenceConfigConfigObject);
    when(mockRunnerLogsPersistenceConfigConfigObject.getData())
        .thenReturn(runnerLogsPersistenceConfig);
    when(runnerLogsConfigServiceConfig.getDefaultRunnerLogsPersistenceConfig())
        .thenReturn(buildDefaultRunnerLogsPersistenceConfig());

    assertEquals(
        runnerLogsPersistenceConfig,
        runnerLogsConfigManager.updateAndGetRunnerLogsPersistenceConfig(
            requestContext,
            UpdateRunnerLogsPersistenceConfigRequest.newBuilder()
                .setRunnerLogsPersistenceConfigUpdate(
                    RunnerLogsPersistenceConfigUpdate.newBuilder()
                        .setAstScanLogsPersistenceConfig(
                            AstScanLogsPersistenceConfigUpdate.newBuilder()
                                .setMaxPersistLogLines(3000)))
                .build()));
  }
}
