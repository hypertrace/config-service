package ai.traceable.runner.logs.config.service;

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
import javax.inject.Inject;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = {@Inject})
class RunnerLogsConfigManager {

  private final RunnerLogsPurgeConfigStore runnerLogsPurgeConfigStore;
  private final RunnerLogsPersistenceConfigStore runnerLogsPersistenceConfigStore;
  private final RunnerLogsConfigServiceConfig runnerLogsConfigServiceConfig;

  RunnerLogsPurgeConfig getRunnerLogsPurgeConfig(final RequestContext requestContext) {
    return runnerLogsPurgeConfigStore
        .getData(requestContext)
        .orElse(runnerLogsConfigServiceConfig.getDefaultRunnerLogsPurgeConfig());
  }

  RunnerLogsPersistenceConfig getRunnerLogsPersistenceConfig(final RequestContext requestContext) {
    return runnerLogsPersistenceConfigStore
        .getData(requestContext)
        .orElse(runnerLogsConfigServiceConfig.getDefaultRunnerLogsPersistenceConfig());
  }

  RunnerLogsPurgeConfig updateAndGetRunnerLogsPurgeConfig(
      final RequestContext requestContext, final UpdateRunnerLogsPurgeConfigRequest request) {
    final RunnerLogsPurgeConfig currentRunnerLogsPurgeConfig =
        getRunnerLogsPurgeConfig(requestContext);
    final RunnerLogsPurgeConfig updatedRunnerLogsPurgeConfig =
        buildUpdatedRunnerLogsPurgeConfig(
            currentRunnerLogsPurgeConfig, request.getRunnerLogsPurgeConfigUpdate());
    return runnerLogsPurgeConfigStore
        .upsertObject(requestContext, updatedRunnerLogsPurgeConfig)
        .getData();
  }

  RunnerLogsPersistenceConfig updateAndGetRunnerLogsPersistenceConfig(
      final RequestContext requestContext, final UpdateRunnerLogsPersistenceConfigRequest request) {
    final RunnerLogsPersistenceConfig currentRunnerLogsPersistenceConfig =
        getRunnerLogsPersistenceConfig(requestContext);
    final RunnerLogsPersistenceConfig updatedRunnerLogsPersistenceConfig =
        buildUpdatedRunnerLogsPersistenceConfig(
            currentRunnerLogsPersistenceConfig, request.getRunnerLogsPersistenceConfigUpdate());
    return runnerLogsPersistenceConfigStore
        .upsertObject(requestContext, updatedRunnerLogsPersistenceConfig)
        .getData();
  }

  private RunnerLogsPurgeConfig buildUpdatedRunnerLogsPurgeConfig(
      final RunnerLogsPurgeConfig currentLogsPurgeConfig,
      final RunnerLogsPurgeConfigUpdate runnerLogsPurgeConfigUpdate) {
    final RunnerLogsPurgeConfig.Builder updatedRunnerLogsPurgeConfigBuilder =
        RunnerLogsPurgeConfig.newBuilder(currentLogsPurgeConfig);
    if (runnerLogsPurgeConfigUpdate.hasAstScanLogsPurgeConfig()) {
      updatedRunnerLogsPurgeConfigBuilder.setAstScanLogsPurgeConfig(
          buildUpdatedAstScanLogsPurgeConfig(
              runnerLogsPurgeConfigUpdate.getAstScanLogsPurgeConfig()));
    }
    return updatedRunnerLogsPurgeConfigBuilder.build();
  }

  private AstScanLogsPurgeConfig buildUpdatedAstScanLogsPurgeConfig(
      final AstScanLogsPurgeConfigUpdate astScanLogsPurgeConfigUpdate) {
    return AstScanLogsPurgeConfig.newBuilder()
        .setEnabled(astScanLogsPurgeConfigUpdate.getEnabled())
        .setRetentionDuration(astScanLogsPurgeConfigUpdate.getRetentionDuration())
        .build();
  }

  private RunnerLogsPersistenceConfig buildUpdatedRunnerLogsPersistenceConfig(
      final RunnerLogsPersistenceConfig currentRunnerLogsPersistenceConfig,
      final RunnerLogsPersistenceConfigUpdate runnerLogsPersistenceConfigUpdate) {
    final RunnerLogsPersistenceConfig.Builder updatedRunnerLogsPersistenceConfigBuilder =
        RunnerLogsPersistenceConfig.newBuilder(currentRunnerLogsPersistenceConfig);
    if (runnerLogsPersistenceConfigUpdate.hasAstScanLogsPersistenceConfig()) {
      updatedRunnerLogsPersistenceConfigBuilder.setAstScanLogsPersistenceConfig(
          buildUpdatedAstScanLogsPersistenceConfig(
              runnerLogsPersistenceConfigUpdate.getAstScanLogsPersistenceConfig()));
    }
    return updatedRunnerLogsPersistenceConfigBuilder.build();
  }

  private AstScanLogsPersistenceConfig buildUpdatedAstScanLogsPersistenceConfig(
      final AstScanLogsPersistenceConfigUpdate astScanLogsPersistenceConfig) {
    final AstScanLogsPersistenceConfig.Builder astScanLogsPeristenceConfigBuilder =
        AstScanLogsPersistenceConfig.newBuilder();
    if (astScanLogsPersistenceConfig.hasMaxPersistLogLines()) {
      astScanLogsPeristenceConfigBuilder.setMaxPersistLogLines(
          astScanLogsPersistenceConfig.getMaxPersistLogLines());
    }
    return astScanLogsPeristenceConfigBuilder.build();
  }
}
