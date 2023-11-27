package ai.traceable.runner.logs.config.service;

import ai.traceable.runner.logs.config.service.v1.RunnerLogsPersistenceConfig;
import ai.traceable.runner.logs.config.service.v1.RunnerLogsPurgeConfig;
import com.google.protobuf.InvalidProtocolBufferException;
import com.typesafe.config.Config;
import lombok.Getter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;

@Getter
class RunnerLogsConfigServiceConfig {

  private static final String RUNNER_LOGS_CONFIG_PATH = "runner.logs.config";
  private static final String DEFAULT_RUNNER_LOGS_PURGE_CONFIG_KEY = "defaultRunnerLogsPurgeConfig";
  private static final String DEFAULT_RUNNER_LOGS_PERSISTENCE_CONFIG_KEY =
      "defaultRunnerLogsPersistenceConfig";

  private final RunnerLogsPurgeConfig defaultRunnerLogsPurgeConfig;
  private final RunnerLogsPersistenceConfig defaultRunnerLogsPersistenceConfig;

  RunnerLogsConfigServiceConfig(final Config config) throws InvalidProtocolBufferException {
    final Config runnerLogsConfig = config.getConfig(RUNNER_LOGS_CONFIG_PATH);
    this.defaultRunnerLogsPurgeConfig = parseDefaultRunnerLogsPurgeConfig(runnerLogsConfig);
    this.defaultRunnerLogsPersistenceConfig =
        parseDefaultRunnerLogsPersistenceConfig(runnerLogsConfig);
  }

  private RunnerLogsPurgeConfig parseDefaultRunnerLogsPurgeConfig(final Config runnerLogsConfig)
      throws InvalidProtocolBufferException {
    RunnerLogsPurgeConfig.Builder runnerLogsPurgeConfigBuilder = RunnerLogsPurgeConfig.newBuilder();
    ConfigProtoConverter.mergeFromJsonString(
        runnerLogsConfig.getObject(DEFAULT_RUNNER_LOGS_PURGE_CONFIG_KEY).render(),
        runnerLogsPurgeConfigBuilder);
    return runnerLogsPurgeConfigBuilder.build();
  }

  private RunnerLogsPersistenceConfig parseDefaultRunnerLogsPersistenceConfig(
      final Config runnerLogsConfig) throws InvalidProtocolBufferException {
    RunnerLogsPersistenceConfig.Builder runnerLogsPersistenceConfigBuilder =
        RunnerLogsPersistenceConfig.newBuilder();
    ConfigProtoConverter.mergeFromJsonString(
        runnerLogsConfig.getObject(DEFAULT_RUNNER_LOGS_PERSISTENCE_CONFIG_KEY).render(),
        runnerLogsPersistenceConfigBuilder);
    return runnerLogsPersistenceConfigBuilder.build();
  }
}
