package ai.traceable.runner.logs.config.service;

import static ai.traceable.runner.logs.config.service.RunnerLogsConfigConstants.RUNNER_LOGS_CONFIG_NAMESPACE;
import static ai.traceable.runner.logs.config.service.RunnerLogsConfigConstants.RUNNER_LOGS_PURGE_CONFIG_RESOURCE_NAME;

import ai.traceable.runner.logs.config.service.v1.RunnerLogsPurgeConfig;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.DefaultObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;

@Slf4j
class RunnerLogsPurgeConfigStore extends DefaultObjectStore<RunnerLogsPurgeConfig> {

  @Inject
  RunnerLogsPurgeConfigStore(
      final ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      final ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        RUNNER_LOGS_CONFIG_NAMESPACE,
        RUNNER_LOGS_PURGE_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<RunnerLogsPurgeConfig> buildDataFromValue(final Value value) {
    try {
      RunnerLogsPurgeConfig.Builder builder = RunnerLogsPurgeConfig.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (Exception exception) {
      log.error(
          "Parsing the value {} into RunnerLogsPurgeConfig failed with an exception",
          value,
          exception);
      return Optional.empty();
    }
  }

  @Override
  @SneakyThrows
  protected Value buildValueFromData(final RunnerLogsPurgeConfig config) {
    return ConfigProtoConverter.convertToValue(config);
  }
}
