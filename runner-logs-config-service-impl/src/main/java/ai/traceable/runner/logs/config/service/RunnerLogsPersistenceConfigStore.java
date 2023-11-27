package ai.traceable.runner.logs.config.service;

import static ai.traceable.runner.logs.config.service.RunnerLogsConfigConstants.RUNNER_LOGS_CONFIG_NAMESPACE;
import static ai.traceable.runner.logs.config.service.RunnerLogsConfigConstants.RUNNER_LOGS_PERSISTENCE_CONFIG_RESOURCE_NAME;

import ai.traceable.runner.logs.config.service.v1.RunnerLogsPersistenceConfig;
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
class RunnerLogsPersistenceConfigStore extends DefaultObjectStore<RunnerLogsPersistenceConfig> {

  @Inject
  RunnerLogsPersistenceConfigStore(
      final ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      final ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        RUNNER_LOGS_CONFIG_NAMESPACE,
        RUNNER_LOGS_PERSISTENCE_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<RunnerLogsPersistenceConfig> buildDataFromValue(Value value) {
    try {
      RunnerLogsPersistenceConfig.Builder builder = RunnerLogsPersistenceConfig.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (Exception exception) {
      log.error(
          "Parsing the value {} into RunnerLogsPersistenceConfig failed with an exception",
          value,
          exception);
      return Optional.empty();
    }
  }

  @Override
  @SneakyThrows
  protected Value buildValueFromData(final RunnerLogsPersistenceConfig config) {
    return ConfigProtoConverter.convertToValue(config);
  }
}
