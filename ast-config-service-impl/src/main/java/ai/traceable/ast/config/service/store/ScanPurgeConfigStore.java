package ai.traceable.ast.config.service.store;

import ai.traceable.ast.config.service.v1.ScanPurgeConfig;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.DefaultObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;

@Slf4j
public class ScanPurgeConfigStore extends DefaultObjectStore<ScanPurgeConfig> {

  private static final String SCAN_PURGE_CONFIG_NAMESPACE = "scan-purge";
  private static final String SCAN_PURGE_CONFIG_RESOURCE_NAME = "scan-purge-config";

  @Inject
  public ScanPurgeConfigStore(ConfigServiceBlockingStub configServiceBlockingStub) {
    super(configServiceBlockingStub, SCAN_PURGE_CONFIG_NAMESPACE, SCAN_PURGE_CONFIG_RESOURCE_NAME);
  }

  @Override
  protected Optional<ScanPurgeConfig> buildDataFromValue(Value value) {
    try {
      ScanPurgeConfig.Builder builder = ScanPurgeConfig.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (Exception exception) {
      log.error(
          "Parsing the value {} into ScanPurgeConfig failed with an exception", value, exception);
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(ScanPurgeConfig config) {
    return ConfigProtoConverter.convertToValue(config);
  }
}
