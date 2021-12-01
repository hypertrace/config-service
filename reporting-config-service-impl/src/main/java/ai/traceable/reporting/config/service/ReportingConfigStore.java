package ai.traceable.reporting.config.service;

import ai.traceable.reporting.config.service.v1.ReportConfiguration;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import io.grpc.Channel;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

@Slf4j
public class ReportingConfigStore extends IdentifiedObjectStore<ReportConfiguration> {

  private static final String REPORTING_CONFIG_NAMESPACE = "reporting";
  private static final String REPORTING_CONFIG_RESOURCE_NAME = "reporting-config";

  public ReportingConfigStore(
      Channel channel, ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        ConfigServiceGrpc.newBlockingStub(channel)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get()),
        REPORTING_CONFIG_NAMESPACE,
        REPORTING_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<ReportConfiguration> buildDataFromValue(Value value) {
    ReportConfiguration.Builder builder = ReportConfiguration.newBuilder();
    try {
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (InvalidProtocolBufferException e) {
      log.error("Conversion failed. value {}", value, e);
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(ReportConfiguration object) {
    return ConfigProtoConverter.convertToValue(object);
  }

  @Override
  protected String getContextFromData(ReportConfiguration object) {
    return object.getId();
  }
}
