package ai.traceable.span.processing.config.service.store;

import ai.traceable.config.utils.TimestampConverter;
import ai.traceable.span.processing.config.service.SpanProcessingConfigConstants;
import ai.traceable.span.processing.config.service.v1.SamplingConfig;
import ai.traceable.span.processing.config.service.v1.SamplingConfigDetails;
import ai.traceable.span.processing.config.service.v1.SamplingConfigMetadata;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class SamplingConfigsConfigStore extends IdentifiedObjectStore<SamplingConfig> {

  private static final String SAMPLING_CONFIGS_RESOURCE_NAME = "sampling-configs";
  private final TimestampConverter timestampConverter;

  @Inject
  public SamplingConfigsConfigStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      TimestampConverter timestampConverter,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        SpanProcessingConfigConstants.RESOURCE_NAMESPACE,
        SAMPLING_CONFIGS_RESOURCE_NAME,
        configChangeEventGenerator);
    this.timestampConverter = timestampConverter;
  }

  public List<SamplingConfigDetails> getAllData(RequestContext requestContext) {
    return this.getAllObjects(requestContext).stream()
        .map(
            contextualConfigObject ->
                SamplingConfigDetails.newBuilder()
                    .setSamplingConfig(contextualConfigObject.getData())
                    .setMetadata(
                        SamplingConfigMetadata.newBuilder()
                            .setCreationTimestamp(
                                timestampConverter.convert(
                                    contextualConfigObject.getCreationTimestamp()))
                            .setLastUpdatedTimestamp(
                                timestampConverter.convert(
                                    contextualConfigObject.getLastUpdatedTimestamp()))
                            .build())
                    .build())
        .collect(Collectors.toUnmodifiableList());
  }

  @SneakyThrows
  @Override
  protected Optional<SamplingConfig> buildDataFromValue(Value value) {
    SamplingConfig.Builder configBuilder = SamplingConfig.newBuilder();
    ConfigProtoConverter.mergeFromValue(value, configBuilder);
    return Optional.of(configBuilder.build());
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(SamplingConfig samplingConfig) {
    return ConfigProtoConverter.convertToValue(samplingConfig);
  }

  @Override
  protected String getContextFromData(SamplingConfig samplingConfig) {
    return samplingConfig.getId();
  }
}
