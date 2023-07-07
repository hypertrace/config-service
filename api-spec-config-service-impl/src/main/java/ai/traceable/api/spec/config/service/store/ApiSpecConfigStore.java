package ai.traceable.api.spec.config.service.store;

import ai.traceable.api.spec.config.service.v1.ApiSpec;
import ai.traceable.config.utils.TimestampConverter;
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

public class ApiSpecConfigStore extends IdentifiedObjectStore<ApiSpec> {

  private static final String API_SPEC_CONFIG_RESOURCE_NAME = "api-spec";
  private static final String API_SPEC_CONFIG_RESOURCE_NAMESPACE = "api-spec-config";
  private final TimestampConverter timestampConverter;

  @Inject
  public ApiSpecConfigStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      TimestampConverter timestampConverter,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        API_SPEC_CONFIG_RESOURCE_NAMESPACE,
        API_SPEC_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
    this.timestampConverter = timestampConverter;
  }

  public List<ApiSpec> getAllData(RequestContext requestContext) {
    return this.getAllObjects(requestContext).stream()
        .map(
            contextualConfigObject ->
                ApiSpec.newBuilder(contextualConfigObject.getData())
                    .setCreationTimestamp(
                        timestampConverter.convert(contextualConfigObject.getCreationTimestamp()))
                    .setLastUpdatedTimestamp(
                        timestampConverter.convert(
                            contextualConfigObject.getLastUpdatedTimestamp()))
                    .build())
        .collect(Collectors.toUnmodifiableList());
  }

  @SneakyThrows
  @Override
  protected Optional<ApiSpec> buildDataFromValue(Value value) {
    ApiSpec.Builder configBuilder = ApiSpec.newBuilder();
    ConfigProtoConverter.mergeFromValue(value, configBuilder);
    return Optional.of(configBuilder.build());
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(ApiSpec apiSpec) {
    return ConfigProtoConverter.convertToValue(apiSpec);
  }

  @Override
  protected String getContextFromData(ApiSpec apiSpec) {
    return apiSpec.getSpecId();
  }
}
