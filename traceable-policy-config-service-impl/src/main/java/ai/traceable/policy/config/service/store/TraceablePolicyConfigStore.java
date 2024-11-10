package ai.traceable.policy.config.service.store;

import ai.traceable.policy.config.service.v1.TraceablePolicy;
import com.google.protobuf.Value;
import java.util.Optional;
import javax.inject.Inject;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;

public class TraceablePolicyConfigStore extends IdentifiedObjectStore<TraceablePolicy> {
  public static final String TRACEABLE_POLICY_CONFIG_RESOURCE_NAMESPACE =
      "traceable-policy-config-namespace";

  public static final String TRACEABLE_POLICY_CONFIG_RESOURCE = "traceable-policy-config-resource";

  @Inject
  public TraceablePolicyConfigStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        TRACEABLE_POLICY_CONFIG_RESOURCE_NAMESPACE,
        TRACEABLE_POLICY_CONFIG_RESOURCE,
        configChangeEventGenerator);
  }

  @SneakyThrows
  @Override
  protected Optional<TraceablePolicy> buildDataFromValue(Value value) {
    TraceablePolicy.Builder builder = TraceablePolicy.newBuilder();
    ConfigProtoConverter.mergeFromValue(value, builder);
    return Optional.of(builder.build());
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(TraceablePolicy traceablePolicy) {
    return ConfigProtoConverter.convertToValue(traceablePolicy);
  }

  @Override
  protected String getContextFromData(TraceablePolicy traceablePolicy) {
    return traceablePolicy.getId();
  }
}
