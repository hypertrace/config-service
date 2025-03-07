package ai.traceable.edge.decision.config.service.store;

import ai.traceable.edge.decision.config.service.v1.EdgeCustomResponse;
import com.google.protobuf.Value;
import io.grpc.Status;
import io.grpc.StatusException;
import jakarta.inject.Inject;
import java.util.Optional;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class EdgeCustomResponseStore extends IdentifiedObjectStore<EdgeCustomResponse> {
  public static final String EDGE_CUSTOM_RESPONSE_NAMESPACE = "edge-custom-response-namespace";
  public static final String EDGE_CUSTOM_RESPONSE_RESOURCE = "edge-custom-response";

  @Inject
  public EdgeCustomResponseStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        EDGE_CUSTOM_RESPONSE_NAMESPACE,
        EDGE_CUSTOM_RESPONSE_RESOURCE,
        configChangeEventGenerator);
  }

  @SneakyThrows
  @Override
  protected Optional<EdgeCustomResponse> buildDataFromValue(Value value) {
    EdgeCustomResponse.Builder builder = EdgeCustomResponse.newBuilder();
    ConfigProtoConverter.mergeFromValue(value, builder);
    return Optional.of(builder.build());
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(EdgeCustomResponse edgeCustomResponse) {
    return ConfigProtoConverter.convertToValue(edgeCustomResponse);
  }

  @Override
  protected String getContextFromData(EdgeCustomResponse edgeCustomResponse) {
    return edgeCustomResponse.getId();
  }

  public EdgeCustomResponse fetchExisting(String id, RequestContext requestContext)
      throws StatusException {
    return this.getData(requestContext, id).orElseThrow(Status.NOT_FOUND::asException);
  }
}
