package ai.traceable.edge.decision.config.service.store;

import ai.traceable.edge.decision.config.service.v1.EdgeDecisionSpec;
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

public class EdgeDecisionSpecStore extends IdentifiedObjectStore<EdgeDecisionSpec> {
  public static final String EDGE_DECISION_SPEC_NAMESPACE = "edge-decision-spec-namespace";
  public static final String EDGE_DECISION_SPEC_RESOURCE = "edge-decision-spec";

  @Inject
  public EdgeDecisionSpecStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        EDGE_DECISION_SPEC_NAMESPACE,
        EDGE_DECISION_SPEC_RESOURCE,
        configChangeEventGenerator);
  }

  @SneakyThrows
  @Override
  protected Optional<EdgeDecisionSpec> buildDataFromValue(Value value) {
    EdgeDecisionSpec.Builder builder = EdgeDecisionSpec.newBuilder();
    ConfigProtoConverter.mergeFromValue(value, builder);
    return Optional.of(builder.build());
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(EdgeDecisionSpec edgeDecisionSpec) {
    return ConfigProtoConverter.convertToValue(edgeDecisionSpec);
  }

  @Override
  protected String getContextFromData(EdgeDecisionSpec edgeDecisionSpec) {
    return edgeDecisionSpec.getId();
  }

  public EdgeDecisionSpec fetchExisting(String id, RequestContext requestContext)
      throws StatusException {
    return this.getData(requestContext, id).orElseThrow(Status.NOT_FOUND::asException);
  }
}
