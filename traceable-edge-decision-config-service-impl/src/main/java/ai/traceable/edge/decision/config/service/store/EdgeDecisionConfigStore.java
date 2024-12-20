package ai.traceable.edge.decision.config.service.store;

import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
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

public class EdgeDecisionConfigStore extends IdentifiedObjectStore<EdgeDecisionEngineConfig> {
  public static final String EDGE_DECISION_CONFIG_NAMESPACE = "edge-decision-config-namespace";
  public static final String EDGE_DECISION_CONFIG_RESOURCE = "edge-decision-config";

  @Inject
  public EdgeDecisionConfigStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        EDGE_DECISION_CONFIG_NAMESPACE,
        EDGE_DECISION_CONFIG_RESOURCE,
        configChangeEventGenerator);
  }

  @SneakyThrows
  @Override
  protected Optional<EdgeDecisionEngineConfig> buildDataFromValue(Value value) {
    EdgeDecisionEngineConfig.Builder builder = EdgeDecisionEngineConfig.newBuilder();
    ConfigProtoConverter.mergeFromValue(value, builder);
    return Optional.of(builder.build());
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(EdgeDecisionEngineConfig edgeDecisionEngineConfig) {
    return ConfigProtoConverter.convertToValue(edgeDecisionEngineConfig);
  }

  @Override
  protected String getContextFromData(EdgeDecisionEngineConfig edgeDecisionEngineConfig) {
    return edgeDecisionEngineConfig.getId();
  }

  public EdgeDecisionEngineConfig fetchExisting(String id, RequestContext requestContext)
      throws StatusException {
    return this.getData(requestContext, id).orElseThrow(Status.NOT_FOUND::asException);
  }
}
