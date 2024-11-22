package ai.traceable.edge.decision.config.service.store;

import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRule;
import com.google.protobuf.Value;
import io.grpc.Status;
import io.grpc.StatusException;
import java.util.Optional;
import javax.inject.Inject;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class EdgeDecisionRuleStore extends IdentifiedObjectStore<EdgeDecisionRule> {
  public static final String EDGE_DECISION_RULE_NAMESPACE = "edge-decision-rule-namespace";
  public static final String EDGE_DECISION_RULE_RESOURCE = "edge-decision-rule";

  @Inject
  public EdgeDecisionRuleStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        EDGE_DECISION_RULE_NAMESPACE,
        EDGE_DECISION_RULE_RESOURCE,
        configChangeEventGenerator);
  }

  @SneakyThrows
  @Override
  protected Optional<EdgeDecisionRule> buildDataFromValue(Value value) {
    EdgeDecisionRule.Builder builder = EdgeDecisionRule.newBuilder();
    ConfigProtoConverter.mergeFromValue(value, builder);
    return Optional.of(builder.build());
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(EdgeDecisionRule edgeDecisionRule) {
    return ConfigProtoConverter.convertToValue(edgeDecisionRule);
  }

  @Override
  protected String getContextFromData(EdgeDecisionRule edgeDecisionRule) {
    return edgeDecisionRule.getId();
  }

  public EdgeDecisionRule fetchExisting(String id, RequestContext requestContext)
      throws StatusException {
    return this.getData(requestContext, id).orElseThrow(Status.NOT_FOUND::asException);
  }
}
