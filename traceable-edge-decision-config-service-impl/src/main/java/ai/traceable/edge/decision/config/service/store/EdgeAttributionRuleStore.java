package ai.traceable.edge.decision.config.service.store;

import ai.traceable.edge.decision.config.service.v1.EdgeAttributionRule;
import com.google.protobuf.Value;
import io.grpc.Status;
import io.grpc.StatusException;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.ConfigObject;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class EdgeAttributionRuleStore extends IdentifiedObjectStore<EdgeAttributionRule> {
  public static final String EDGE_ATTRIBUTION_RULE_NAMESPACE = "edge-attribution-rule-namespace";
  public static final String EDGE_ATTRIBUTION_RULE_RESOURCE = "edge-attribution-rule";

  @Inject
  public EdgeAttributionRuleStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        EDGE_ATTRIBUTION_RULE_NAMESPACE,
        EDGE_ATTRIBUTION_RULE_RESOURCE,
        configChangeEventGenerator);
  }

  @SneakyThrows
  @Override
  protected Optional<EdgeAttributionRule> buildDataFromValue(Value value) {
    EdgeAttributionRule.Builder builder = EdgeAttributionRule.newBuilder();
    ConfigProtoConverter.mergeFromValue(value, builder);
    return Optional.of(builder.build());
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(EdgeAttributionRule rule) {
    return ConfigProtoConverter.convertToValue(rule);
  }

  @Override
  protected String getContextFromData(EdgeAttributionRule rule) {
    return rule.getId();
  }

  public EdgeAttributionRule fetchExisting(String id, RequestContext requestContext)
      throws StatusException {
    return this.getData(requestContext, id).orElseThrow(Status.NOT_FOUND::asException);
  }

  public List<EdgeAttributionRule> getRules(RequestContext context) {
    return getAllObjects(context).stream()
        .map(ConfigObject::getData)
        .collect(Collectors.toUnmodifiableList());
  }
}
