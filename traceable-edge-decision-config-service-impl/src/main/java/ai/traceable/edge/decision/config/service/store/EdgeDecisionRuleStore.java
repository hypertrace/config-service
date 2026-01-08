package ai.traceable.edge.decision.config.service.store;

import ai.traceable.config.commons.v1.AuditDetails;
import ai.traceable.config.commons.v1.CreationDetails;
import ai.traceable.config.commons.v1.LastUpdateDetails;
import ai.traceable.config.utils.TimestampConverter;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRule;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleRecord;
import ai.traceable.edge.decision.config.service.v1.Filter;
import com.google.protobuf.Value;
import io.grpc.Status;
import io.grpc.StatusException;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.ConfigObject;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class EdgeDecisionRuleStore extends IdentifiedObjectStore<EdgeDecisionRule> {
  public static final String EDGE_DECISION_RULE_NAMESPACE = "edge-decision-rule-namespace";
  public static final String EDGE_DECISION_RULE_RESOURCE = "edge-decision-rule";

  private final FilterEvaluator filterEvaluator;
  private final TimestampConverter timestampConverter;

  @Inject
  public EdgeDecisionRuleStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator,
      FilterEvaluator filterEvaluator,
      TimestampConverter timestampConverter) {
    super(
        configServiceBlockingStub,
        EDGE_DECISION_RULE_NAMESPACE,
        EDGE_DECISION_RULE_RESOURCE,
        configChangeEventGenerator);
    this.filterEvaluator = filterEvaluator;
    this.timestampConverter = timestampConverter;
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

  public List<EdgeDecisionRule> getRules(RequestContext context, Filter filter) {
    return getAllObjects(context).stream()
        .filter(ruleWithContext -> filterEvaluator.evaluate(filter, ruleWithContext))
        .map(ConfigObject::getData)
        .collect(Collectors.toUnmodifiableList());
  }

  public List<EdgeDecisionRuleRecord> getRuleRecords(RequestContext context, Filter filter) {
    return getAllObjects(context).stream()
        .filter(ruleWithContext -> filterEvaluator.evaluate(filter, ruleWithContext))
        .map(this::toRuleRecord)
        .collect(Collectors.toUnmodifiableList());
  }

  private EdgeDecisionRuleRecord toRuleRecord(ContextualConfigObject<EdgeDecisionRule> contextual) {
    return EdgeDecisionRuleRecord.newBuilder()
        .setRule(contextual.getData())
        .setAuditDetails(buildAuditDetails(contextual))
        .build();
  }

  private AuditDetails buildAuditDetails(ContextualConfigObject<?> contextual) {
    AuditDetails.Builder builder = AuditDetails.newBuilder();

    Instant creationTimestamp = contextual.getCreationTimestamp();
    if (creationTimestamp != null && creationTimestamp.getEpochSecond() > 0) {
      builder.setCreationDetails(
          CreationDetails.newBuilder()
              .setCreatedBy(contextual.getCreatedByEmail())
              .setCreatedAt(timestampConverter.convert(creationTimestamp))
              .build());
    }

    Instant lastUserUpdateTimestamp = contextual.getLastUserUpdateTimestamp();
    if (lastUserUpdateTimestamp != null && lastUserUpdateTimestamp.getEpochSecond() > 0) {
      builder.setLastUserUpdateDetails(
          LastUpdateDetails.newBuilder()
              .setUpdatedBy(contextual.getLastUserUpdateEmail())
              .setUpdatedAt(timestampConverter.convert(lastUserUpdateTimestamp))
              .build());
    }

    return builder.build();
  }
}
