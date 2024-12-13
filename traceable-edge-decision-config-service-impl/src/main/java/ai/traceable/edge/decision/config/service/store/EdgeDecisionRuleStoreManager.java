package ai.traceable.edge.decision.config.service.store;

import ai.traceable.edge.decision.config.service.v1.CreateEdgeDecisionRuleRequest;
import ai.traceable.edge.decision.config.service.v1.CreateEdgeDecisionRuleResponse;
import ai.traceable.edge.decision.config.service.v1.DeleteEdgeDecisionRuleRequest;
import ai.traceable.edge.decision.config.service.v1.DeleteEdgeDecisionRuleResponse;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRule;
import ai.traceable.edge.decision.config.service.v1.GetAllEdgeDecisionRulesRequest;
import ai.traceable.edge.decision.config.service.v1.GetAllEdgeDecisionRulesResponse;
import ai.traceable.edge.decision.config.service.v1.UpdateEdgeDecisionRuleRequest;
import ai.traceable.edge.decision.config.service.v1.UpdateEdgeDecisionRuleResponse;
import io.grpc.Status;
import java.util.List;
import javax.inject.Inject;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class EdgeDecisionRuleStoreManager {
  private final EdgeDecisionRuleStore edgeDecisionRuleStore;

  @Inject
  public EdgeDecisionRuleStoreManager(EdgeDecisionRuleStore edgeDecisionRuleStore) {
    this.edgeDecisionRuleStore = edgeDecisionRuleStore;
  }

  public CreateEdgeDecisionRuleResponse create(
      RequestContext requestContext, CreateEdgeDecisionRuleRequest request) {
    var edgeDecisionRule = request.getEdgeDecisionRule();
    ContextualConfigObject<EdgeDecisionRule> configObject =
        edgeDecisionRuleStore.upsertObject(requestContext, edgeDecisionRule);
    return CreateEdgeDecisionRuleResponse.newBuilder()
        .setEdgeDecisionRule(configObject.getData())
        .build();
  }

  @SneakyThrows
  public UpdateEdgeDecisionRuleResponse update(
      RequestContext requestContext, UpdateEdgeDecisionRuleRequest request) {
    var edgeDecisionRule = request.getEdgeDecisionRule();
    EdgeDecisionRule existing =
        edgeDecisionRuleStore.fetchExisting(edgeDecisionRule.getId(), requestContext);
    if (existing.getVersion() != request.getCurrentVersion()) {
      throw Status.FAILED_PRECONDITION
          .withDescription(
              String.format(
                  "Received current version=%d, Existing policy version=%d. Read latest and update again",
                  request.getCurrentVersion(), existing.getVersion()))
          .asException();
    }
    ContextualConfigObject<EdgeDecisionRule> configObject =
        edgeDecisionRuleStore.upsertObject(requestContext, edgeDecisionRule);
    return UpdateEdgeDecisionRuleResponse.newBuilder()
        .setEdgeDecisionRule(configObject.getData())
        .build();
  }

  public GetAllEdgeDecisionRulesResponse getAll(
      RequestContext requestContext, GetAllEdgeDecisionRulesRequest request) {
    List<EdgeDecisionRule> rules =
        edgeDecisionRuleStore.getRules(requestContext, request.getFilter());
    return GetAllEdgeDecisionRulesResponse.newBuilder().addAllEdgeDecisionRules(rules).build();
  }

  public DeleteEdgeDecisionRuleResponse delete(
      RequestContext requestContext, DeleteEdgeDecisionRuleRequest request) {
    var response = edgeDecisionRuleStore.deleteObject(requestContext, request.getId());
    if (response.isPresent() && response.get().getDeletedData().isPresent()) {
      return DeleteEdgeDecisionRuleResponse.newBuilder()
          .setDeletedEdgeDecisionRule(response.get().getDeletedData().get())
          .build();
    }
    return DeleteEdgeDecisionRuleResponse.getDefaultInstance();
  }
}
