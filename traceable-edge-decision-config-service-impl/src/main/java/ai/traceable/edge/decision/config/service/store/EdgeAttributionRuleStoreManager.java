package ai.traceable.edge.decision.config.service.store;

import static ai.traceable.config.proto.utils.FieldMaskUtils.applyFieldMask;

import ai.traceable.edge.decision.config.service.v1.CreateEdgeAttributionRuleRequest;
import ai.traceable.edge.decision.config.service.v1.CreateEdgeAttributionRuleResponse;
import ai.traceable.edge.decision.config.service.v1.DeleteEdgeAttributionRuleRequest;
import ai.traceable.edge.decision.config.service.v1.DeleteEdgeAttributionRuleResponse;
import ai.traceable.edge.decision.config.service.v1.EdgeAttributionRule;
import ai.traceable.edge.decision.config.service.v1.GetAllEdgeAttributionRulesRequest;
import ai.traceable.edge.decision.config.service.v1.GetAllEdgeAttributionRulesResponse;
import ai.traceable.edge.decision.config.service.v1.GetEdgeAttributionRuleRequest;
import ai.traceable.edge.decision.config.service.v1.GetEdgeAttributionRuleResponse;
import ai.traceable.edge.decision.config.service.v1.UpdateEdgeAttributionRuleRequest;
import ai.traceable.edge.decision.config.service.v1.UpdateEdgeAttributionRuleResponse;
import io.grpc.Status;
import jakarta.inject.Inject;
import java.util.List;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class EdgeAttributionRuleStoreManager {
  private final EdgeAttributionRuleStore store;

  @Inject
  public EdgeAttributionRuleStoreManager(EdgeAttributionRuleStore store) {
    this.store = store;
  }

  public CreateEdgeAttributionRuleResponse create(
      RequestContext requestContext, CreateEdgeAttributionRuleRequest request) {
    var rule = request.getEdgeAttributionRule();
    ContextualConfigObject<EdgeAttributionRule> configObject =
        store.upsertObject(requestContext, rule);
    return CreateEdgeAttributionRuleResponse.newBuilder()
        .setEdgeAttributionRule(configObject.getData())
        .build();
  }

  @SneakyThrows
  public GetEdgeAttributionRuleResponse get(
      RequestContext requestContext, GetEdgeAttributionRuleRequest request) {
    EdgeAttributionRule rule = store.fetchExisting(request.getId(), requestContext);
    return GetEdgeAttributionRuleResponse.newBuilder().setEdgeAttributionRule(rule).build();
  }

  @SneakyThrows
  public UpdateEdgeAttributionRuleResponse update(
      RequestContext requestContext, UpdateEdgeAttributionRuleRequest request) {
    EdgeAttributionRule existing =
        store.fetchExisting(request.getEdgeAttributionRule().getId(), requestContext);
    if (existing.getVersion() != request.getCurrentVersion()) {
      throw Status.FAILED_PRECONDITION
          .withDescription(
              String.format(
                  "Received current version=%d, Existing policy version=%d. Read latest and update again",
                  request.getCurrentVersion(), existing.getVersion()))
          .asException();
    }
    // Merge the existing object with the new object, using update masks
    EdgeAttributionRule updatedResource =
        applyFieldMask(existing, request.getEdgeAttributionRule(), request.getUpdateMask());

    ContextualConfigObject<EdgeAttributionRule> configObject =
        store.upsertObject(requestContext, updatedResource);
    return UpdateEdgeAttributionRuleResponse.newBuilder()
        .setEdgeAttributionRule(configObject.getData())
        .build();
  }

  public GetAllEdgeAttributionRulesResponse getAll(
      RequestContext requestContext, GetAllEdgeAttributionRulesRequest request) {
    List<EdgeAttributionRule> rules = store.getRules(requestContext);
    return GetAllEdgeAttributionRulesResponse.newBuilder()
        .addAllEdgeAttributionRules(rules)
        .build();
  }

  public DeleteEdgeAttributionRuleResponse delete(
      RequestContext requestContext, DeleteEdgeAttributionRuleRequest request) {
    var response = store.deleteObject(requestContext, request.getId());
    if (response.isPresent() && response.get().getDeletedData().isPresent()) {
      return DeleteEdgeAttributionRuleResponse.newBuilder()
          .setDeletedEdgeAttributionRule(response.get().getDeletedData().get())
          .build();
    }
    return DeleteEdgeAttributionRuleResponse.getDefaultInstance();
  }
}
