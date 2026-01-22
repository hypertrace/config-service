package ai.traceable.edge.decision.config.service.supplier.userattribution;

import ai.traceable.edge.decision.config.service.supplier.EdgeDecisionEngineConfigSupplier;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.edge.decision.config.service.v1.EdgeInputKind;
import ai.traceable.edge.decision.config.service.v1.GetEdgeDecisionConfigsFilter;
import ai.traceable.userattribution.config.service.v2.GetUserAttributionEdgeDecisionRulesRequest;
import ai.traceable.userattribution.config.service.v2.GetUserAttributionRulesRequest;
import ai.traceable.userattribution.config.service.v2.GetUserAttributionRulesRequest.GetUserAttributionRulesFilter;
import ai.traceable.userattribution.config.service.v2.UserAttributionConfigServiceGrpc.UserAttributionConfigServiceBlockingStub;
import com.google.inject.Inject;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = @Inject)
public class UserAttributionEdgeDecisionConfigSupplier implements EdgeDecisionEngineConfigSupplier {

  private static final GetUserAttributionEdgeDecisionRulesRequest
      USER_ATTRIBUTION_EDGE_RULES_REQUEST =
          GetUserAttributionEdgeDecisionRulesRequest.newBuilder()
              .setFilter(GetUserAttributionRulesFilter.newBuilder().setDisabled(false))
              .setRuleSource(
                  GetUserAttributionRulesRequest.UserAttributionRuleSource
                      .USER_ATTRIBUTION_RULE_SOURCE_UNSPECIFIED)
              .build();

  private final UserAttributionConfigServiceBlockingStub stub;

  @Override
  public String getName() {
    return UserAttributionEdgeDecisionConfigSupplier.class.getSimpleName();
  }

  @Override
  public EdgeDecisionEngineConfig get(
      RequestContext requestContext, GetEdgeDecisionConfigsFilter filter) {
    if (filter != null) {
      List<EdgeInputKind> inputKinds = filter.getEdgeInputKindsList();
      if (!inputKinds.isEmpty()
          && !inputKinds.contains(EdgeInputKind.EDGE_INPUT_KIND_HTTP_REQUEST)) {
        return EdgeDecisionEngineConfig.getDefaultInstance();
      }
    }

    try {
      return requestContext.call(
          () ->
              stub.getUserAttributionEdgeDecisionRules(USER_ATTRIBUTION_EDGE_RULES_REQUEST)
                  .getEdgeDecisionEngineConfig());
    } catch (Exception e) {
      log.error("Error in fetching user attribution edge decision rules", e);
      return EdgeDecisionEngineConfig.getDefaultInstance();
    }
  }
}
