package ai.traceable.edge.decision.config.service.supplier.abusepolicy;

import ai.traceable.edge.decision.config.service.supplier.EdgeDecisionEngineConfigSupplier;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.edge.decision.config.service.v1.EdgeInputKind;
import ai.traceable.edge.decision.config.service.v1.GetEdgeDecisionConfigsFilter;
import ai.traceable.fraud.policy.config.service.v1.FraudPolicyConfigServiceGrpc.FraudPolicyConfigServiceBlockingStub;
import ai.traceable.fraud.policy.config.service.v1.GetAbusePolicyEdgeDecisionRulesRequest;
import com.google.inject.Inject;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class AbusePolicyEdgeDecisionEngineConfigSupplier
    implements EdgeDecisionEngineConfigSupplier {

  private static final String NAME = "abuse-policy";
  private static final GetAbusePolicyEdgeDecisionRulesRequest
      ABUSE_POLICY_EDGE_DECISION_RULES_REQUEST =
          GetAbusePolicyEdgeDecisionRulesRequest.getDefaultInstance();

  private final FraudPolicyConfigServiceBlockingStub stub;

  @Inject
  public AbusePolicyEdgeDecisionEngineConfigSupplier(FraudPolicyConfigServiceBlockingStub stub) {
    this.stub = stub;
  }

  @Override
  public String getName() {
    return NAME;
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
    Optional<String> tenantIdHolder = requestContext.getTenantId();
    return tenantIdHolder
        .map(
            s -> {
              try {
                return requestContext
                    .call(
                        () ->
                            stub.getAbusePolicyEdgeDecisionRules(
                                ABUSE_POLICY_EDGE_DECISION_RULES_REQUEST))
                    .getEdgeDecisionEngineConfig();
              } catch (Exception e) {
                log.error("Error in fetching abuse policy rules for edge decision config", e);
                return EdgeDecisionEngineConfig.getDefaultInstance();
              }
            })
        .orElseGet(EdgeDecisionEngineConfig::getDefaultInstance);
  }
}
