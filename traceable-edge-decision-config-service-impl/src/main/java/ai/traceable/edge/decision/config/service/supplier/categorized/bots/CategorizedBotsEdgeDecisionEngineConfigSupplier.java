package ai.traceable.edge.decision.config.service.supplier.categorized.bots;

import ai.traceable.bot.categorized.policy.service.v1.CategorizedBotAction;
import ai.traceable.bot.categorized.policy.service.v1.CategorizedBotConfigPolicyEdgeDecisionRulesFilter;
import ai.traceable.bot.categorized.policy.service.v1.CategorizedBotConfigPolicyServiceGrpc.CategorizedBotConfigPolicyServiceBlockingStub;
import ai.traceable.bot.categorized.policy.service.v1.GetCategorizedBotConfigPolicyEdgeDecisionRulesRequest;
import ai.traceable.edge.decision.config.service.supplier.EdgeDecisionEngineConfigSupplier;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.edge.decision.config.service.v1.EdgeInputKind;
import ai.traceable.edge.decision.config.service.v1.GetEdgeDecisionConfigsFilter;
import com.google.inject.Inject;
import java.util.List;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@RequiredArgsConstructor(onConstructor_ = @Inject)
public class CategorizedBotsEdgeDecisionEngineConfigSupplier
    implements EdgeDecisionEngineConfigSupplier {

  private final CategorizedBotConfigPolicyServiceBlockingStub
      categorizedBotConfigPolicyServiceBlockingStub;

  @Override
  public String getName() {
    return CategorizedBotsEdgeDecisionEngineConfigSupplier.class.getSimpleName();
  }

  @Override
  public EdgeDecisionEngineConfig get(
      final RequestContext requestContext, final GetEdgeDecisionConfigsFilter filter) {
    if (filter != null) {
      List<EdgeInputKind> inputKinds = filter.getEdgeInputKindsList();
      if (!inputKinds.isEmpty()
          && !inputKinds.contains(EdgeInputKind.EDGE_INPUT_KIND_HTTP_REQUEST)) {
        // currently, this supplier doesn't provide any other input kind.
        return EdgeDecisionEngineConfig.getDefaultInstance();
      }
    }
    Optional<String> tenantIdHolder = requestContext.getTenantId();
    return tenantIdHolder
        .map(
            tenantId ->
                requestContext
                    .call(
                        () ->
                            categorizedBotConfigPolicyServiceBlockingStub
                                .getCategorizedBotConfigPolicyEdgeDecisionRules(
                                    GetCategorizedBotConfigPolicyEdgeDecisionRulesRequest
                                        .newBuilder()
                                        .setCategorizedBotConfigPolicyEdgeDecisionRulesFilter(
                                            CategorizedBotConfigPolicyEdgeDecisionRulesFilter
                                                .newBuilder()
                                                .addAllowedActions(
                                                    CategorizedBotAction
                                                        .CATEGORIZED_BOT_ACTION_BLOCK)
                                                .addAllowedActions(
                                                    CategorizedBotAction
                                                        .CATEGORIZED_BOT_ACTION_ALLOW))
                                        .build()))
                    .getEdgeDecisionEngineConfig())
        .orElseGet(EdgeDecisionEngineConfig::getDefaultInstance);
  }
}
