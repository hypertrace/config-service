package ai.traceable.edge.decision.config.service.supplier.customsignature;

import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureEdgeDecisionRulesRequest;
import ai.traceable.customsignature.config.service.v1.GetRulesFilter;
import ai.traceable.edge.decision.config.service.supplier.EdgeDecisionEngineConfigSupplier;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.edge.decision.config.service.v1.EdgeInputKind;
import ai.traceable.edge.decision.config.service.v1.GetEdgeDecisionConfigsFilter;
import com.google.inject.Inject;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = @Inject)
public class CustomSignatureEdgeDecisionConfigSupplier implements EdgeDecisionEngineConfigSupplier {

  private static final GetRulesFilter CUSTOM_SIGNATURE_RULES_FILTER =
      GetRulesFilter.newBuilder().setDisabled(false).build();

  private final CustomSignatureConfigServiceBlockingStub stub;

  @Override
  public String getName() {
    return CustomSignatureEdgeDecisionConfigSupplier.class.getSimpleName();
  }

  @Override
  public EdgeDecisionEngineConfig get(
      RequestContext requestContext, GetEdgeDecisionConfigsFilter filter) {
    if (filter != null) {
      List<EdgeInputKind> inputKinds = filter.getEdgeInputKindsList();
      if (!inputKinds.isEmpty()
          && !inputKinds.contains(EdgeInputKind.EDGE_INPUT_KIND_HTTP_REQUEST)) {
        // currently, this supplier doesn't provide any other input kind.
        return EdgeDecisionEngineConfig.getDefaultInstance();
      }
    }
    try {
      return requestContext.call(
          () ->
              stub.getCustomSignatureEdgeDecisionRules(
                      GetCustomSignatureEdgeDecisionRulesRequest.newBuilder()
                          .setRulesFilter(CUSTOM_SIGNATURE_RULES_FILTER)
                          .build())
                  .getEdgeDecisionEngineConfig());
    } catch (Exception e) {
      log.error("Error in fetching custom signature rules for edge decision config", e);
      return EdgeDecisionEngineConfig.getDefaultInstance();
    }
  }
}
