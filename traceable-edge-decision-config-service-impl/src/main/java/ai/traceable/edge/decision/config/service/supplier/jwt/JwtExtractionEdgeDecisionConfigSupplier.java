package ai.traceable.edge.decision.config.service.supplier.jwt;

import ai.traceable.edge.decision.config.service.supplier.EdgeDecisionEngineConfigSupplier;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.edge.decision.config.service.v1.EdgeInputKind;
import ai.traceable.edge.decision.config.service.v1.GetEdgeDecisionConfigsFilter;
import ai.traceable.jwt.extraction.config.service.v1.GetJwtExtractionEdgeDecisionRulesRequest;
import ai.traceable.jwt.extraction.config.service.v1.JwtExtractionConfigServiceGrpc.JwtExtractionConfigServiceBlockingStub;
import ai.traceable.jwt.extraction.config.service.v1.JwtExtractionRuleFilter;
import com.google.inject.Inject;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = @Inject)
public class JwtExtractionEdgeDecisionConfigSupplier implements EdgeDecisionEngineConfigSupplier {

  private static final JwtExtractionRuleFilter JWT_EXTRACTION_RULE_FILTER =
      JwtExtractionRuleFilter.newBuilder().setDisabled(false).build();

  private final JwtExtractionConfigServiceBlockingStub stub;

  @Override
  public String getName() {
    return JwtExtractionEdgeDecisionConfigSupplier.class.getSimpleName();
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
              stub.getJwtExtractionEdgeDecisionRules(
                      GetJwtExtractionEdgeDecisionRulesRequest.newBuilder()
                          .setFilter(JWT_EXTRACTION_RULE_FILTER)
                          .build())
                  .getEdgeDecisionEngineConfig());
    } catch (Exception e) {
      log.error("Error in fetching jwt extraction rules for edge decision config", e);
      return EdgeDecisionEngineConfig.getDefaultInstance();
    }
  }
}
