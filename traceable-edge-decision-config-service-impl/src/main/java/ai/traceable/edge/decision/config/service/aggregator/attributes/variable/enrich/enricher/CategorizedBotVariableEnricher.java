package ai.traceable.edge.decision.config.service.aggregator.attributes.variable.enrich.enricher;

import ai.traceable.bot.categorized.config.service.v1.CategorizedBotConfigServiceGrpc.CategorizedBotConfigServiceBlockingStub;
import ai.traceable.bot.categorized.config.service.v1.GetCategorizedBotConfigEdgeDecisionVariablesRequest;
import ai.traceable.datamodel.data.transformation.config.v1.VariableDerivationMapping;
import ai.traceable.edge.decision.config.service.aggregator.attributes.variable.enrich.CheckAndAddVariableToRule;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import com.google.inject.Inject;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@RequiredArgsConstructor(onConstructor_ = @Inject)
public class CategorizedBotVariableEnricher implements VariableEnricher {

  private final CheckAndAddVariableToRule variableChecker;
  private final CategorizedBotConfigServiceBlockingStub categorizedBotConfigServiceBlockingStub;

  @Override
  public EdgeDecisionEngineConfig enrichRule(
      RequestContext requestContext, EdgeDecisionEngineConfig edgeDecisionEngineConfig) {
    final List<VariableDerivationMapping> variableDerivationMappings =
        requestContext
            .getTenantId()
            .map(
                tenantId ->
                    requestContext
                        .call(
                            () ->
                                categorizedBotConfigServiceBlockingStub
                                    .getCategorizedBotConfigEdgeDecisionVariables(
                                        GetCategorizedBotConfigEdgeDecisionVariablesRequest
                                            .getDefaultInstance()))
                        .getVariableDerivationMappingsList())
            .orElse(List.of());
    for (var variableDerivationMapping : variableDerivationMappings) {
      edgeDecisionEngineConfig =
          variableChecker.checkAndAddVariableToRule(
              edgeDecisionEngineConfig,
              variableDerivationMapping.getName(),
              variableDerivationMapping);
    }
    return edgeDecisionEngineConfig;
  }
}
