package ai.traceable.edge.decision.config.service.supplier;

import static ai.traceable.edge.decision.config.service.supplier.EdgeDecisionEngineConfigMergeUtil.merge;

import ai.traceable.edge.decision.config.service.aggregator.attributes.RuleVariableEnricher;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.edge.decision.config.service.v1.GetResolvedEdgeDecisionEngineConfigsRequest;
import jakarta.inject.Inject;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

/**
 * This supplier is responsible for providing the runtime config based on all the rules, specs,
 * common variables in a tenant.
 */
@Slf4j
public class EdgeDecisionEngineConfigResolver {
  private final Set<EdgeDecisionEngineConfigSupplier> configSuppliers;
  private final RuleVariableEnricher ruleVariableEnricher;

  @Inject
  public EdgeDecisionEngineConfigResolver(
      Set<EdgeDecisionEngineConfigSupplier> configSuppliers,
      RuleVariableEnricher ruleVariableEnricher) {
    this.configSuppliers = configSuppliers;
    this.ruleVariableEnricher = ruleVariableEnricher;
  }

  public EdgeDecisionEngineConfig getResolvedEdgeDecisionEngineConfig(
      RequestContext requestContext, GetResolvedEdgeDecisionEngineConfigsRequest request) {
    EdgeDecisionEngineConfig finalConfig = null;
    for (EdgeDecisionEngineConfigSupplier configSupplier : configSuppliers) {
      if (configSupplier != null) {
        String supplierName = configSupplier.getName();
        try {
          EdgeDecisionEngineConfig config = configSupplier.get(requestContext, request.getFilter());
          if (finalConfig == null) {
            finalConfig = config;
          } else {
            finalConfig = merge(finalConfig, config);
          }
        } catch (Exception e) {
          log.error("Error occurred when trying to merge config from supplier={}", supplierName);
        }
      }
    }
    if (finalConfig == null) {
      finalConfig = EdgeDecisionEngineConfig.getDefaultInstance();
    }
    // Check if we need to add variable definition of any missing variables
    finalConfig = ruleVariableEnricher.enrichRule(requestContext, finalConfig);
    return finalConfig;
  }
}
