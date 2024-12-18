package ai.traceable.edge.decision.config.service.supplier;

import static ai.traceable.edge.decision.config.service.supplier.EdgeDecisionEngineConfigMergeUtil.merge;

import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.edge.decision.config.service.v1.GetResolvedEdgeDecisionEngineConfigsRequest;
import java.util.Set;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

/**
 * This supplier is responsible for providing the runtime config based on all the rules, specs,
 * common variables in a tenant.
 */
@Slf4j
public class EdgeDecisionEngineConfigResolver {
  private final Set<EdgeDecisionEngineConfigSupplier> configSuppliers;

  @Inject
  public EdgeDecisionEngineConfigResolver(Set<EdgeDecisionEngineConfigSupplier> configSuppliers) {
    this.configSuppliers = configSuppliers;
  }

  // todo: further enhance to process the filters provided in the request.
  public EdgeDecisionEngineConfig getResolvedEdgeDecisionEngineConfig(
      RequestContext requestContext, GetResolvedEdgeDecisionEngineConfigsRequest request) {
    EdgeDecisionEngineConfig finalConfig = null;
    for (EdgeDecisionEngineConfigSupplier configSupplier : configSuppliers) {
      if (configSupplier != null) {
        String supplierName = configSupplier.getName();
        try {
          EdgeDecisionEngineConfig config = configSupplier.get(requestContext);
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
    return finalConfig;
  }
}
