package ai.traceable.edge.decision.config.service.supplier;

import static ai.traceable.edge.decision.config.service.supplier.EdgeDecisionEngineConfigMergeUtil.merge;
import static ai.traceable.edge.decision.config.service.validation.RequestValidator.validateRequestContext;

import ai.traceable.edge.decision.config.service.store.EdgeDecisionConfigStoreManager;
import ai.traceable.edge.decision.config.service.store.EdgeDecisionRuleStoreManager;
import ai.traceable.edge.decision.config.service.store.EdgeDecisionSpecStoreManager;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRule;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionSpec;
import ai.traceable.edge.decision.config.service.v1.GetAllEdgeDecisionRulesRequest;
import ai.traceable.edge.decision.config.service.v1.GetAllEdgeDecisionRulesResponse;
import ai.traceable.edge.decision.config.service.v1.GetAllEdgeDecisionSpecsRequest;
import ai.traceable.edge.decision.config.service.v1.GetAllEdgeDecisionSpecsResponse;
import ai.traceable.edge.decision.config.service.v1.GetEdgeDecisionEngineConfigRequest;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;

/**
 * This supplier is responsible for providing the runtime config based on all the rules, specs,
 * common variables in a tenant.
 */
public class StoredEdgeDecisionEngineConfigSupplier implements EdgeDecisionEngineConfigSupplier {
  private final EdgeDecisionConfigStoreManager configStoreManager;
  private final EdgeDecisionRuleStoreManager ruleStoreManager;
  private final EdgeDecisionSpecStoreManager specStoreManager;

  @Inject
  public StoredEdgeDecisionEngineConfigSupplier(
      EdgeDecisionConfigStoreManager configStoreManager,
      EdgeDecisionRuleStoreManager ruleStoreManager,
      EdgeDecisionSpecStoreManager specStoreManager) {
    this.configStoreManager = configStoreManager;
    this.ruleStoreManager = ruleStoreManager;
    this.specStoreManager = specStoreManager;
  }

  @Override
  public String getName() {
    return StoredEdgeDecisionEngineConfigSupplier.class.getName();
  }

  @Override
  public EdgeDecisionEngineConfig get(RequestContext requestContext) {
    validateRequestContext(requestContext);
    return mergeStoredConfigAndStoredRules(
        requestContext,
        GetEdgeDecisionEngineConfigRequest.newBuilder()
            .setId(requestContext.getTenantId().get())
            .build());
  }

  private EdgeDecisionEngineConfig getStoredEdgeDecisionEngineConfig(
      RequestContext requestContext, GetEdgeDecisionEngineConfigRequest request) {
    // get stored config. by default, get the config that's stored with the tenant id as its id.
    Optional<String> tenantIdHolder = requestContext.getTenantId();
    return tenantIdHolder
        .map(
            s ->
                requestContext
                    .call(() -> configStoreManager.get(requestContext, request))
                    .getEdgeDecisionEngineConfig())
        .orElseGet(EdgeDecisionEngineConfig::getDefaultInstance);
  }

  private List<EdgeDecisionRule> getStoredRules(RequestContext requestContext) {
    GetAllEdgeDecisionRulesResponse response =
        requestContext.call(
            () ->
                ruleStoreManager.getAll(
                    requestContext, GetAllEdgeDecisionRulesRequest.getDefaultInstance()));
    return response.getEdgeDecisionRulesList();
  }

  private List<EdgeDecisionSpec> getStoredSpecs(RequestContext requestContext) {
    GetAllEdgeDecisionSpecsResponse response =
        requestContext.call(
            () ->
                specStoreManager.getAll(
                    requestContext, GetAllEdgeDecisionSpecsRequest.getDefaultInstance()));
    return response.getEdgeDecisionSpecsList();
  }

  public EdgeDecisionEngineConfig mergeStoredConfigAndStoredRules(
      RequestContext requestContext, GetEdgeDecisionEngineConfigRequest request) {
    validateRequestContext(requestContext);
    Optional<String> tenantIdHolder = requestContext.getTenantId();
    EdgeDecisionEngineConfig decisionEngineConfig =
        getStoredEdgeDecisionEngineConfig(requestContext, request);
    List<EdgeDecisionRule> decisionRules = getStoredRules(requestContext);
    var decisionSpecs = getStoredSpecs(requestContext);
    List<EdgeDecisionRule> mergedRules =
        merge(decisionEngineConfig.getDecisionRulesList(), decisionRules, EdgeDecisionRule::getId);
    List<EdgeDecisionSpec> mergedSpecs =
        merge(decisionEngineConfig.getDecisionSpecsList(), decisionSpecs, EdgeDecisionSpec::getId);
    return decisionEngineConfig.toBuilder()
        .setId(tenantIdHolder.get())
        .clearDecisionRules()
        .clearDecisionSpecs()
        .addAllDecisionRules(mergedRules)
        .addAllDecisionSpecs(mergedSpecs)
        .build();
  }
}
