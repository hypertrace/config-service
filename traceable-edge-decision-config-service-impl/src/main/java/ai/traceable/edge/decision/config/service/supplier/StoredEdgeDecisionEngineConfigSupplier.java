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
import ai.traceable.edge.decision.config.service.v1.GetEdgeDecisionConfigsFilter;
import ai.traceable.edge.decision.config.service.v1.GetEdgeDecisionEngineConfigRequest;
import com.google.protobuf.Timestamp;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
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
  public EdgeDecisionEngineConfig get(
      RequestContext requestContext, GetEdgeDecisionConfigsFilter filter) {
    validateRequestContext(requestContext);
    return mergeStoredConfigAndStoredRules(
        requestContext,
        GetEdgeDecisionEngineConfigRequest.newBuilder()
            .setId(requestContext.getTenantId().get())
            .build(),
        filter);
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

  public List<EdgeDecisionRule> getStoredRules(RequestContext requestContext) {
    GetAllEdgeDecisionRulesResponse response =
        requestContext.call(
            () ->
                ruleStoreManager.getAll(
                    requestContext, GetAllEdgeDecisionRulesRequest.getDefaultInstance()));
    return response.getEdgeDecisionRulesList().stream()
        .filter(this::checkExpiration)
        .collect(Collectors.toUnmodifiableList());
  }

  private Boolean checkExpiration(EdgeDecisionRule rule) {
    if (!rule.hasRuleStatus()) return true;
    if (rule.getRuleStatus().getDisabled()) return false;
    if (!rule.getRuleStatus().hasTtl()) return true;
    if (!rule.getRuleStatus().getTtl().hasExpiresAt()) return true;

    Timestamp expiresAt = rule.getRuleStatus().getTtl().getExpiresAt();
    Instant expiryTime = Instant.ofEpochSecond(expiresAt.getSeconds(), expiresAt.getNanos());
    return expiryTime.isAfter(Instant.now());
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
      RequestContext requestContext,
      GetEdgeDecisionEngineConfigRequest request,
      GetEdgeDecisionConfigsFilter filter) {
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
    // apply filters.
    if (!isEmpty(filter)) {
      mergedRules =
          mergedRules.stream()
              .filter(rule -> applyFilter(rule, filter))
              .collect(Collectors.toList());
      mergedSpecs =
          mergedSpecs.stream()
              .filter(spec -> applyFilter(spec, filter))
              .collect(Collectors.toList());
    }
    return decisionEngineConfig.toBuilder()
        .setId(tenantIdHolder.get())
        .clearDecisionRules()
        .clearDecisionSpecs()
        .addAllDecisionRules(mergedRules)
        .addAllDecisionSpecs(mergedSpecs)
        .build();
  }

  private boolean isEmpty(GetEdgeDecisionConfigsFilter filter) {
    return filter == null
        || (filter.getEdgeInputKindsCount() == 0
            && filter.getRuleCategoriesCount() == 0
            && filter.getDecisionTypesCount() == 0);
  }

  private boolean applyFilter(EdgeDecisionRule rule, GetEdgeDecisionConfigsFilter filter) {
    var inputKinds = filter.getEdgeInputKindsList();
    var decisionTypes = filter.getDecisionTypesList();
    var ruleCategories = filter.getRuleCategoriesList();
    if (!inputKinds.isEmpty()) {
      if (!inputKinds.contains(rule.getRuleDefinition().getEdgeInputKind())) {
        return false;
      }
    }
    if (!decisionTypes.isEmpty()) {
      if (!decisionTypes.contains(rule.getRuleDecision().getEdgeDecisionType())) {
        return false;
      }
    }
    return ruleCategories.isEmpty() || ruleCategories.contains(rule.getRuleCategory());
  }

  private boolean applyFilter(EdgeDecisionSpec spec, GetEdgeDecisionConfigsFilter filter) {
    var inputKinds = filter.getEdgeInputKindsList();
    return inputKinds.isEmpty() || inputKinds.contains(spec.getEdgeInputKind());
  }
}
