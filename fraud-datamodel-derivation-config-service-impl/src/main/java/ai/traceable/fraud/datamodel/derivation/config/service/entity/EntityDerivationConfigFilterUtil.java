package ai.traceable.fraud.datamodel.derivation.config.service.entity;

import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfig;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfigData;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfigFilter;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationScopeFilter;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityScope;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityType;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EventDerivationConfigDetails;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.GetEntityDerivationConfigsRequest;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.Scope;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.SpanProjection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class EntityDerivationConfigFilterUtil {

  private EntityDerivationConfigFilterUtil() {}

  public static boolean applyEntityFilter(
      EntityDerivationConfig config, GetEntityDerivationConfigsRequest request) {
    return filterByIds(config, request)
        && filterByDisabled(config, request.getFilter())
        && filterByInternal(config, request.getFilter());
  }

  public static boolean applyScopeFilter(
      EntityDerivationConfig config,
      EntityDerivationScopeFilter scopeFilter,
      Map<String, EntityDerivationConfig> allConfigsById) {
    return applyScopeFilterInternal(
        config, scopeFilter, allConfigsById, new HashSet<>(), new HashMap<>());
  }

  private static boolean applyScopeFilterInternal(
      EntityDerivationConfig config,
      EntityDerivationScopeFilter scopeFilter,
      Map<String, EntityDerivationConfig> allConfigsById,
      Set<String> visiting,
      Map<String, Boolean> memo) {
    if (config == null || !config.hasData()) {
      return true;
    }

    String configId = config.getId();
    if (memo.containsKey(configId)) {
      return memo.get(configId);
    }
    if (visiting.contains(configId)) {
      return true; // Cycle detected, safe default
    }

    EntityDerivationConfigData data = config.getData();
    boolean result;

    switch (data.getValueSourceCase()) {
      case PREPOPULATED_SPAN_ATTRIBUTE:
        result = true;
        break;

      case PARENT_DERIVATION:
        visiting.add(configId);
        String parentId = data.getParentDerivation().getParentEntityDerivationId();
        EntityDerivationConfig parentConfig = allConfigsById.get(parentId);
        result =
            parentConfig == null
                || applyScopeFilterInternal(
                    parentConfig, scopeFilter, allConfigsById, visiting, memo);
        visiting.remove(configId);
        break;

      case SPAN_PROJECTION:
        result = checkSpanProjectionScopeOverlap(data.getSpanProjection(), scopeFilter);
        break;

      case VALUESOURCE_NOT_SET:
      default:
        result = true;
    }

    memo.put(configId, result);
    return result;
  }

  private static boolean checkSpanProjectionScopeOverlap(
      SpanProjection spanProjection, EntityDerivationScopeFilter scopeFilter) {
    List<EventDerivationConfigDetails> derivationConfigs =
        spanProjection.getEventDerivationConfigsList();

    if (derivationConfigs.isEmpty()) {
      return false;
    }

    for (EventDerivationConfigDetails details : derivationConfigs) {
      if (scopeOverlaps(details.getScope(), scopeFilter)) {
        return true;
      }
    }
    return false;
  }

  private static boolean scopeOverlaps(Scope ruleScope, EntityDerivationScopeFilter filter) {
    return environmentScopeOverlaps(ruleScope, filter) && apiScopeOverlaps(ruleScope, filter);
  }

  private static boolean environmentScopeOverlaps(
      Scope ruleScope, EntityDerivationScopeFilter filter) {
    List<String> filterEnvIds = filter.getEnvironmentIdsList();
    if (filterEnvIds.isEmpty()) {
      return true;
    }

    if (!ruleScope.hasEnvironmentScope()) {
      return true;
    }

    List<String> ruleEnvs = ruleScope.getEnvironmentScope().getEnvironmentsList();
    if (ruleEnvs.isEmpty()) {
      return true;
    }

    return !Collections.disjoint(filterEnvIds, ruleEnvs);
  }

  private static boolean apiScopeOverlaps(Scope ruleScope, EntityDerivationScopeFilter filter) {
    List<String> filterApiIds = filter.getApiIdsList();
    if (filterApiIds.isEmpty()) {
      return true;
    }

    if (!ruleScope.hasEntityScope()) {
      return true;
    }

    EntityScope entityScope = ruleScope.getEntityScope();
    if (entityScope.getEntityType() == EntityType.ENTITY_TYPE_SERVICE) {
      return true;
    }

    List<String> ruleEntityIds = entityScope.getEntityIdsList();
    if (ruleEntityIds.isEmpty()) {
      return true;
    }

    return !Collections.disjoint(filterApiIds, ruleEntityIds);
  }

  private static boolean filterByIds(
      EntityDerivationConfig config, GetEntityDerivationConfigsRequest request) {
    if (request.getIdsCount() == 0) {
      return true;
    }
    return request.getIdsList().contains(config.getId());
  }

  private static boolean filterByDisabled(
      EntityDerivationConfig config, EntityDerivationConfigFilter filter) {
    if (!filter.hasIncludeDisabled() || !filter.getIncludeDisabled()) {
      return !config.getData().getDisabled();
    }
    return true;
  }

  private static boolean filterByInternal(
      EntityDerivationConfig config, EntityDerivationConfigFilter filter) {
    if (!filter.hasIncludeInternal() || !filter.getIncludeInternal()) {
      return !config.getData().getInternal();
    }
    return true;
  }
}
