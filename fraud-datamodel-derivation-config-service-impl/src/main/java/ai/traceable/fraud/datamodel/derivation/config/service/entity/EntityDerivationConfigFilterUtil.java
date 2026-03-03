package ai.traceable.fraud.datamodel.derivation.config.service.entity;

import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfig;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfigFilter;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.GetEntityDerivationConfigsRequest;

public class EntityDerivationConfigFilterUtil {

  private EntityDerivationConfigFilterUtil() {}

  public static boolean applyEntityFilter(
      EntityDerivationConfig config, GetEntityDerivationConfigsRequest request) {
    return filterByIds(config, request)
        && filterByDisabled(config, request.getFilter())
        && filterByInternal(config, request.getFilter());
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
