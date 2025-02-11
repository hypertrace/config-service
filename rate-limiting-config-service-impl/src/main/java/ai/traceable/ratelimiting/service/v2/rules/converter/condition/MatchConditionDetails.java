package ai.traceable.ratelimiting.service.v2.rules.converter.condition;

import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.VariableDerivationMapping;
import ai.traceable.entity.fetcher.cache.CachedApiMappingProvider.ApiIdentifierEntity;
import java.util.List;

@lombok.Value
public class MatchConditionDetails {
  MatchCondition matchCondition;
  List<VariableDerivationMapping> variableDerivationMappings;
  List<ApiIdentifierEntity> apiIdentifierEntities;
}
