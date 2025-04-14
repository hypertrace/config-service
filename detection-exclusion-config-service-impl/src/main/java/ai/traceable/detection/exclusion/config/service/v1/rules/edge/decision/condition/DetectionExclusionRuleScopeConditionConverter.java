package ai.traceable.detection.exclusion.config.service.v1.rules.edge.decision.condition;

import static ai.traceable.detection.exclusion.config.service.v1.EntityType.ENTITY_TYPE_API;
import static ai.traceable.detection.exclusion.config.service.v1.LabelType.LABEL_TYPE_API;
import static ai.traceable.edge.decision.converter.utils.ConverterUtils.PATH_ATTRIBUTE;
import static ai.traceable.edge.decision.converter.utils.ConverterUtils.SERVICE_ATTRIBUTE;

import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionCondition;
import ai.traceable.detection.exclusion.config.service.v1.EntityScope;
import ai.traceable.detection.exclusion.config.service.v1.LabelScope;
import ai.traceable.detection.exclusion.config.service.v1.ScopeCondition;
import ai.traceable.edge.decision.converter.utils.ConverterUtils;
import ai.traceable.entity.fetcher.cache.CachedApiMappingProvider;
import ai.traceable.entity.fetcher.cache.CachedServiceMappingProvider;
import ai.traceable.entity.fetcher.cache.CachedServiceMappingProvider.ServiceIdentifierEntity;
import com.google.inject.Inject;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = @Inject)
class DetectionExclusionRuleScopeConditionConverter
    implements DetectionExclusionRuleConditionConverter {

  private final CachedServiceMappingProvider cachedServiceMappingProvider;
  private final CachedApiMappingProvider cachedApiMappingProvider;

  @Override
  public MatchCondition buildMatchCondition(
      RequestContext requestContext, DetectionExclusionCondition condition) {
    final ScopeCondition scopeCondition = condition.getScopeCondition();
    switch (scopeCondition.getScopeCase()) {
      case URL_SCOPE:
        return buildPathMatchCondition(
            scopeCondition.getUrlScope().getUrlRegexesList(), scopeCondition.getExclude());
      case ENTITY_SCOPE:
        EntityScope entityScope = scopeCondition.getEntityScope();
        switch (entityScope.getEntityType()) {
          case ENTITY_TYPE_API:
            Map<String, Optional<CachedApiMappingProvider.ApiIdentifierEntity>>
                apiIdentifierEntities =
                    cachedApiMappingProvider.getApiIdentifierEntities(
                        requestContext, Set.copyOf(entityScope.getEntityIdsList()));
            List<String> urlRegexes =
                apiIdentifierEntities.values().stream()
                    .flatMap(Optional::stream)
                    .flatMap(
                        apiIdentifierEntity ->
                            apiIdentifierEntity.getResolvedUrlPatterns().stream())
                    .distinct()
                    .collect(Collectors.toUnmodifiableList());
            return buildPathMatchCondition(urlRegexes, scopeCondition.getExclude());
          case ENTITY_TYPE_SERVICE:
            Map<String, Optional<ServiceIdentifierEntity>> serviceIdentifierEntities =
                cachedServiceMappingProvider.getServiceIdentifierEntities(
                    requestContext, Set.copyOf(entityScope.getEntityIdsList()));
            List<String> serviceNames =
                serviceIdentifierEntities.values().stream()
                    .flatMap(Optional::stream)
                    .map(ServiceIdentifierEntity::getServiceName)
                    .distinct()
                    .collect(Collectors.toUnmodifiableList());
            return ConverterUtils.buildInOperatorMatchCondition(SERVICE_ATTRIBUTE, serviceNames)
                .setNegate(scopeCondition.getExclude())
                .build();
          default:
            throw new IllegalArgumentException(
                "Unsupported entity type case: " + entityScope.getEntityType());
        }
      case LABEL_SCOPE:
        LabelScope labelScope = scopeCondition.getLabelScope();
        if (!labelScope.getLabelType().equals(LABEL_TYPE_API)) {
          throw new IllegalArgumentException(
              "Unsupported label type: " + labelScope.getLabelType());
        }
        Map<String, Set<CachedApiMappingProvider.ApiIdentifierEntity>> apiIdentifierEntitiesMap =
            cachedApiMappingProvider.getApiIdentifierEntitiesHavingLabels(
                requestContext, Set.copyOf(labelScope.getLabelIdsList()));
        List<String> urlRegexes =
            apiIdentifierEntitiesMap.values().stream()
                .flatMap(Collection::stream)
                .distinct()
                .flatMap(
                    apiIdentifierEntity -> apiIdentifierEntity.getResolvedUrlPatterns().stream())
                .distinct()
                .collect(Collectors.toUnmodifiableList());
        return buildPathMatchCondition(urlRegexes, scopeCondition.getExclude());
      default:
        throw new IllegalArgumentException(
            "Unsupported scope case: " + scopeCondition.getScopeCase());
    }
  }

  @Override
  public DetectionExclusionCondition.ConditionCase getConditionCase() {
    return DetectionExclusionCondition.ConditionCase.SCOPE_CONDITION;
  }

  private MatchCondition buildPathMatchCondition(List<String> urlRegexes, boolean exclude) {
    return ConverterUtils.buildLikeOperatorMatchCondition(PATH_ATTRIBUTE, urlRegexes)
        .setNegate(exclude)
        .build();
  }
}
