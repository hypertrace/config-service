package ai.traceable.customsignature.config.service.rules.converter.expression;

import static ai.traceable.edge.decision.converter.utils.ConverterUtils.PATH_ATTRIBUTE;
import static ai.traceable.edge.decision.converter.utils.ConverterUtils.SERVICE_ATTRIBUTE;

import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.Clause.ClauseCase;
import ai.traceable.customsignature.config.service.v1.ScopeExpression;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.edge.decision.converter.utils.ConverterUtils;
import ai.traceable.entity.fetcher.cache.CachedApiMappingProvider;
import ai.traceable.entity.fetcher.cache.CachedServiceMappingProvider;
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
public class ScopeExpressionConverter implements CustomSignatureExpressionConverter {

  private final CachedServiceMappingProvider cachedServiceMappingProvider;
  private final CachedApiMappingProvider cachedApiMappingProvider;

  @Override
  public MatchCondition buildMatchCondition(Clause clause) {
    ScopeExpression scopeExpression = clause.getScopeExpression();

    switch (scopeExpression.getScopeCase()) {
      case ENTITY_SCOPE:
        ScopeExpression.EntityScope entityScope = scopeExpression.getEntityScope();
        switch (entityScope.getEntityType()) {
          case ENTITY_TYPE_API:
            Map<String, Optional<CachedApiMappingProvider.ApiIdentifierEntity>>
                apiIdentifierEntities =
                    cachedApiMappingProvider.getApiIdentifierEntities(
                        RequestContext.CURRENT.get(), Set.copyOf(entityScope.getEntityIdsList()));
            List<String> urlRegexes =
                apiIdentifierEntities.values().stream()
                    .flatMap(Optional::stream)
                    .flatMap(
                        apiIdentifierEntity ->
                            apiIdentifierEntity.getResolvedUrlPatterns().stream())
                    .distinct()
                    .collect(Collectors.toUnmodifiableList());
            return buildPathMatchCondition(urlRegexes, scopeExpression.getExclude());

          case ENTITY_TYPE_SERVICE:
            Map<String, Optional<CachedServiceMappingProvider.ServiceIdentifierEntity>>
                serviceIdentifierEntities =
                    cachedServiceMappingProvider.getServiceIdentifierEntities(
                        RequestContext.CURRENT.get(), Set.copyOf(entityScope.getEntityIdsList()));
            List<String> serviceNames =
                serviceIdentifierEntities.values().stream()
                    .flatMap(Optional::stream)
                    .map(CachedServiceMappingProvider.ServiceIdentifierEntity::getServiceName)
                    .distinct()
                    .collect(Collectors.toUnmodifiableList());
            return ConverterUtils.buildInOperatorMatchCondition(SERVICE_ATTRIBUTE, serviceNames)
                .setNegate(scopeExpression.getExclude())
                .build();

          default:
            throw new IllegalArgumentException(
                "Unsupported entity type case: " + entityScope.getEntityType());
        }

      case LABEL_SCOPE:
        ScopeExpression.LabelScope labelScope = scopeExpression.getLabelScope();
        if (!labelScope.getLabelType().equals(ScopeExpression.LabelType.LABEL_TYPE_API)) {
          throw new IllegalArgumentException(
              "Unsupported label type: " + labelScope.getLabelType());
        }
        Map<String, Set<CachedApiMappingProvider.ApiIdentifierEntity>> apiIdentifierEntitiesMap =
            cachedApiMappingProvider.getApiIdentifierEntitiesHavingLabels(
                RequestContext.CURRENT.get(), Set.copyOf(labelScope.getLabelIdsList()));
        List<String> urlRegexes =
            apiIdentifierEntitiesMap.values().stream()
                .flatMap(Collection::stream)
                .distinct()
                .flatMap(
                    apiIdentifierEntity -> apiIdentifierEntity.getResolvedUrlPatterns().stream())
                .distinct()
                .collect(Collectors.toUnmodifiableList());
        return buildPathMatchCondition(urlRegexes, scopeExpression.getExclude());

      case URL_SCOPE:
        return buildPathMatchCondition(
            scopeExpression.getUrlScope().getUrlRegexesList(), scopeExpression.getExclude());

      default:
        throw new IllegalArgumentException("Unknown scope case: " + scopeExpression.getScopeCase());
    }
  }

  @Override
  public Clause.ClauseCase getClauseCase() {
    return ClauseCase.SCOPE_EXPRESSION;
  }

  private MatchCondition buildPathMatchCondition(List<String> urlRegexes, boolean exclude) {
    return ConverterUtils.buildLikeOperatorMatchCondition(PATH_ATTRIBUTE, urlRegexes)
        .setNegate(exclude)
        .build();
  }
}
