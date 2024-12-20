package ai.traceable.ratelimiting.service.v2.rules.converter.condition;

import static ai.traceable.datamodel.data.transformation.config.v1.FieldType.FIELD_TYPE_STR;
import static ai.traceable.ratelimiting.config.service.v2.LeafCondition.ConditionCase.SCOPE_CONDITION;
import static ai.traceable.ratelimiting.config.service.v2.ScopeCondition.EntityType.ENTITY_TYPE_API;
import static ai.traceable.ratelimiting.config.service.v2.ScopeCondition.LabelType.LABEL_TYPE_API;

import ai.traceable.datamodel.data.transformation.config.v1.AttributeDerivationMapping;
import ai.traceable.datamodel.data.transformation.config.v1.DataTransformationConfig;
import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.edge.decision.config.service.v1.BinaryOperator;
import ai.traceable.edge.decision.config.service.v1.MatchCondition;
import ai.traceable.edge.decision.config.service.v1.MatchOperator;
import ai.traceable.edge.decision.config.service.v1.StructuredMatchCondition;
import ai.traceable.entity.fetcher.cache.CachedApiMappingProvider;
import ai.traceable.entity.fetcher.cache.CachedApiMappingProvider.ApiIdentifierEntity;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition.ConditionCase;
import ai.traceable.ratelimiting.config.service.v2.ScopeCondition;
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
public class RateLimitingScopeConditionConverter implements RateLimitingConditionConverter {

  CachedApiMappingProvider cachedApiMappingProvider;

  @Override
  public MatchCondition buildMatchCondition(
      final RequestContext requestContext, final LeafCondition leafCondition) {
    final ScopeCondition scopeCondition = leafCondition.getScopeCondition();
    final StructuredMatchCondition.Builder builder =
        StructuredMatchCondition.newBuilder()
            .setLhs(
                AttributeDerivationMapping.newBuilder()
                    .setName("lhs")
                    .setType(FIELD_TYPE_STR)
                    .addRules(
                        DerivationRule.newBuilder()
                            .setTransformationConfig(
                                DataTransformationConfig.newBuilder()
                                    .setJexlExpression(
                                        JexlExpressionConfig.newBuilder()
                                            .setJexlExpression("$s.getPath()")))));
    List<String> regexesList;
    switch (scopeCondition.getScopeCase()) {
      case URL_SCOPE:
        regexesList = scopeCondition.getUrlScope().getUrlRegexesList();
        break;
      case ENTITY_SCOPE:
        ScopeCondition.EntityScope entityScope = scopeCondition.getEntityScope();
        if (!entityScope.getEntityType().equals(ENTITY_TYPE_API)) {
          throw new IllegalArgumentException(
              "Unsupported entity type: " + entityScope.getEntityType());
        }
        Map<String, Optional<ApiIdentifierEntity>> apiIdentifierEntities =
            cachedApiMappingProvider.getApiIdentifierEntities(
                requestContext, Set.copyOf(entityScope.getEntityIdsList()));
        regexesList =
            apiIdentifierEntities.values().stream()
                .flatMap(Optional::stream)
                .distinct()
                .flatMap(
                    apiIdentifierEntity -> apiIdentifierEntity.getResolvedUrlPattern().stream())
                .collect(Collectors.toUnmodifiableList());
        break;
      case LABEL_SCOPE:
        ScopeCondition.LabelScope labelScope = scopeCondition.getLabelScope();
        if (!labelScope.getLabelType().equals(LABEL_TYPE_API)) {
          throw new IllegalArgumentException(
              "Unsupported label type: " + labelScope.getLabelType());
        }
        Map<String, Set<ApiIdentifierEntity>> apiIdentifierEntitiesMap =
            cachedApiMappingProvider.getApiIdentifierEntitiesHavingLabels(
                requestContext, Set.copyOf(labelScope.getLabelIdsList()));
        regexesList =
            apiIdentifierEntitiesMap.values().stream()
                .flatMap(Collection::stream)
                .distinct()
                .flatMap(
                    apiIdentifierEntity -> apiIdentifierEntity.getResolvedUrlPattern().stream())
                .collect(Collectors.toUnmodifiableList());
        break;
      default:
        throw new IllegalArgumentException("Unknown scope case: " + scopeCondition.getScopeCase());
    }
    if (regexesList.isEmpty()) {
      throw new IllegalArgumentException("regexes list is empty");
    }
    String regexes = String.join("|", regexesList);
    builder.setBinaryOperator(
        BinaryOperator.newBuilder()
            .setMatchOperator(MatchOperator.MATCH_OPERATOR_LIKE)
            .setRegex(regexes));
    return MatchCondition.newBuilder().setStructuredMatchCondition(builder).build();
  }

  @Override
  public ConditionCase getConditionCase() {
    return SCOPE_CONDITION;
  }
}
