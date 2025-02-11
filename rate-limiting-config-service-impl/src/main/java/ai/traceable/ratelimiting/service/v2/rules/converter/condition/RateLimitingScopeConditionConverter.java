package ai.traceable.ratelimiting.service.v2.rules.converter.condition;

import static ai.traceable.datamodel.data.transformation.config.v1.FieldType.FIELD_TYPE_STR;
import static ai.traceable.datamodel.data.transformation.config.v1.MatchOperator.MATCH_OPERATOR_LIKE;
import static ai.traceable.ratelimiting.config.service.v2.LeafCondition.ConditionCase.SCOPE_CONDITION;
import static ai.traceable.ratelimiting.config.service.v2.ScopeCondition.EntityType.ENTITY_TYPE_API;
import static ai.traceable.ratelimiting.config.service.v2.ScopeCondition.LabelType.LABEL_TYPE_API;

import ai.traceable.datamodel.data.transformation.config.v1.AttributeDerivationMapping;
import ai.traceable.datamodel.data.transformation.config.v1.BinaryOperator;
import ai.traceable.datamodel.data.transformation.config.v1.DataTransformationConfig;
import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.StructuredMatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.UnaryOperator;
import ai.traceable.datamodel.data.transformation.config.v1.VariableDerivationMapping;
import ai.traceable.entity.fetcher.cache.CachedApiMappingProvider;
import ai.traceable.entity.fetcher.cache.CachedApiMappingProvider.ApiIdentifierEntity;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition.ConditionCase;
import ai.traceable.ratelimiting.config.service.v2.ScopeCondition;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = @Inject)
public class RateLimitingScopeConditionConverter implements RateLimitingConditionConverter {
  public static final String ENDPOINT_ID = "TRACEABLE_ENDPOINT_ID";
  private static final StructuredMatchCondition CONDITION_WITH_PATH =
      StructuredMatchCondition.newBuilder()
          .setLhs(
              AttributeDerivationMapping.newBuilder()
                  .setName("lhs")
                  .setType(FIELD_TYPE_STR)
                  .addRules(
                      DerivationRule.newBuilder()
                          .setTransformationConfig(
                              DataTransformationConfig.newBuilder()
                                  .setOutputType(FIELD_TYPE_STR)
                                  .setJexlExpression(
                                      JexlExpressionConfig.newBuilder()
                                          .setJexlExpression("$s.getPath()")))))
          .build();
  private static final StructuredMatchCondition CONDITION_WITH_ENDPOINT_ID =
      StructuredMatchCondition.newBuilder()
          .setLhs(
              AttributeDerivationMapping.newBuilder()
                  .setName("lhs")
                  .setType(FIELD_TYPE_STR)
                  .addRules(
                      DerivationRule.newBuilder()
                          .setTransformationConfig(
                              DataTransformationConfig.newBuilder()
                                  .setOutputType(FIELD_TYPE_STR)
                                  .setJexlExpression(
                                      JexlExpressionConfig.newBuilder()
                                          .setJexlExpression(ENDPOINT_ID)))))
          .setUnaryOperator(UnaryOperator.UNARY_OPERATOR_IS_NOT_NULL)
          .build();

  private static final MatchCondition MATCH_CONDITION_WITH_ENDPOINT_ID =
      MatchCondition.newBuilder().setStructuredMatchCondition(CONDITION_WITH_ENDPOINT_ID).build();

  private final CachedApiMappingProvider cachedApiMappingProvider;

  private List<ApiIdentifierEntity> getApiIdentifierEntities(
      RequestContext requestContext, ScopeCondition scopeCondition) {
    final List<DerivationRule> derivationRules = new ArrayList<>();
    final List<ApiIdentifierEntity> apiIdentifierEntityList = new ArrayList<>();
    switch (scopeCondition.getScopeCase()) {
      case ENTITY_SCOPE:
        ScopeCondition.EntityScope entityScope = scopeCondition.getEntityScope();
        if (!entityScope.getEntityType().equals(ENTITY_TYPE_API)) {
          throw new IllegalArgumentException(
              "Unsupported entity type: " + entityScope.getEntityType());
        }
        Map<String, Optional<ApiIdentifierEntity>> apiIdentifierEntities =
            cachedApiMappingProvider.getApiIdentifierEntities(
                requestContext, Set.copyOf(entityScope.getEntityIdsList()));
        return apiIdentifierEntities.values().stream()
            .flatMap(Optional::stream)
            .collect(Collectors.toUnmodifiableList());
      case LABEL_SCOPE:
        ScopeCondition.LabelScope labelScope = scopeCondition.getLabelScope();
        if (!labelScope.getLabelType().equals(LABEL_TYPE_API)) {
          throw new IllegalArgumentException(
              "Unsupported label type: " + labelScope.getLabelType());
        }
        Map<String, Set<ApiIdentifierEntity>> apiIdentifierEntitiesMap =
            cachedApiMappingProvider.getApiIdentifierEntitiesHavingLabels(
                requestContext, Set.copyOf(labelScope.getLabelIdsList()));
        return apiIdentifierEntitiesMap.values().stream()
            .flatMap(Collection::stream)
            .distinct()
            .collect(Collectors.toUnmodifiableList());
      default:
        return Collections.emptyList();
    }
  }

  @Override
  public MatchConditionDetails buildMatchCondition(
      final RequestContext requestContext, final LeafCondition leafCondition) {
    final ScopeCondition scopeCondition = leafCondition.getScopeCondition();
    switch (scopeCondition.getScopeCase()) {
      case URL_SCOPE:
        String regexes = String.join("|", scopeCondition.getUrlScope().getUrlRegexesList());
        StructuredMatchCondition.Builder builderWithPath =
            CONDITION_WITH_PATH.toBuilder()
                .setBinaryOperator(
                    BinaryOperator.newBuilder()
                        .setMatchOperator(MATCH_OPERATOR_LIKE)
                        .setRegex(regexes));
        return new MatchConditionDetails(
            MatchCondition.newBuilder().setStructuredMatchCondition(builderWithPath).build(),
            Collections.emptyList(),
            Collections.emptyList());
      case ENTITY_SCOPE:
      case LABEL_SCOPE:
        return new MatchConditionDetails(
            MATCH_CONDITION_WITH_ENDPOINT_ID,
            Collections.emptyList(),
            getApiIdentifierEntities(requestContext, scopeCondition));
      default:
        throw new IllegalArgumentException("Unknown scope case: " + scopeCondition.getScopeCase());
    }
  }

  @Override
  public ConditionCase getConditionCase() {
    return SCOPE_CONDITION;
  }

  public static Optional<VariableDerivationMapping> buildVariableDerivationMapping(
      List<ApiIdentifierEntity> apiIdentifierEntities) {
    List<DerivationRule> derivationRules = new ArrayList<>();
    apiIdentifierEntities.forEach(
        apiIdentifierEntity -> {
          derivationRules.add(createDerivationRule(apiIdentifierEntity));
        });
    return derivationRules.isEmpty()
        ? Optional.empty()
        : Optional.of(
            VariableDerivationMapping.newBuilder()
                .setName(ENDPOINT_ID)
                .addAllRules(derivationRules)
                .build());
  }

  private static DerivationRule createDerivationRule(ApiIdentifierEntity apiIdentifierEntity) {
    DerivationRule.Builder builder = DerivationRule.newBuilder();
    String regexes = String.join("|", apiIdentifierEntity.getResolvedUrlPatterns());
    builder.setMatchCondition(
        MatchCondition.newBuilder()
            .setStructuredMatchCondition(
                CONDITION_WITH_PATH.toBuilder()
                    .setBinaryOperator(
                        BinaryOperator.newBuilder()
                            .setMatchOperator(MATCH_OPERATOR_LIKE)
                            .setRegex(regexes))));
    builder.setTransformationConfig(
        DataTransformationConfig.newBuilder()
            .setStaticValue(Value.newBuilder().setStringValue(apiIdentifierEntity.getApiId())));
    return builder.build();
  }
}
