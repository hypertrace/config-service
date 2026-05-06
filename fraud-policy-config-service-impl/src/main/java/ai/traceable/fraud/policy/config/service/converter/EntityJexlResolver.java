package ai.traceable.fraud.policy.config.service.converter;

import static ai.traceable.fraud.policy.config.service.converter.JexlExpressionUtils.SPAN_VAR;
import static ai.traceable.fraud.policy.config.service.converter.JexlExpressionUtils.buildMapAccessJexl;
import static ai.traceable.fraud.policy.config.service.converter.JexlExpressionUtils.toChainedGetAccess;
import static ai.traceable.fraud.policy.config.service.converter.JexlExpressionUtils.toPascalCase;
import static ai.traceable.fraud.policy.config.service.converter.JexlExpressionUtils.validateExactMatchOnly;

import ai.traceable.datamodel.data.transformation.config.v1.DataTransformationConfig;
import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.datamodel.data.transformation.config.v1.GenericMatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfig;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfigData;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfigServiceGrpc.EntityDerivationConfigServiceBlockingStub;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EventDerivationConfigDetails;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.ExtractionLocation;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.ExtractionLocationType;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.GetEntityDerivationConfigsRequest;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.GetEntityDerivationConfigsResponse;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.KeyMatchType;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.SpanBasedExtraction;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

/**
 * Resolves derived_entity_id values to lists of {@link DerivationRule} protos by fetching
 * EntityDerivationConfig from the entity derivation config service.
 *
 * <p>Each enabled {@link EventDerivationConfigDetails} produces one {@link DerivationRule} with:
 *
 * <ul>
 *   <li>{@code transformation_config}: JEXL expression (with pipeline applied)
 *   <li>{@code match_condition}: scope converted to JEXL boolean (if scope is present)
 * </ul>
 *
 * <p>JEXL patterns use Java-style getters for the EDS span context:
 *
 * <ul>
 *   <li>SpanBasedExtraction: {@code $s.getRequestHeaders().get('key')}
 *   <li>PrepopulatedSpanAttribute: {@code $s.getIpAddress()} (column_name to camelCase getter)
 *   <li>Raw jexl_expression: passed through as-is
 *   <li>ParentDerivation: recursively resolved to the parent's DerivationRules
 * </ul>
 */
@Slf4j
@Singleton
public class EntityJexlResolver {

  private final EntityDerivationConfigServiceBlockingStub entityDerivationConfigServiceStub;
  private final ScopeToJexlConverter scopeToJexlConverter;
  private final PipelineToJexlConverter pipelineToJexlConverter;

  @Inject
  public EntityJexlResolver(
      EntityDerivationConfigServiceBlockingStub entityDerivationConfigServiceStub,
      PipelineToJexlConverter pipelineToJexlConverter,
      ScopeToJexlConverter scopeToJexlConverter) {
    this.entityDerivationConfigServiceStub = entityDerivationConfigServiceStub;
    this.scopeToJexlConverter = scopeToJexlConverter;
    this.pipelineToJexlConverter = pipelineToJexlConverter;
  }

  /**
   * Batch-fetches entity derivation configs from the entity derivation config service.
   *
   * @param ids set of derived entity IDs to fetch
   * @return map of entity ID to EntityDerivationConfig
   */
  public Map<String, EntityDerivationConfig> fetchConfigs(
      RequestContext requestContext, Set<String> ids) {
    if (ids.isEmpty()) {
      return Map.of();
    }
    try {
      GetEntityDerivationConfigsResponse response =
          requestContext.call(
              () ->
                  entityDerivationConfigServiceStub.getEntityDerivationConfigs(
                      GetEntityDerivationConfigsRequest.newBuilder().addAllIds(ids).build()));
      return response.getEntityDerivationConfigsList().stream()
          .collect(
              Collectors.toMap(EntityDerivationConfig::getId, Function.identity(), (a, b) -> a));
    } catch (Exception ex) {
      log.error("Failed to fetch entity derivation configs for ids: {}", ids, ex);
      return Map.of();
    }
  }

  /**
   * Resolves all entity IDs in the given config map to lists of {@link DerivationRule} protos. Each
   * enabled {@link EventDerivationConfigDetails} produces one rule with scope-based match_condition
   * and extraction JEXL (with pipeline applied).
   *
   * @param entityConfigMap pre-fetched entity derivation configs
   * @return map of derived_entity_id to list of DerivationRule protos
   */
  public Map<String, List<DerivationRule>> resolveAll(
      RequestContext requestContext, Map<String, EntityDerivationConfig> entityConfigMap) {
    return entityConfigMap.entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                entry ->
                    resolveEntityToRules(
                        requestContext, entry.getValue(), entityConfigMap, new HashSet<>()),
                (a, b) -> a));
  }

  /**
   * Resolves entity IDs to variable names. Prefers column_name (already snake_case for system
   * entities), falls back to display_name (converted to snake_case), then entity ID.
   */
  public Map<String, String> resolveVariableNames(
      Map<String, EntityDerivationConfig> entityConfigMap) {
    return entityConfigMap.entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                entry -> {
                  EntityDerivationConfig config = entry.getValue();
                  String columnName = config.getColumnName();
                  if (!columnName.isEmpty()) {
                    return columnName;
                  }
                  String displayName = config.getData().getDisplayName();
                  if (!displayName.isEmpty()) {
                    return JexlExpressionUtils.toSnakeCase(displayName);
                  }
                  return config.getId();
                }));
  }

  private List<DerivationRule> resolveEntityToRules(
      RequestContext requestContext,
      EntityDerivationConfig config,
      Map<String, EntityDerivationConfig> allConfigs,
      Set<String> visited) {
    EntityDerivationConfigData data = config.getData();

    if (data.hasSpanProjection()) {
      List<DerivationRule> rules = new ArrayList<>();
      for (EventDerivationConfigDetails details :
          data.getSpanProjection().getEventDerivationConfigsList()) {
        if (details.getDisabled()) {
          continue;
        }

        String extractionJexl;
        if (details.hasJexlExpression()) {
          extractionJexl = details.getJexlExpression();
        } else if (details.hasSpanExtraction()) {
          extractionJexl = convertSpanBasedExtractionToJexl(details.getSpanExtraction());
        } else {
          continue;
        }
        if (extractionJexl.isEmpty()) {
          continue;
        }

        String fullJexl =
            pipelineToJexlConverter.apply(extractionJexl, details.getPipeline(), config.getId());
        String scopeJexl =
            details.hasScope()
                ? scopeToJexlConverter.convert(requestContext, details.getScope())
                : "";
        rules.add(buildDerivationRule(fullJexl, scopeJexl.isEmpty() ? null : scopeJexl));
      }
      return rules;
    }

    if (data.hasPrepopulatedSpanAttribute()) {
      String columnName = getColumnName(config);
      if (columnName.isEmpty()) {
        log.error(
            "PrepopulatedSpanAttribute entity {} has no column_name or display_name, cannot resolve JEXL",
            config.getId());
        return List.of();
      }
      String jexl = SPAN_VAR + ".get" + toPascalCase(columnName) + "()";
      return List.of(buildDerivationRule(jexl, null));
    }

    if (data.hasParentDerivation()) {
      return resolveParentDerivation(requestContext, config, allConfigs, visited);
    }

    return List.of();
  }

  private DerivationRule buildDerivationRule(String jexlExpression, String scopeJexl) {
    DerivationRule.Builder builder =
        DerivationRule.newBuilder()
            .setTransformationConfig(
                DataTransformationConfig.newBuilder()
                    .setJexlExpression(
                        JexlExpressionConfig.newBuilder().setJexlExpression(jexlExpression)));

    if (scopeJexl != null) {
      builder.setMatchCondition(
          MatchCondition.newBuilder()
              .setGenericMatchCondition(
                  GenericMatchCondition.newBuilder()
                      .setJexlExpression(
                          JexlExpressionConfig.newBuilder().setJexlExpression(scopeJexl))));
    }

    return builder.build();
  }

  private List<DerivationRule> resolveParentDerivation(
      RequestContext requestContext,
      EntityDerivationConfig config,
      Map<String, EntityDerivationConfig> allConfigs,
      Set<String> visited) {
    String parentId = config.getData().getParentDerivation().getParentEntityDerivationId();
    if (!visited.add(parentId)) {
      log.warn(
          "Cycle detected in parent derivation chain for entity: {} -> parent: {}",
          config.getId(),
          parentId);
      return List.of();
    }
    EntityDerivationConfig parentConfig = allConfigs.get(parentId);
    if (parentConfig == null) {
      Map<String, EntityDerivationConfig> parentMap =
          fetchConfigs(requestContext, Set.of(parentId));
      parentConfig = parentMap.get(parentId);
    }
    if (parentConfig != null) {
      return resolveEntityToRules(requestContext, parentConfig, allConfigs, visited);
    }
    log.warn("Could not resolve parent derivation for entity: {}", config.getId());
    return List.of();
  }

  /** Converts SpanBasedExtraction to JEXL using Java-style getters for the EDS span context. */
  private String convertSpanBasedExtractionToJexl(SpanBasedExtraction extraction) {
    ExtractionLocation location = extraction.getLocation();
    ExtractionLocationType locationType = location.getLocationType();
    KeyMatchType keyMatchType = location.getKeyMatchType();
    String rawKey = location.getKey();

    switch (locationType) {
      case EXTRACTION_LOCATION_TYPE_REQUEST_HEADER:
        return buildMapAccessJexl(SPAN_VAR + ".getRequestHeaders()", rawKey, keyMatchType);
      case EXTRACTION_LOCATION_TYPE_REQUEST_BODY:
        validateExactMatchOnly(keyMatchType, locationType);
        return SPAN_VAR
            + ".getParsedRequestBodyJson()"
            + toChainedGetAccess(rawKey)
            + ".getAsString()";
      case EXTRACTION_LOCATION_TYPE_REQUEST_COOKIE:
        return buildMapAccessJexl(SPAN_VAR + ".getRequestCookies()", rawKey, keyMatchType);
      case EXTRACTION_LOCATION_TYPE_REQUEST_QUERY_PARAM:
        return buildMapAccessJexl(SPAN_VAR + ".getQueryParams()", rawKey, keyMatchType);
      case EXTRACTION_LOCATION_TYPE_RESPONSE_HEADER:
      case EXTRACTION_LOCATION_TYPE_RESPONSE_BODY:
      case EXTRACTION_LOCATION_TYPE_RESPONSE_COOKIE:
      case EXTRACTION_LOCATION_TYPE_SPAN_ATTRIBUTE:
        log.warn(
            "Skipping monitor-only extraction location (unsupported for EDS): {}", locationType);
        return "";
      default:
        log.warn("Unsupported extraction location type: {}", locationType);
        return "";
    }
  }

  private String getColumnName(EntityDerivationConfig config) {
    String columnName = config.getColumnName();
    if (!columnName.isEmpty()) {
      return columnName;
    }
    return config.getData().getDisplayName();
  }
}
