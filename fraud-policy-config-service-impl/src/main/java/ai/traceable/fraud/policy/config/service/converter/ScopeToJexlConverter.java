package ai.traceable.fraud.policy.config.service.converter;

import static ai.traceable.fraud.policy.config.service.converter.JexlExpressionUtils.SPAN_VAR;
import static ai.traceable.fraud.policy.config.service.converter.JexlExpressionUtils.escapeJexlString;
import static ai.traceable.fraud.policy.config.service.converter.JexlExpressionUtils.valueToString;

import ai.traceable.entity.fetcher.cache.CachedApiMappingProvider;
import ai.traceable.entity.fetcher.cache.CachedApiMappingProvider.ApiIdentifierEntity;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityScope;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityType;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.ExtractionLocation;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.FilterOperator;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.Scope;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.SpanBasedFilter;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.SpanBasedScope;
import com.google.protobuf.Value;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

/**
 * Converts a proto Scope object to a JEXL boolean expression string for the EDS context.
 *
 * <p>Uses Java-style getter accessors (e.g. {@code $s.getUrl()}, {@code $s.getEnvironment()})
 * matching the EDS span context API. Multiple scope dimensions are ANDed together.
 *
 * <p>For API entity scopes, resolves API IDs to url/httpMethod/serviceName conditions via {@link
 * CachedApiMappingProvider} instead of using raw apiId matching.
 */
@Slf4j
@Singleton
public class ScopeToJexlConverter {

  private final CachedApiMappingProvider cachedApiMappingProvider;

  @Inject
  public ScopeToJexlConverter(CachedApiMappingProvider cachedApiMappingProvider) {
    this.cachedApiMappingProvider = cachedApiMappingProvider;
  }

  /**
   * Converts a Scope proto to a JEXL boolean expression for EDS.
   *
   * @param requestContext tenant context for resolving API IDs
   * @param scope the proto Scope containing filter dimensions
   * @return JEXL boolean expression string, or empty string when scope is null/empty (no condition)
   */
  public String convert(RequestContext requestContext, Scope scope) {
    if (scope == null) {
      return "";
    }

    List<String> expressions = new ArrayList<>();

    if (scope.hasEnvironmentScope()
        && !scope.getEnvironmentScope().getEnvironmentsList().isEmpty()) {
      expressions.add(
          JexlExpressionUtils.toEqualsExpr(
              SPAN_VAR + ".getEnvironment()", scope.getEnvironmentScope().getEnvironmentsList()));
    }

    if (scope.hasEntityScope() && !scope.getEntityScope().getEntityIdsList().isEmpty()) {
      String entityExpr = resolveEntityScope(requestContext, scope.getEntityScope());
      if (!entityExpr.isEmpty()) {
        expressions.add(entityExpr);
      }
    }

    if (scope.hasJexlScope() && !scope.getJexlScope().getJexlExpression().isEmpty()) {
      expressions.add(scope.getJexlScope().getJexlExpression());
    }

    if (scope.hasSpanBasedScope() && !scope.getSpanBasedScope().getFiltersList().isEmpty()) {
      expressions.add(convertSpanBasedScope(scope.getSpanBasedScope()));
    }

    if (expressions.isEmpty()) {
      return "";
    }

    if (expressions.size() == 1) {
      return expressions.get(0);
    }

    return "(" + String.join(" && ", expressions) + ")";
  }

  // --- Entity scope resolution ---

  private String resolveEntityScope(RequestContext requestContext, EntityScope entityScope) {
    if (entityScope.getEntityType() == EntityType.ENTITY_TYPE_SERVICE) {
      return JexlExpressionUtils.toEqualsExpr(
          SPAN_VAR + ".getServiceName()", entityScope.getEntityIdsList());
    }

    // API type: resolve API IDs to url/method/service JEXL conditions
    Set<String> apiIds = new HashSet<>(entityScope.getEntityIdsList());
    try {
      Map<String, Optional<ApiIdentifierEntity>> apiDetailsMap =
          cachedApiMappingProvider.getApiIdentifierEntities(requestContext, apiIds);

      Set<String> urlRegexes = new LinkedHashSet<>();
      Set<String> httpMethods = new LinkedHashSet<>();
      Set<String> serviceNames = new LinkedHashSet<>();
      ApiScopeResolver.aggregateApiDetails(apiDetailsMap, urlRegexes, httpMethods, serviceNames);

      List<String> parts = new ArrayList<>();
      if (!urlRegexes.isEmpty()) {
        parts.add(JexlExpressionUtils.toUrlRegexExpr(urlRegexes));
      }
      if (!httpMethods.isEmpty()) {
        parts.add(
            JexlExpressionUtils.toEqualsExpr(
                SPAN_VAR + ".getHttpMethod()", new ArrayList<>(httpMethods)));
      }
      if (!serviceNames.isEmpty()) {
        parts.add(
            JexlExpressionUtils.toEqualsExpr(
                SPAN_VAR + ".getServiceName()", new ArrayList<>(serviceNames)));
      }

      if (parts.isEmpty()) {
        return "";
      }
      if (parts.size() == 1) {
        return parts.get(0);
      }
      return "(" + String.join(" && ", parts) + ")";
    } catch (Exception e) {
      log.warn("Failed to resolve entity scope API IDs for match_condition, skipping", e);
      return "";
    }
  }

  // --- Span-based scope ---

  private static String convertSpanBasedScope(SpanBasedScope spanBasedScope) {
    List<SpanBasedFilter> filters = spanBasedScope.getFiltersList();
    if (filters == null || filters.isEmpty()) {
      return "";
    }
    String expr =
        filters.stream()
            .map(ScopeToJexlConverter::convertFilter)
            .collect(Collectors.joining(" && "));
    return filters.size() > 1 ? "(" + expr + ")" : expr;
  }

  private static String convertFilter(SpanBasedFilter filter) {
    String fieldPath = resolveFieldPath(filter.getLocation());
    return applyOperator(fieldPath, filter.getOperator(), filter.getValue());
  }

  private static String resolveFieldPath(ExtractionLocation location) {
    String base;
    switch (location.getLocationType()) {
      case EXTRACTION_LOCATION_TYPE_REQUEST_HEADER:
        base = "getRequestHeaders()";
        break;
      case EXTRACTION_LOCATION_TYPE_REQUEST_BODY:
        base = "getParsedRequestBodyJson()";
        break;
      case EXTRACTION_LOCATION_TYPE_REQUEST_QUERY_PARAM:
        base = "getRequestQueryParams()";
        break;
      case EXTRACTION_LOCATION_TYPE_REQUEST_COOKIE:
        base = "getRequestCookies()";
        break;
      case EXTRACTION_LOCATION_TYPE_RESPONSE_HEADER:
      case EXTRACTION_LOCATION_TYPE_RESPONSE_BODY:
      default:
        log.warn("Unsupported extraction location type in scope: {}", location.getLocationType());
        return SPAN_VAR;
    }
    String key = location.getKey();
    if (key == null || key.isEmpty()) {
      return SPAN_VAR + "." + base;
    }
    return SPAN_VAR + "." + base + ".get('" + escapeJexlString(key) + "')";
  }

  private static String applyOperator(String fieldPath, FilterOperator operator, Value value) {
    boolean isNull = value.getKindCase() == Value.KindCase.NULL_VALUE;
    boolean isString = value.getKindCase() == Value.KindCase.STRING_VALUE;
    String val =
        isString ? "'" + escapeJexlString(valueToString(value)) + "'" : valueToString(value);
    switch (operator) {
      case FILTER_OPERATOR_EQ:
        if (isNull) {
          return fieldPath + " == null";
        }
        return isString ? fieldPath + ".equals(" + val + ")" : fieldPath + " == " + val;
      case FILTER_OPERATOR_NEQ:
        if (isNull) {
          return fieldPath + " != null";
        }
        return isString ? "!" + fieldPath + ".equals(" + val + ")" : fieldPath + " != " + val;
      case FILTER_OPERATOR_GT:
        return fieldPath + " > " + val;
      case FILTER_OPERATOR_LT:
        return fieldPath + " < " + val;
      case FILTER_OPERATOR_GTE:
        return fieldPath + " >= " + val;
      case FILTER_OPERATOR_LTE:
        return fieldPath + " <= " + val;
      case FILTER_OPERATOR_REGEX_MATCHES:
        return fieldPath + " =~ " + val;
      case FILTER_OPERATOR_CONTAINS:
        return fieldPath + ".contains(" + val + ")";
      default:
        log.warn("Unsupported filter operator in scope: {}", operator);
        return "true";
    }
  }
}
