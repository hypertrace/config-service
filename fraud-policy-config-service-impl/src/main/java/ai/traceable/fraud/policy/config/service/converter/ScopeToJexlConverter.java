package ai.traceable.fraud.policy.config.service.converter;

import static ai.traceable.fraud.policy.config.service.converter.JexlExpressionUtils.SPAN_VAR;
import static ai.traceable.fraud.policy.config.service.converter.JexlExpressionUtils.buildMapAccessJexl;
import static ai.traceable.fraud.policy.config.service.converter.JexlExpressionUtils.escapeJexlString;
import static ai.traceable.fraud.policy.config.service.converter.JexlExpressionUtils.toChainedGetAccess;
import static ai.traceable.fraud.policy.config.service.converter.JexlExpressionUtils.validateExactMatchOnly;
import static ai.traceable.fraud.policy.config.service.converter.JexlExpressionUtils.valueToString;

import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityScope;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.ExtractionLocation;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.ExtractionLocationType;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.FilterOperator;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.KeyMatchType;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.Scope;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.SpanBasedFilter;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.SpanBasedScope;
import ai.traceable.fraud.policy.config.service.converter.EntityScopeResolver.ApiDetails;
import com.google.protobuf.Value;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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
 * <p>Delegates entity ID resolution (API IDs → url/method/service, service IDs → service names) to
 * {@link EntityScopeResolver} and focuses on JEXL expression generation.
 */
@Slf4j
@Singleton
public class ScopeToJexlConverter {

  private final EntityScopeResolver entityScopeResolver;

  @Inject
  public ScopeToJexlConverter(EntityScopeResolver entityScopeResolver) {
    this.entityScopeResolver = entityScopeResolver;
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
    switch (entityScope.getEntityType()) {
      case ENTITY_TYPE_SERVICE:
        return resolveServiceScope(requestContext, entityScope);
      case ENTITY_TYPE_API:
        return resolveApiScope(requestContext, entityScope);
      default:
        log.warn("Unsupported entity type in scope, skipping: {}", entityScope.getEntityType());
        return "";
    }
  }

  private String resolveServiceScope(RequestContext requestContext, EntityScope entityScope) {
    Set<String> serviceIds = new HashSet<>(entityScope.getEntityIdsList());
    List<String> serviceNames = entityScopeResolver.resolveServiceNames(requestContext, serviceIds);
    if (serviceNames.isEmpty()) {
      return "";
    }
    return JexlExpressionUtils.toEqualsExpr(SPAN_VAR + ".getServiceName()", serviceNames);
  }

  private String resolveApiScope(RequestContext requestContext, EntityScope entityScope) {
    Set<String> apiIds = new HashSet<>(entityScope.getEntityIdsList());
    ApiDetails details = entityScopeResolver.resolveApiDetails(requestContext, apiIds);
    if (details.isEmpty()) {
      return "";
    }

    List<String> parts = new ArrayList<>();
    if (!details.getUrlRegexes().isEmpty()) {
      parts.add(JexlExpressionUtils.toUrlRegexExpr(details.getUrlRegexes()));
    }
    if (!details.getHttpMethods().isEmpty()) {
      parts.add(
          JexlExpressionUtils.toEqualsExpr(
              SPAN_VAR + ".getMethod()", new ArrayList<>(details.getHttpMethods())));
    }
    if (!details.getServiceNames().isEmpty()) {
      parts.add(
          JexlExpressionUtils.toEqualsExpr(
              SPAN_VAR + ".getServiceName()", new ArrayList<>(details.getServiceNames())));
    }

    if (parts.isEmpty()) {
      return "";
    }
    if (parts.size() == 1) {
      return parts.get(0);
    }
    return "(" + String.join(" && ", parts) + ")";
  }

  // --- Span-based scope ---

  private static String convertSpanBasedScope(SpanBasedScope spanBasedScope) {
    List<SpanBasedFilter> filters = spanBasedScope.getFiltersList();
    if (filters.isEmpty()) {
      return "";
    }
    List<String> expressions =
        filters.stream()
            .map(ScopeToJexlConverter::convertFilter)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toList());
    if (expressions.isEmpty()) {
      return "";
    }
    String expr = String.join(" && ", expressions);
    return expressions.size() > 1 ? "(" + expr + ")" : expr;
  }

  private static Optional<String> convertFilter(SpanBasedFilter filter) {
    return resolveFieldPath(filter.getLocation())
        .map(fieldPath -> applyOperator(fieldPath, filter.getOperator(), filter.getValue()));
  }

  private static Optional<String> resolveFieldPath(ExtractionLocation location) {
    String base;
    ExtractionLocationType locationType = location.getLocationType();
    KeyMatchType keyMatchType = location.getKeyMatchType();

    switch (locationType) {
      case EXTRACTION_LOCATION_TYPE_REQUEST_HEADER:
        base = "getRequestHeaders()";
        break;
      case EXTRACTION_LOCATION_TYPE_REQUEST_BODY:
        validateExactMatchOnly(keyMatchType, locationType);
        String bodyKey = location.getKey();
        if (bodyKey.isEmpty()) {
          return Optional.of(SPAN_VAR + ".getParsedRequestBodyJson()");
        }
        return Optional.of(
            SPAN_VAR
                + ".getParsedRequestBodyJson()"
                + toChainedGetAccess(bodyKey)
                + ".getAsString()");
      case EXTRACTION_LOCATION_TYPE_REQUEST_QUERY_PARAM:
        base = "getQueryParams()";
        break;
      case EXTRACTION_LOCATION_TYPE_REQUEST_COOKIE:
        base = "getRequestCookies()";
        break;
      case EXTRACTION_LOCATION_TYPE_RESPONSE_HEADER:
      case EXTRACTION_LOCATION_TYPE_RESPONSE_BODY:
      case EXTRACTION_LOCATION_TYPE_RESPONSE_COOKIE:
        log.warn(
            "Skipping response-based extraction location in scope (unsupported for block policies): {}",
            locationType);
        return Optional.empty();
      default:
        log.warn("Unsupported extraction location type in scope: {}", locationType);
        return Optional.empty();
    }
    String key = location.getKey();
    if (key.isEmpty()) {
      return Optional.of(SPAN_VAR + "." + base);
    }
    return Optional.of(buildMapAccessJexl(SPAN_VAR + "." + base, key, keyMatchType));
  }

  private static String applyOperator(String fieldPath, FilterOperator operator, Value value) {
    boolean isNull = value.getKindCase() == Value.KindCase.NULL_VALUE;
    boolean isString = value.getKindCase() == Value.KindCase.STRING_VALUE;
    String val =
        isString ? "'" + escapeJexlString(valueToString(value)) + "'" : valueToString(value);
    String toNum = "traceable:toNum(" + fieldPath + ")";
    switch (operator) {
      case FILTER_OPERATOR_EQ:
        if (isNull) {
          return fieldPath + " == null";
        }
        return fieldPath + " == " + val;
      case FILTER_OPERATOR_NEQ:
        if (isNull) {
          return fieldPath + " != null";
        }
        return fieldPath + " != " + val;
      case FILTER_OPERATOR_GT:
        return toNum + " > " + val;
      case FILTER_OPERATOR_LT:
        return toNum + " < " + val;
      case FILTER_OPERATOR_GTE:
        return toNum + " >= " + val;
      case FILTER_OPERATOR_LTE:
        return toNum + " <= " + val;
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
