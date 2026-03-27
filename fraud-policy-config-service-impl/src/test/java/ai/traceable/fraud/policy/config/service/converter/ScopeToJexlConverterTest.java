package ai.traceable.fraud.policy.config.service.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import ai.traceable.entity.fetcher.cache.CachedApiMappingProvider;
import ai.traceable.entity.fetcher.cache.CachedApiMappingProvider.ApiIdentifierEntity;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityScope;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityType;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EnvironmentScope;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.ExtractionLocation;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.ExtractionLocationType;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.FilterOperator;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.JexlScope;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.Scope;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.SpanBasedFilter;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.SpanBasedScope;
import com.google.protobuf.Value;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ScopeToJexlConverterTest {

  @Mock private CachedApiMappingProvider cachedApiMappingProvider;

  private ScopeToJexlConverter converter;
  private RequestContext requestContext;

  @BeforeEach
  void setUp() {
    when(cachedApiMappingProvider.getApiIdentifierEntities(any(), any())).thenReturn(Map.of());
    converter = new ScopeToJexlConverter(cachedApiMappingProvider);
    requestContext = RequestContext.forTenantId("test-tenant");
  }

  // --- Environment scope ---

  @Test
  void convert_environmentScope_singleEnv() {
    Scope scope =
        Scope.newBuilder()
            .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironments("production"))
            .build();

    String result = convert(scope);

    assertEquals("$s.getEnvironment().equals('production')", result);
  }

  @Test
  void convert_environmentScope_multipleEnvs() {
    Scope scope =
        Scope.newBuilder()
            .setEnvironmentScope(
                EnvironmentScope.newBuilder()
                    .addEnvironments("production")
                    .addEnvironments("staging"))
            .build();

    String result = convert(scope);

    assertEquals(
        "($s.getEnvironment().equals('production') || $s.getEnvironment().equals('staging'))",
        result);
  }

  // --- API entity scope (resolved to url/httpMethod/serviceName) ---

  @Test
  void convert_apiEntityScope_resolvedToUrlMethodService() {
    when(cachedApiMappingProvider.getApiIdentifierEntities(any(), any()))
        .thenReturn(Map.of("api-1", Optional.of(apiEntity("/api/v1/test", "GET", "test-svc"))));

    Scope scope = apiScope("api-1");
    String result = convert(scope);

    assertTrue(result.contains("$s.getUrl() =~ '/api/v1/test'"));
    assertTrue(result.contains("$s.getHttpMethod().equals('GET')"));
    assertTrue(result.contains("$s.getServiceName().equals('test-svc')"));
  }

  @Test
  void convert_apiEntityScope_multipleUrlPatterns_orJoined() {
    when(cachedApiMappingProvider.getApiIdentifierEntities(any(), any()))
        .thenReturn(
            Map.of(
                "api-1",
                Optional.of(
                    new ApiIdentifierEntity(
                        "api-1",
                        "api-1",
                        "/api-1",
                        List.of("/api/v1/foo", "/api/v1/bar"),
                        List.of(),
                        "POST",
                        "svc"))));

    Scope scope = apiScope("api-1");
    String result = convert(scope);

    assertTrue(result.contains("$s.getUrl() =~ '/api/v1/foo'"));
    assertTrue(result.contains("$s.getUrl() =~ '/api/v1/bar'"));
    assertTrue(result.contains("||"));
  }

  @Test
  void convert_apiEntityScope_resolutionFails_returnsEmpty() {
    when(cachedApiMappingProvider.getApiIdentifierEntities(any(), any()))
        .thenThrow(new RuntimeException("service unavailable"));

    String result = convert(apiScope("api-1"));

    assertEquals("", result);
  }

  // --- Service entity scope ---

  @Test
  void convert_serviceEntityScope_usesServiceName() {
    Scope scope =
        Scope.newBuilder()
            .setEntityScope(
                EntityScope.newBuilder()
                    .setEntityType(EntityType.ENTITY_TYPE_SERVICE)
                    .addEntityIds("svc-1"))
            .build();

    String result = convert(scope);

    assertEquals("$s.getServiceName().equals('svc-1')", result);
  }

  @Test
  void convert_serviceEntityScope_multipleIds() {
    Scope scope =
        Scope.newBuilder()
            .setEntityScope(
                EntityScope.newBuilder()
                    .setEntityType(EntityType.ENTITY_TYPE_SERVICE)
                    .addEntityIds("svc-1")
                    .addEntityIds("svc-2"))
            .build();

    String result = convert(scope);

    assertEquals(
        "($s.getServiceName().equals('svc-1') || $s.getServiceName().equals('svc-2'))", result);
  }

  // --- JEXL scope ---

  @Test
  void convert_jexlScope_passesThrough() {
    Scope scope =
        Scope.newBuilder()
            .setJexlScope(JexlScope.newBuilder().setJexlExpression("$s.custom() == 'value'"))
            .build();

    String result = convert(scope);

    assertEquals("$s.custom() == 'value'", result);
  }

  // --- Span-based scope ---

  @Test
  void convert_spanBasedScope_eqFilter() {
    Scope scope =
        Scope.newBuilder()
            .setSpanBasedScope(
                SpanBasedScope.newBuilder()
                    .addFilters(
                        SpanBasedFilter.newBuilder()
                            .setOperator(FilterOperator.FILTER_OPERATOR_EQ)
                            .setLocation(
                                ExtractionLocation.newBuilder()
                                    .setLocationType(
                                        ExtractionLocationType
                                            .EXTRACTION_LOCATION_TYPE_REQUEST_HEADER)
                                    .setKey("x-api-key"))
                            .setValue(Value.newBuilder().setStringValue("secret"))))
            .build();

    String result = convert(scope);

    assertEquals("$s.getRequestHeaders().get('x-api-key').equals('secret')", result);
  }

  @Test
  void convert_spanBasedScope_multipleFilters_anded() {
    Scope scope =
        Scope.newBuilder()
            .setSpanBasedScope(
                SpanBasedScope.newBuilder()
                    .addFilters(
                        SpanBasedFilter.newBuilder()
                            .setOperator(FilterOperator.FILTER_OPERATOR_EQ)
                            .setLocation(
                                ExtractionLocation.newBuilder()
                                    .setLocationType(
                                        ExtractionLocationType
                                            .EXTRACTION_LOCATION_TYPE_REQUEST_HEADER)
                                    .setKey("h1"))
                            .setValue(Value.newBuilder().setStringValue("v1")))
                    .addFilters(
                        SpanBasedFilter.newBuilder()
                            .setOperator(FilterOperator.FILTER_OPERATOR_NEQ)
                            .setLocation(
                                ExtractionLocation.newBuilder()
                                    .setLocationType(
                                        ExtractionLocationType
                                            .EXTRACTION_LOCATION_TYPE_REQUEST_HEADER)
                                    .setKey("h2"))
                            .setValue(Value.newBuilder().setStringValue("v2"))))
            .build();

    String result = convert(scope);

    assertTrue(result.contains("$s.getRequestHeaders().get('h1').equals('v1')"));
    assertTrue(result.contains("!$s.getRequestHeaders().get('h2').equals('v2')"));
    assertTrue(result.contains("&&"));
  }

  // --- Combined scopes (ANDed) ---

  @Test
  void convert_envAndApiScope_anded() {
    when(cachedApiMappingProvider.getApiIdentifierEntities(any(), any()))
        .thenReturn(Map.of("api-1", Optional.of(apiEntity("/v1/test", "POST", "my-svc"))));

    Scope scope =
        Scope.newBuilder()
            .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironments("production"))
            .setEntityScope(
                EntityScope.newBuilder()
                    .setEntityType(EntityType.ENTITY_TYPE_API)
                    .addEntityIds("api-1"))
            .build();

    String result = convert(scope);

    assertTrue(result.contains("$s.getEnvironment().equals('production')"));
    assertTrue(result.contains("$s.getUrl() =~ '/v1/test'"));
    assertTrue(result.contains("$s.getHttpMethod().equals('POST')"));
    assertTrue(result.contains("$s.getServiceName().equals('my-svc')"));
    assertTrue(result.startsWith("("));
    assertTrue(result.contains("&&"));
  }

  @Test
  void convert_envAndApiAndSpanBased_allAnded() {
    when(cachedApiMappingProvider.getApiIdentifierEntities(any(), any()))
        .thenReturn(Map.of("api-1", Optional.of(apiEntity("/v1/test", "GET", "svc"))));

    Scope scope =
        Scope.newBuilder()
            .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironments("prod"))
            .setEntityScope(
                EntityScope.newBuilder()
                    .setEntityType(EntityType.ENTITY_TYPE_API)
                    .addEntityIds("api-1"))
            .setSpanBasedScope(
                SpanBasedScope.newBuilder()
                    .addFilters(
                        SpanBasedFilter.newBuilder()
                            .setOperator(FilterOperator.FILTER_OPERATOR_EQ)
                            .setLocation(
                                ExtractionLocation.newBuilder()
                                    .setLocationType(
                                        ExtractionLocationType
                                            .EXTRACTION_LOCATION_TYPE_REQUEST_HEADER)
                                    .setKey("head"))
                            .setValue(Value.newBuilder().setStringValue("toe"))))
            .build();

    String result = convert(scope);

    assertTrue(result.contains("$s.getEnvironment().equals('prod')"));
    assertTrue(result.contains("$s.getUrl() =~ '/v1/test'"));
    assertTrue(result.contains("$s.getRequestHeaders().get('head').equals('toe')"));
  }

  // --- Null / empty ---

  @Test
  void convert_nullScope_returnsEmpty() {
    assertEquals("", converter.convert(requestContext, null));
  }

  @Test
  void convert_emptyScope_returnsEmpty() {
    assertEquals("", convert(Scope.getDefaultInstance()));
  }

  // --- Operator coverage ---

  @Test
  void convert_spanBasedScope_regexOperator() {
    Scope scope =
        Scope.newBuilder()
            .setSpanBasedScope(
                SpanBasedScope.newBuilder()
                    .addFilters(
                        SpanBasedFilter.newBuilder()
                            .setOperator(FilterOperator.FILTER_OPERATOR_REGEX_MATCHES)
                            .setLocation(
                                ExtractionLocation.newBuilder()
                                    .setLocationType(
                                        ExtractionLocationType
                                            .EXTRACTION_LOCATION_TYPE_REQUEST_HEADER)
                                    .setKey("user-agent"))
                            .setValue(Value.newBuilder().setStringValue(".*bot.*"))))
            .build();

    String result = convert(scope);

    assertEquals("$s.getRequestHeaders().get('user-agent') =~ '.*bot.*'", result);
  }

  @Test
  void convert_spanBasedScope_containsOperator() {
    Scope scope =
        Scope.newBuilder()
            .setSpanBasedScope(
                SpanBasedScope.newBuilder()
                    .addFilters(
                        SpanBasedFilter.newBuilder()
                            .setOperator(FilterOperator.FILTER_OPERATOR_CONTAINS)
                            .setLocation(
                                ExtractionLocation.newBuilder()
                                    .setLocationType(
                                        ExtractionLocationType
                                            .EXTRACTION_LOCATION_TYPE_REQUEST_BODY)
                                    .setKey("data"))
                            .setValue(Value.newBuilder().setStringValue("needle"))))
            .build();

    String result = convert(scope);

    assertEquals("$s.getParsedRequestBodyJson().get('data').contains('needle')", result);
  }

  @Test
  void convert_spanBasedScope_nullValueEq() {
    Scope scope =
        Scope.newBuilder()
            .setSpanBasedScope(
                SpanBasedScope.newBuilder()
                    .addFilters(
                        SpanBasedFilter.newBuilder()
                            .setOperator(FilterOperator.FILTER_OPERATOR_EQ)
                            .setLocation(
                                ExtractionLocation.newBuilder()
                                    .setLocationType(
                                        ExtractionLocationType
                                            .EXTRACTION_LOCATION_TYPE_REQUEST_HEADER)
                                    .setKey("h"))
                            .setValue(
                                Value.newBuilder()
                                    .setNullValue(com.google.protobuf.NullValue.NULL_VALUE))))
            .build();

    String result = convert(scope);

    assertEquals("$s.getRequestHeaders().get('h') == null", result);
  }

  @Test
  void convert_spanBasedScope_numericGt() {
    Scope scope =
        Scope.newBuilder()
            .setSpanBasedScope(
                SpanBasedScope.newBuilder()
                    .addFilters(
                        SpanBasedFilter.newBuilder()
                            .setOperator(FilterOperator.FILTER_OPERATOR_GT)
                            .setLocation(
                                ExtractionLocation.newBuilder()
                                    .setLocationType(
                                        ExtractionLocationType
                                            .EXTRACTION_LOCATION_TYPE_REQUEST_QUERY_PARAM)
                                    .setKey("count"))
                            .setValue(Value.newBuilder().setNumberValue(10))))
            .build();

    String result = convert(scope);

    assertEquals("$s.getRequestQueryParams().get('count') > 10", result);
  }

  // --- Helper ---

  private String convert(Scope scope) {
    return requestContext.call(() -> converter.convert(requestContext, scope));
  }

  private static Scope apiScope(String apiId) {
    return Scope.newBuilder()
        .setEntityScope(
            EntityScope.newBuilder().setEntityType(EntityType.ENTITY_TYPE_API).addEntityIds(apiId))
        .build();
  }

  private static ApiIdentifierEntity apiEntity(String urlRegex, String method, String service) {
    return new ApiIdentifierEntity(
        "api-1", "api-1", "/api-1", List.of(urlRegex), List.of(), method, service);
  }
}
