package ai.traceable.fraud.policy.config.service.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.entity.fetcher.cache.CachedApiMappingProvider;
import ai.traceable.entity.fetcher.cache.CachedApiMappingProvider.ApiIdentifierEntity;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfig;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfigData;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfigServiceGrpc.EntityDerivationConfigServiceBlockingStub;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityScope;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityType;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EnvironmentScope;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EventDerivationConfigDetails;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.ExtractionLocation;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.ExtractionLocationType;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.GetEntityDerivationConfigsResponse;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.KeyMatchType;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.ParentDerivation;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.PrepopulatedSpanAttribute;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.Scope;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.SpanBasedExtraction;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.SpanProjection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class EntityJexlResolverTest {

  @Mock private EntityDerivationConfigServiceBlockingStub entityDerivationConfigServiceStub;
  @Mock private PipelineToJexlConverter pipelineToJexlConverter;
  @Mock private CachedApiMappingProvider cachedApiMappingProvider;

  private EntityJexlResolver resolver;

  @BeforeEach
  void setUp() {
    // Default: API resolution returns empty map (no url/method/service resolved)
    when(cachedApiMappingProvider.getApiIdentifierEntities(any(), any())).thenReturn(Map.of());
    ScopeToJexlConverter scopeToJexlConverter = new ScopeToJexlConverter(cachedApiMappingProvider);
    resolver =
        new EntityJexlResolver(
            entityDerivationConfigServiceStub, pipelineToJexlConverter, scopeToJexlConverter);
    // By default, pipeline converter returns the input expression unchanged
    when(pipelineToJexlConverter.apply(anyString(), any(), anyString()))
        .thenAnswer(inv -> inv.getArgument(0));
  }

  // --- Extraction JEXL tests ---

  @Test
  void resolveAll_prepopulatedIpAddress_singleRuleNoMatchCondition() {
    mockEntityConfigs(
        EntityDerivationConfig.newBuilder()
            .setId("system_entity_ip_address")
            .setColumnName("ip_address")
            .setData(
                EntityDerivationConfigData.newBuilder()
                    .setPrepopulatedSpanAttribute(PrepopulatedSpanAttribute.getDefaultInstance()))
            .build());

    Map<String, List<DerivationRule>> result = resolve(Set.of("system_entity_ip_address"));

    List<DerivationRule> rules = result.get("system_entity_ip_address");
    assertEquals(1, rules.size());
    assertEquals("$s.getIpAddress()", getJexl(rules.get(0)));
    assertFalse(rules.get(0).hasMatchCondition());
  }

  @Test
  void resolveAll_prepopulatedIpAsn() {
    mockEntityConfigs(
        EntityDerivationConfig.newBuilder()
            .setId("system_entity_ip_asn")
            .setColumnName("ip_asn")
            .setData(
                EntityDerivationConfigData.newBuilder()
                    .setPrepopulatedSpanAttribute(PrepopulatedSpanAttribute.getDefaultInstance()))
            .build());

    Map<String, List<DerivationRule>> result = resolve(Set.of("system_entity_ip_asn"));
    assertEquals("$s.getIpAsn()", getJexl(result.get("system_entity_ip_asn").get(0)));
  }

  @Test
  void resolveAll_prepopulatedUserAgent() {
    mockEntityConfigs(
        EntityDerivationConfig.newBuilder()
            .setId("system_entity_user_agent")
            .setColumnName("user_agent")
            .setData(
                EntityDerivationConfigData.newBuilder()
                    .setPrepopulatedSpanAttribute(PrepopulatedSpanAttribute.getDefaultInstance()))
            .build());

    Map<String, List<DerivationRule>> result = resolve(Set.of("system_entity_user_agent"));
    assertEquals("$s.getUserAgent()", getJexl(result.get("system_entity_user_agent").get(0)));
  }

  @Test
  void resolveAll_spanProjectionWithJexlExpression() {
    mockEntityConfigs(
        EntityDerivationConfig.newBuilder()
            .setId("custom_entity_jwt")
            .setData(
                EntityDerivationConfigData.newBuilder()
                    .setSpanProjection(
                        SpanProjection.newBuilder()
                            .addEventDerivationConfigs(
                                EventDerivationConfigDetails.newBuilder()
                                    .setJexlExpression(
                                        "$s.request_headers.get('traceableai_jwt_alg')"))))
            .build());

    Map<String, List<DerivationRule>> result = resolve(Set.of("custom_entity_jwt"));
    assertEquals(
        "$s.request_headers.get('traceableai_jwt_alg')",
        getJexl(result.get("custom_entity_jwt").get(0)));
  }

  @Test
  void resolveAll_spanBasedExtraction_requestHeader() {
    mockEntityConfigs(
        buildSpanExtractionEntity(
            "entity_header",
            ExtractionLocationType.EXTRACTION_LOCATION_TYPE_REQUEST_HEADER,
            "accept-language"));

    Map<String, List<DerivationRule>> result = resolve(Set.of("entity_header"));
    assertEquals(
        "$s.getRequestHeaders().get('accept-language')",
        getJexl(result.get("entity_header").get(0)));
  }

  @Test
  void resolveAll_spanBasedExtraction_requestBody() {
    mockEntityConfigs(
        buildSpanExtractionEntity(
            "entity_body",
            ExtractionLocationType.EXTRACTION_LOCATION_TYPE_REQUEST_BODY,
            "firstName"));

    Map<String, List<DerivationRule>> result = resolve(Set.of("entity_body"));
    assertEquals(
        "$s.getParsedRequestBodyJson().get('firstName').getAsString()",
        getJexl(result.get("entity_body").get(0)));
  }

  @Test
  void resolveAll_spanBasedExtraction_requestCookie() {
    mockEntityConfigs(
        buildSpanExtractionEntity(
            "entity_cookie",
            ExtractionLocationType.EXTRACTION_LOCATION_TYPE_REQUEST_COOKIE,
            "session_id"));

    Map<String, List<DerivationRule>> result = resolve(Set.of("entity_cookie"));
    assertEquals(
        "$s.getRequestCookies().get('session_id')", getJexl(result.get("entity_cookie").get(0)));
  }

  @Test
  void resolveAll_spanBasedExtraction_requestQueryParam() {
    mockEntityConfigs(
        buildSpanExtractionEntity(
            "entity_qp",
            ExtractionLocationType.EXTRACTION_LOCATION_TYPE_REQUEST_QUERY_PARAM,
            "page"));

    Map<String, List<DerivationRule>> result = resolve(Set.of("entity_qp"));
    assertEquals("$s.getQueryParams().get('page')", getJexl(result.get("entity_qp").get(0)));
  }

  @Test
  void resolveAll_spanBasedExtraction_responseHeader_skipped() {
    mockEntityConfigs(
        buildSpanExtractionEntity(
            "entity_resp_hdr",
            ExtractionLocationType.EXTRACTION_LOCATION_TYPE_RESPONSE_HEADER,
            "x-ratelimit"));

    Map<String, List<DerivationRule>> result = resolve(Set.of("entity_resp_hdr"));
    assertTrue(result.get("entity_resp_hdr").isEmpty());
  }

  @Test
  void resolveAll_spanBasedExtraction_responseBody_skipped() {
    mockEntityConfigs(
        buildSpanExtractionEntity(
            "entity_resp_body",
            ExtractionLocationType.EXTRACTION_LOCATION_TYPE_RESPONSE_BODY,
            "error_code"));

    Map<String, List<DerivationRule>> result = resolve(Set.of("entity_resp_body"));
    assertTrue(result.get("entity_resp_body").isEmpty());
  }

  @Test
  void resolveAll_keyWithSingleQuote_escapesCorrectly() {
    mockEntityConfigs(
        buildSpanExtractionEntity(
            "entity_escape",
            ExtractionLocationType.EXTRACTION_LOCATION_TYPE_REQUEST_HEADER,
            "x-header'val"));

    Map<String, List<DerivationRule>> result = resolve(Set.of("entity_escape"));
    assertEquals(
        "$s.getRequestHeaders().get('x-header\\'val')",
        getJexl(result.get("entity_escape").get(0)));
  }

  // --- Multi-rule emission tests ---

  @Test
  void resolveAll_emitsAllEnabledDetailsAsRules() {
    mockEntityConfigs(
        EntityDerivationConfig.newBuilder()
            .setId("multi_entity")
            .setData(
                EntityDerivationConfigData.newBuilder()
                    .setSpanProjection(
                        SpanProjection.newBuilder()
                            .addEventDerivationConfigs(
                                EventDerivationConfigDetails.newBuilder()
                                    .setScope(apiScope("api-1"))
                                    .setJexlExpression("$s.forApi1()"))
                            .addEventDerivationConfigs(
                                EventDerivationConfigDetails.newBuilder()
                                    .setScope(apiScope("api-2"))
                                    .setJexlExpression("$s.forApi2()"))))
            .build());

    Map<String, List<DerivationRule>> result = resolve(Set.of("multi_entity"));

    List<DerivationRule> rules = result.get("multi_entity");
    assertEquals(2, rules.size());
    assertEquals("$s.forApi1()", getJexl(rules.get(0)));
    assertEquals("$s.forApi2()", getJexl(rules.get(1)));
  }

  @Test
  void resolveAll_skipsDisabledEventDerivationConfigDetails() {
    mockEntityConfigs(
        EntityDerivationConfig.newBuilder()
            .setId("entity_with_disabled")
            .setData(
                EntityDerivationConfigData.newBuilder()
                    .setSpanProjection(
                        SpanProjection.newBuilder()
                            .addEventDerivationConfigs(
                                EventDerivationConfigDetails.newBuilder()
                                    .setDisabled(true)
                                    .setJexlExpression("$s.shouldBeSkipped()"))
                            .addEventDerivationConfigs(
                                EventDerivationConfigDetails.newBuilder()
                                    .setJexlExpression("$s.ip_address"))))
            .build());

    Map<String, List<DerivationRule>> result = resolve(Set.of("entity_with_disabled"));

    List<DerivationRule> rules = result.get("entity_with_disabled");
    assertEquals(1, rules.size());
    assertEquals("$s.ip_address", getJexl(rules.get(0)));
  }

  // --- Scope → match_condition tests ---

  @Test
  void resolveAll_scopeConvertedToMatchCondition_apiScope() {
    when(cachedApiMappingProvider.getApiIdentifierEntities(any(), any()))
        .thenReturn(
            Map.of("api-1", Optional.of(apiEntity("api-1", "/api/v1/test", "GET", "test-svc"))));

    mockEntityConfigs(
        EntityDerivationConfig.newBuilder()
            .setId("scoped_entity")
            .setData(
                EntityDerivationConfigData.newBuilder()
                    .setSpanProjection(
                        SpanProjection.newBuilder()
                            .addEventDerivationConfigs(
                                EventDerivationConfigDetails.newBuilder()
                                    .setScope(apiScope("api-1"))
                                    .setJexlExpression("$s.forApi1()"))))
            .build());

    Map<String, List<DerivationRule>> result = resolve(Set.of("scoped_entity"));

    DerivationRule rule = result.get("scoped_entity").get(0);
    assertTrue(rule.hasMatchCondition());
    String conditionJexl =
        rule.getMatchCondition().getGenericMatchCondition().getJexlExpression().getJexlExpression();
    // API IDs resolved to url/httpMethod/serviceName
    assertTrue(conditionJexl.contains("$s.getPath() =~ '/api/v1/test'"));
    assertTrue(conditionJexl.contains("$s.getMethod() == 'GET'"));
    assertTrue(conditionJexl.contains("$s.getServiceName() == 'test-svc'"));
  }

  @Test
  void resolveAll_scopeConvertedToMatchCondition_environmentScope() {
    mockEntityConfigs(
        EntityDerivationConfig.newBuilder()
            .setId("env_entity")
            .setData(
                EntityDerivationConfigData.newBuilder()
                    .setSpanProjection(
                        SpanProjection.newBuilder()
                            .addEventDerivationConfigs(
                                EventDerivationConfigDetails.newBuilder()
                                    .setScope(
                                        Scope.newBuilder()
                                            .setEnvironmentScope(
                                                EnvironmentScope.newBuilder()
                                                    .addEnvironments("production")))
                                    .setJexlExpression("$s.prodExpr()"))))
            .build());

    Map<String, List<DerivationRule>> result = resolve(Set.of("env_entity"));

    DerivationRule rule = result.get("env_entity").get(0);
    assertTrue(rule.hasMatchCondition());
    String conditionJexl =
        rule.getMatchCondition().getGenericMatchCondition().getJexlExpression().getJexlExpression();
    assertTrue(conditionJexl.contains("$s.getEnvironment() == 'production'"));
  }

  @Test
  void resolveAll_noScopeNoMatchCondition() {
    mockEntityConfigs(
        EntityDerivationConfig.newBuilder()
            .setId("wildcard_entity")
            .setData(
                EntityDerivationConfigData.newBuilder()
                    .setSpanProjection(
                        SpanProjection.newBuilder()
                            .addEventDerivationConfigs(
                                EventDerivationConfigDetails.newBuilder()
                                    .setJexlExpression("$s.wildcardExpr()"))))
            .build());

    Map<String, List<DerivationRule>> result = resolve(Set.of("wildcard_entity"));

    DerivationRule rule = result.get("wildcard_entity").get(0);
    assertFalse(rule.hasMatchCondition());
  }

  @Test
  void resolveAll_combinedScopeProducesAndCondition() {
    when(cachedApiMappingProvider.getApiIdentifierEntities(any(), any()))
        .thenReturn(
            Map.of("api-1", Optional.of(apiEntity("api-1", "/api/v1/test", "POST", "test-svc"))));

    mockEntityConfigs(
        EntityDerivationConfig.newBuilder()
            .setId("combined_entity")
            .setData(
                EntityDerivationConfigData.newBuilder()
                    .setSpanProjection(
                        SpanProjection.newBuilder()
                            .addEventDerivationConfigs(
                                EventDerivationConfigDetails.newBuilder()
                                    .setScope(
                                        Scope.newBuilder()
                                            .setEntityScope(
                                                EntityScope.newBuilder()
                                                    .setEntityType(EntityType.ENTITY_TYPE_API)
                                                    .addEntityIds("api-1"))
                                            .setEnvironmentScope(
                                                EnvironmentScope.newBuilder()
                                                    .addEnvironments("production")))
                                    .setJexlExpression("$s.api1Prod()"))))
            .build());

    Map<String, List<DerivationRule>> result = resolve(Set.of("combined_entity"));

    DerivationRule rule = result.get("combined_entity").get(0);
    assertTrue(rule.hasMatchCondition());
    String conditionJexl =
        rule.getMatchCondition().getGenericMatchCondition().getJexlExpression().getJexlExpression();
    assertTrue(conditionJexl.contains("$s.getEnvironment() == 'production'"));
    assertTrue(conditionJexl.contains("$s.getPath() =~ '/api/v1/test'"));
    assertTrue(conditionJexl.contains("$s.getMethod() == 'POST'"));
    assertTrue(conditionJexl.contains("&&"));
  }

  // --- Parent derivation tests ---

  @Test
  void resolveAll_parentDerivation_resolvesToParentRules() {
    EntityDerivationConfig parent =
        EntityDerivationConfig.newBuilder()
            .setId("parent_entity")
            .setColumnName("ip_address")
            .setData(
                EntityDerivationConfigData.newBuilder()
                    .setPrepopulatedSpanAttribute(PrepopulatedSpanAttribute.getDefaultInstance()))
            .build();

    EntityDerivationConfig child =
        EntityDerivationConfig.newBuilder()
            .setId("child_entity")
            .setData(
                EntityDerivationConfigData.newBuilder()
                    .setParentDerivation(
                        ParentDerivation.newBuilder().setParentEntityDerivationId("parent_entity")))
            .build();

    when(entityDerivationConfigServiceStub.getEntityDerivationConfigs(any()))
        .thenReturn(
            GetEntityDerivationConfigsResponse.newBuilder()
                .addEntityDerivationConfigs(child)
                .addEntityDerivationConfigs(parent)
                .build());

    Map<String, List<DerivationRule>> result = resolve(Set.of("child_entity"));

    List<DerivationRule> rules = result.get("child_entity");
    assertEquals(1, rules.size());
    assertEquals("$s.getIpAddress()", getJexl(rules.get(0)));
  }

  @Test
  void resolveAll_circularParentDerivation_returnsEmptyInsteadOfStackOverflow() {
    // parent_a → parent_b → parent_a (cycle)
    EntityDerivationConfig parentA =
        EntityDerivationConfig.newBuilder()
            .setId("parent_a")
            .setData(
                EntityDerivationConfigData.newBuilder()
                    .setParentDerivation(
                        ParentDerivation.newBuilder().setParentEntityDerivationId("parent_b")))
            .build();

    EntityDerivationConfig parentB =
        EntityDerivationConfig.newBuilder()
            .setId("parent_b")
            .setData(
                EntityDerivationConfigData.newBuilder()
                    .setParentDerivation(
                        ParentDerivation.newBuilder().setParentEntityDerivationId("parent_a")))
            .build();

    when(entityDerivationConfigServiceStub.getEntityDerivationConfigs(any()))
        .thenReturn(
            GetEntityDerivationConfigsResponse.newBuilder()
                .addEntityDerivationConfigs(parentA)
                .addEntityDerivationConfigs(parentB)
                .build());

    Map<String, List<DerivationRule>> result = resolve(Set.of("parent_a"));

    // Should not throw StackOverflowError; returns empty rules for the cyclic entity
    assertTrue(result.get("parent_a").isEmpty());
  }

  @Test
  void entity_level_disabled_test() {
    mockEntityConfigs(
        EntityDerivationConfig.newBuilder()
            .setId("disabled_span_entity")
            .setData(
                EntityDerivationConfigData.newBuilder()
                    .setDisabled(true)
                    .setSpanProjection(
                        SpanProjection.newBuilder()
                            .addEventDerivationConfigs(
                                EventDerivationConfigDetails.newBuilder()
                                    .setJexlExpression("$s.shouldNotResolve()"))))
            .build());

    Map<String, List<DerivationRule>> result = resolve(Set.of("disabled_span_entity"));
    assertTrue(result.get("disabled_span_entity").isEmpty());
  }

  @Test
  void resolveAll_disabledPrepopulatedEntity_returnsEmptyRules() {
    mockEntityConfigs(
        EntityDerivationConfig.newBuilder()
            .setId("disabled_prepopulated")
            .setColumnName("ip_address")
            .setData(
                EntityDerivationConfigData.newBuilder()
                    .setDisabled(true)
                    .setPrepopulatedSpanAttribute(PrepopulatedSpanAttribute.getDefaultInstance()))
            .build());

    Map<String, List<DerivationRule>> result = resolve(Set.of("disabled_prepopulated"));
    assertTrue(result.get("disabled_prepopulated").isEmpty());
  }

  @Test
  void resolveAll_allEventDerivationDetailsDisabled_returnsEmptyRules() {
    mockEntityConfigs(
        EntityDerivationConfig.newBuilder()
            .setId("all_details_disabled")
            .setData(
                EntityDerivationConfigData.newBuilder()
                    .setSpanProjection(
                        SpanProjection.newBuilder()
                            .addEventDerivationConfigs(
                                EventDerivationConfigDetails.newBuilder()
                                    .setDisabled(true)
                                    .setJexlExpression("$s.first()"))
                            .addEventDerivationConfigs(
                                EventDerivationConfigDetails.newBuilder()
                                    .setDisabled(true)
                                    .setJexlExpression("$s.second()"))))
            .build());

    Map<String, List<DerivationRule>> result = resolve(Set.of("all_details_disabled"));
    assertTrue(result.get("all_details_disabled").isEmpty());
  }

  @Test
  void resolveAll_disabledParentDerivationEntity_returnsEmptyRules() {
    EntityDerivationConfig parent =
        EntityDerivationConfig.newBuilder()
            .setId("parent_entity_active")
            .setColumnName("ip_address")
            .setData(
                EntityDerivationConfigData.newBuilder()
                    .setPrepopulatedSpanAttribute(PrepopulatedSpanAttribute.getDefaultInstance()))
            .build();

    EntityDerivationConfig disabledChild =
        EntityDerivationConfig.newBuilder()
            .setId("disabled_child")
            .setData(
                EntityDerivationConfigData.newBuilder()
                    .setDisabled(true)
                    .setParentDerivation(
                        ParentDerivation.newBuilder()
                            .setParentEntityDerivationId("parent_entity_active")))
            .build();

    when(entityDerivationConfigServiceStub.getEntityDerivationConfigs(any()))
        .thenReturn(
            GetEntityDerivationConfigsResponse.newBuilder()
                .addEntityDerivationConfigs(disabledChild)
                .addEntityDerivationConfigs(parent)
                .build());

    Map<String, List<DerivationRule>> result = resolve(Set.of("disabled_child"));
    assertTrue(result.get("disabled_child").isEmpty());
  }

  // --- Edge cases ---

  @Test
  void resolveAll_emptyIds_returnsEmptyMap() {
    Map<String, List<DerivationRule>> result = resolve(Set.of());
    assertTrue(result.isEmpty());
  }

  @Test
  void resolveAll_serviceException_returnsEmptyMap() {
    when(entityDerivationConfigServiceStub.getEntityDerivationConfigs(any()))
        .thenThrow(new RuntimeException("gRPC error"));

    Map<String, List<DerivationRule>> result = resolve(Set.of("some_entity"));
    assertTrue(result.isEmpty());
  }

  // --- Helpers ---

  private String getJexl(DerivationRule rule) {
    return rule.getTransformationConfig().getJexlExpression().getJexlExpression();
  }

  private Map<String, List<DerivationRule>> resolve(Set<String> ids) {
    RequestContext requestContext = RequestContext.forTenantId("test-tenant");
    return requestContext.call(
        () -> {
          Map<String, EntityDerivationConfig> configs = resolver.fetchConfigs(requestContext, ids);
          return resolver.resolveAll(requestContext, configs);
        });
  }

  private EntityDerivationConfig buildSpanExtractionEntity(
      String id, ExtractionLocationType locationType, String key) {
    return EntityDerivationConfig.newBuilder()
        .setId(id)
        .setData(
            EntityDerivationConfigData.newBuilder()
                .setSpanProjection(
                    SpanProjection.newBuilder()
                        .addEventDerivationConfigs(
                            EventDerivationConfigDetails.newBuilder()
                                .setSpanExtraction(
                                    SpanBasedExtraction.newBuilder()
                                        .setLocation(
                                            ExtractionLocation.newBuilder()
                                                .setLocationType(locationType)
                                                .setKey(key))))))
        .build();
  }

  private void mockEntityConfigs(EntityDerivationConfig... configs) {
    GetEntityDerivationConfigsResponse.Builder responseBuilder =
        GetEntityDerivationConfigsResponse.newBuilder();
    for (EntityDerivationConfig config : configs) {
      responseBuilder.addEntityDerivationConfigs(config);
    }
    when(entityDerivationConfigServiceStub.getEntityDerivationConfigs(any()))
        .thenReturn(responseBuilder.build());
  }

  private static Scope apiScope(String apiId) {
    return Scope.newBuilder()
        .setEntityScope(
            EntityScope.newBuilder().setEntityType(EntityType.ENTITY_TYPE_API).addEntityIds(apiId))
        .build();
  }

  private static ApiIdentifierEntity apiEntity(
      String id, String urlRegex, String method, String service) {
    return new ApiIdentifierEntity(id, id, "/" + id, List.of(urlRegex), List.of(), method, service);
  }

  // --- KeyMatchType tests ---

  private EntityDerivationConfig buildSpanExtractionEntityWithKeyMatchType(
      String id, ExtractionLocationType locationType, String key, KeyMatchType keyMatchType) {
    return EntityDerivationConfig.newBuilder()
        .setId(id)
        .setData(
            EntityDerivationConfigData.newBuilder()
                .setSpanProjection(
                    SpanProjection.newBuilder()
                        .addEventDerivationConfigs(
                            EventDerivationConfigDetails.newBuilder()
                                .setSpanExtraction(
                                    SpanBasedExtraction.newBuilder()
                                        .setLocation(
                                            ExtractionLocation.newBuilder()
                                                .setLocationType(locationType)
                                                .setKey(key)
                                                .setKeyMatchType(keyMatchType))))))
        .build();
  }

  @Test
  void resolveAll_spanBasedExtraction_regexKeyMatch_header() {
    mockEntityConfigs(
        buildSpanExtractionEntityWithKeyMatchType(
            "entity_regex",
            ExtractionLocationType.EXTRACTION_LOCATION_TYPE_REQUEST_HEADER,
            "x-custom-.*",
            KeyMatchType.KEY_MATCH_TYPE_REGEX));

    Map<String, List<DerivationRule>> result = resolve(Set.of("entity_regex"));
    assertEquals(
        "map:matchingValue($s.getRequestHeaders(), predicate:matchesRegex('x-custom-.*'))",
        getJexl(result.get("entity_regex").get(0)));
  }

  @Test
  void resolveAll_spanBasedExtraction_prefixKeyMatch_cookie() {
    mockEntityConfigs(
        buildSpanExtractionEntityWithKeyMatchType(
            "entity_prefix",
            ExtractionLocationType.EXTRACTION_LOCATION_TYPE_REQUEST_COOKIE,
            "session",
            KeyMatchType.KEY_MATCH_TYPE_PREFIX));

    Map<String, List<DerivationRule>> result = resolve(Set.of("entity_prefix"));
    assertEquals(
        "map:matchingValue($s.getRequestCookies(), predicate:startsWith('session'))",
        getJexl(result.get("entity_prefix").get(0)));
  }

  @Test
  void resolveAll_spanBasedExtraction_containsKeyMatch_queryParam() {
    mockEntityConfigs(
        buildSpanExtractionEntityWithKeyMatchType(
            "entity_contains",
            ExtractionLocationType.EXTRACTION_LOCATION_TYPE_REQUEST_QUERY_PARAM,
            "token",
            KeyMatchType.KEY_MATCH_TYPE_CONTAINS));

    Map<String, List<DerivationRule>> result = resolve(Set.of("entity_contains"));
    assertEquals(
        "map:matchingValue($s.getQueryParams(), predicate:contains('token'))",
        getJexl(result.get("entity_contains").get(0)));
  }

  @Test
  void resolveAll_spanBasedExtraction_bodyNonExactKeyMatch_throws() {
    mockEntityConfigs(
        buildSpanExtractionEntityWithKeyMatchType(
            "entity_body_regex",
            ExtractionLocationType.EXTRACTION_LOCATION_TYPE_REQUEST_BODY,
            "field",
            KeyMatchType.KEY_MATCH_TYPE_REGEX));

    Assertions.assertThrows(
        IllegalArgumentException.class, () -> resolve(Set.of("entity_body_regex")));
  }

  @Test
  void resolveAll_spanBasedExtraction_exactKeyMatch_sameAsDefault() {
    mockEntityConfigs(
        buildSpanExtractionEntityWithKeyMatchType(
            "entity_exact",
            ExtractionLocationType.EXTRACTION_LOCATION_TYPE_REQUEST_HEADER,
            "accept-language",
            KeyMatchType.KEY_MATCH_TYPE_EXACT));

    Map<String, List<DerivationRule>> exactResult = resolve(Set.of("entity_exact"));

    mockEntityConfigs(
        buildSpanExtractionEntity(
            "entity_exact",
            ExtractionLocationType.EXTRACTION_LOCATION_TYPE_REQUEST_HEADER,
            "accept-language"));

    Map<String, List<DerivationRule>> defaultResult = resolve(Set.of("entity_exact"));

    assertEquals(
        getJexl(defaultResult.get("entity_exact").get(0)),
        getJexl(exactResult.get("entity_exact").get(0)));
  }
}
