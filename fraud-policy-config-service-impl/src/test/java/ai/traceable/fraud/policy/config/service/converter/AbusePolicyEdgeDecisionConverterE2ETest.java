package ai.traceable.fraud.policy.config.service.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.VariableDerivationMapping;
import ai.traceable.edge.decision.config.service.v1.AggregateThresholdRule;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRule;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleCategory;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleDefinition;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionType;
import ai.traceable.edge.decision.config.service.v1.PolicyKind;
import ai.traceable.edge.decision.config.service.v1.SpanAttributeDecoration;
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
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.FilterOperator;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.GetEntityDerivationConfigsResponse;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.PrepopulatedSpanAttribute;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.Scope;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.SpanBasedExtraction;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.SpanBasedFilter;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.SpanBasedScope;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.SpanProjection;
import ai.traceable.fraud.datamodel.event.kind.v1.AggregationFunctionType;
import ai.traceable.fraud.datamodel.event.kind.v1.OperatorType;
import ai.traceable.fraud.datamodel.event.kind.v1.TransformationFunctionInvocation;
import ai.traceable.fraud.datamodel.event.kind.v1.TransformationPipeline;
import ai.traceable.fraud.policy.config.service.v1.AbuseActionConfig;
import ai.traceable.fraud.policy.config.service.v1.AbuseActionType;
import ai.traceable.fraud.policy.config.service.v1.AbuseAggregationConfig;
import ai.traceable.fraud.policy.config.service.v1.AbuseApiIds;
import ai.traceable.fraud.policy.config.service.v1.AbuseApiScope;
import ai.traceable.fraud.policy.config.service.v1.AbuseEnvironmentScope;
import ai.traceable.fraud.policy.config.service.v1.AbuseGroupByConfig;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicy;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyData;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyDetectionFilter;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyLiteralValues;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyLogicalFilter;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyLogicalOperator;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyRelationalFilter;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicyScope;
import ai.traceable.fraud.policy.config.service.v1.AbuseRiskSeverity;
import ai.traceable.fraud.policy.config.service.v1.AbuseSimpleAggregationTemplateConfig;
import ai.traceable.fraud.policy.config.service.v1.AbuseThresholdConfig;
import ai.traceable.fraud.policy.config.service.v1.AbuseThresholdOperator;
import ai.traceable.fraud.policy.config.service.v1.AbuseTimeWindow;
import com.google.protobuf.Duration;
import com.google.protobuf.Value;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/**
 * E2E converter tests that exercise the full AbusePolicy → EdgeDecisionRule conversion pipeline
 * with mocked external deps: entity resolution, scope→JEXL, pipeline transformation, detection
 * filters, and span attribute decoration.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class AbusePolicyEdgeDecisionConverterE2ETest {

  @Mock private EntityDerivationConfigServiceBlockingStub entityDerivationConfigServiceStub;
  @Mock private ApiScopeResolver apiScopeResolver;
  @Mock private PipelineToJexlConverter pipelineToJexlConverter;
  @Mock private CachedApiMappingProvider cachedApiMappingProvider;

  private AbusePolicyEdgeDecisionConverter converter;

  @BeforeEach
  void setUp() {
    when(pipelineToJexlConverter.apply(any(), any(), any())).thenAnswer(inv -> inv.getArgument(0));
    when(cachedApiMappingProvider.getApiIdentifierEntities(any(), any())).thenReturn(Map.of());
    ScopeToJexlConverter scopeToJexlConverter = new ScopeToJexlConverter(cachedApiMappingProvider);
    EntityJexlResolver entityJexlResolver =
        new EntityJexlResolver(
            entityDerivationConfigServiceStub, pipelineToJexlConverter, scopeToJexlConverter);
    when(apiScopeResolver.resolveApiScope(any(), any())).thenReturn(Collections.emptyList());
    Map<AbusePolicyData.TemplateConfigCase, TemplateEdgeDecisionConverter> templateConverters =
        Map.of(
            AbusePolicyData.TemplateConfigCase.SIMPLE_AGGREGATION_TEMPLATE,
            new SimpleAggregationTemplateConverter(new DetectionFilterConverter()));
    converter =
        new AbusePolicyEdgeDecisionConverter(
            entityJexlResolver, apiScopeResolver, templateConverters);
  }

  /**
   * Full end-to-end conversion of a complex policy with: - 3 entities: "Auth Token" (2 derivation
   * configs), "Auth Code" (2 configs), ip_address - API entity scopes resolved to
   * url/httpMethod/serviceName JEXL - Span-based scope filter (header match) - Transformation
   * pipeline (hash→to_string→split) - Detection filters: AND(CONTAINS, STRING_NOT_EQUALS) -
   * Group-by and detection_signature referencing variable names
   */
  @Test
  void convert_complexMultiEntityPolicy_producesFullEdsRule() {
    // --- Mock API resolution ---
    Map<String, Optional<ApiIdentifierEntity>> apiEntities =
        Map.of(
            "api-alpha-001",
            Optional.of(apiEntity("api-alpha-001", "/api/v1/alpha/.*", "POST", "alpha-svc")),
            "api-beta-002",
            Optional.of(apiEntity("api-beta-002", "/api/v1/beta/.*", "POST", "beta-svc")),
            "api-gamma-003",
            Optional.of(apiEntity("api-gamma-003", "/api/v1/gamma/.*", "GET", "gamma-svc")));
    when(cachedApiMappingProvider.getApiIdentifierEntities(any(), any()))
        .thenAnswer(
            inv -> {
              Set<String> apiIds = inv.getArgument(1);
              Map<String, Optional<ApiIdentifierEntity>> result = new HashMap<>();
              for (String id : apiIds) {
                if (apiEntities.containsKey(id)) {
                  result.put(id, apiEntities.get(id));
                }
              }
              return result;
            });

    // --- Mock pipeline: when pipeline is non-empty, simulate hash→to_string→split ---
    when(pipelineToJexlConverter.apply(any(), any(), any()))
        .thenAnswer(
            inv -> {
              String base = inv.getArgument(0);
              TransformationPipeline pipeline = inv.getArgument(1);
              if (pipeline != null && pipeline.getTransformationPipelineCount() > 0) {
                return "String.valueOf(hash(" + base + ", 'SHA256')).split('.')";
              }
              return base;
            });

    // --- Entity derivation configs ---

    // Entity "Auth Token" — 2 EventDerivationConfigDetails
    EntityDerivationConfig authTokenConfig =
        EntityDerivationConfig.newBuilder()
            .setId("entity-hint-001")
            .setData(
                EntityDerivationConfigData.newBuilder()
                    .setDisplayName("Auth Token")
                    .setSpanProjection(
                        SpanProjection.newBuilder()
                            // Detail 1: env=production, api=beta, REQUEST_BODY
                            .addEventDerivationConfigs(
                                EventDerivationConfigDetails.newBuilder()
                                    .setScope(scope("production", "api-beta-002"))
                                    .setSpanExtraction(bodyExtraction("$.AuthTokenValue")))
                            // Detail 2: api=gamma, span_based header x-mode=secure, pipeline
                            .addEventDerivationConfigs(
                                EventDerivationConfigDetails.newBuilder()
                                    .setScope(
                                        Scope.newBuilder()
                                            .setEntityScope(
                                                EntityScope.newBuilder()
                                                    .setEntityType(EntityType.ENTITY_TYPE_API)
                                                    .addEntityIds("api-gamma-003"))
                                            .setSpanBasedScope(
                                                SpanBasedScope.newBuilder()
                                                    .addFilters(
                                                        SpanBasedFilter.newBuilder()
                                                            .setOperator(
                                                                FilterOperator.FILTER_OPERATOR_EQ)
                                                            .setLocation(
                                                                ExtractionLocation.newBuilder()
                                                                    .setLocationType(
                                                                        ExtractionLocationType
                                                                            .EXTRACTION_LOCATION_TYPE_REQUEST_HEADER)
                                                                    .setKey("x-mode"))
                                                            .setValue(
                                                                Value.newBuilder()
                                                                    .setStringValue("secure")))))
                                    .setSpanExtraction(bodyExtraction("$.authToken"))
                                    .setPipeline(
                                        TransformationPipeline.newBuilder()
                                            .addTransformationPipeline(
                                                TransformationFunctionInvocation.newBuilder()
                                                    .setFunctionId(
                                                        "system_defined_function_hash"))))))
            .build();

    // Entity "Auth Code" — 2 EventDerivationConfigDetails
    EntityDerivationConfig authCodeConfig =
        EntityDerivationConfig.newBuilder()
            .setId("entity-word-002")
            .setData(
                EntityDerivationConfigData.newBuilder()
                    .setDisplayName("Auth Code")
                    .setSpanProjection(
                        SpanProjection.newBuilder()
                            .addEventDerivationConfigs(
                                EventDerivationConfigDetails.newBuilder()
                                    .setScope(scope("production", "api-beta-002"))
                                    .setSpanExtraction(bodyExtraction("$.AuthCode")))
                            .addEventDerivationConfigs(
                                EventDerivationConfigDetails.newBuilder()
                                    .setScope(scope("production", "api-alpha-001"))
                                    .setSpanExtraction(
                                        bodyExtraction("$.inputData.Request.AuthCode")))))
            .build();

    // Entity "ip_address" — prepopulated
    EntityDerivationConfig ipConfig =
        EntityDerivationConfig.newBuilder()
            .setId("system_entity_ip_address")
            .setColumnName("ip_address")
            .setData(
                EntityDerivationConfigData.newBuilder()
                    .setPrepopulatedSpanAttribute(PrepopulatedSpanAttribute.getDefaultInstance()))
            .build();

    // Entity "fil17" — filter-only custom header entity (AAP-12668 scenario)
    EntityDerivationConfig filterOnlyEntity =
        EntityDerivationConfig.newBuilder()
            .setId("custom-filter-entity")
            .setData(
                EntityDerivationConfigData.newBuilder()
                    .setDisplayName("Custom Filter Header")
                    .setSpanProjection(
                        SpanProjection.newBuilder()
                            .addEventDerivationConfigs(
                                EventDerivationConfigDetails.newBuilder()
                                    .setSpanExtraction(
                                        SpanBasedExtraction.newBuilder()
                                            .setLocation(
                                                ExtractionLocation.newBuilder()
                                                    .setLocationType(
                                                        ExtractionLocationType
                                                            .EXTRACTION_LOCATION_TYPE_REQUEST_HEADER)
                                                    .setKey("fil17"))))))
            .build();

    when(entityDerivationConfigServiceStub.getEntityDerivationConfigs(any()))
        .thenReturn(
            GetEntityDerivationConfigsResponse.newBuilder()
                .addEntityDerivationConfigs(authTokenConfig)
                .addEntityDerivationConfigs(authCodeConfig)
                .addEntityDerivationConfigs(ipConfig)
                .addEntityDerivationConfigs(filterOnlyEntity)
                .build());

    // --- Policy: COUNT auth_token GROUP BY auth_code, threshold>5, filters ---
    AbusePolicy policy =
        AbusePolicy.newBuilder()
            .setId("policy-complex-001")
            .setData(
                AbusePolicyData.newBuilder()
                    .setName("Auth Token Detect")
                    .setDescription("detect repeat auth token")
                    .setEnabled(true)
                    .setSeverity(AbuseRiskSeverity.ABUSE_RISK_SEVERITY_MEDIUM)
                    .setMessageFormat("Policy alert: Auth Token Detect")
                    .setScope(
                        AbusePolicyScope.newBuilder()
                            .setEnvironmentScope(
                                AbuseEnvironmentScope.newBuilder().addEnvironmentIds("PROD"))
                            .setApiScope(
                                AbuseApiScope.newBuilder()
                                    .setApiIds(AbuseApiIds.newBuilder().addIds("api-beta-002"))))
                    .setAction(
                        AbuseActionConfig.newBuilder()
                            .setActionType(AbuseActionType.ABUSE_ACTION_TYPE_BLOCK))
                    .setSimpleAggregationTemplate(
                        AbuseSimpleAggregationTemplateConfig.newBuilder()
                            .setAggregation(
                                AbuseAggregationConfig.newBuilder()
                                    .setAggregationFunction(
                                        AggregationFunctionType.AGGREGATION_FUNCTION_TYPE_COUNT)
                                    .setDerivedEntityId("entity-hint-001"))
                            .setGroupBy(
                                AbuseGroupByConfig.newBuilder()
                                    .setDerivedEntityId("entity-word-002"))
                            .setThreshold(
                                AbuseThresholdConfig.newBuilder()
                                    .setOperator(
                                        AbuseThresholdOperator
                                            .ABUSE_THRESHOLD_OPERATOR_GREATER_THAN)
                                    .setValue(5))
                            .setTimeWindow(
                                AbuseTimeWindow.newBuilder()
                                    .setLookbackDuration(Duration.newBuilder().setSeconds(300)))
                            .addFilters(
                                AbusePolicyDetectionFilter.newBuilder()
                                    .setLogicalFilter(
                                        AbusePolicyLogicalFilter.newBuilder()
                                            .setOperator(
                                                AbusePolicyLogicalOperator
                                                    .ABUSE_POLICY_LOGICAL_OPERATOR_AND)
                                            .addOperands(
                                                relationalFilter(
                                                    "entity-word-002",
                                                    OperatorType.OPERATOR_TYPE_CONTAINS,
                                                    "xK9mP2"))
                                            .addOperands(
                                                relationalFilter(
                                                    "system_entity_ip_address",
                                                    OperatorType.OPERATOR_TYPE_STRING_NOT_EQUALS,
                                                    "10.0.0.1"))
                                            .addOperands(
                                                relationalFilter(
                                                    "custom-filter-entity",
                                                    OperatorType.OPERATOR_TYPE_STRING_EQUALS,
                                                    "val17"))))))
            .build();

    // --- Convert ---
    EdgeDecisionEngineConfig result = convert(List.of(policy));

    assertEquals(1, result.getDecisionRulesCount());
    EdgeDecisionRule rule = result.getDecisionRules(0);

    // --- Verify envelope ---
    assertEquals("policy-complex-001", rule.getId());
    assertEquals("Auth Token Detect", rule.getName());
    assertEquals("detect repeat auth token", rule.getDescription());
    assertEquals(
        EdgeDecisionRuleCategory.EDGE_DECISION_RULE_CATEGORY_ABUSE_DETECTION,
        rule.getRuleCategory());
    assertEquals(
        EdgeDecisionType.EDGE_DECISION_TYPE_BLOCK, rule.getRuleDecision().getEdgeDecisionType());
    assertEquals("Auth Token Detect", rule.getRuleDecision().getThreatType());
    assertEquals(PolicyKind.POLICY_KIND_BOT_MITIGATION, rule.getPolicyKind());
    assertFalse(rule.getRuleStatus().getDisabled());

    // --- Verify rule_definition ---
    EdgeDecisionRuleDefinition def = rule.getRuleDefinition();
    assertEquals(
        4, def.getRuleVariablesCount()); // auth_token, auth_code, ip_address, custom_filter_header

    // Find variables by name (HashMap order is non-deterministic)
    VariableDerivationMapping authTokenVar = findVariable(def, "auth_token");
    VariableDerivationMapping authCodeVar = findVariable(def, "auth_code");
    VariableDerivationMapping ipAddressVar = findVariable(def, "ip_address");
    VariableDerivationMapping filterVar = findVariable(def, "custom_filter_header");

    // --- auth_token: 2 derivation rules ---
    assertEquals(2, authTokenVar.getRulesCount());

    // Rule 1: REQUEST_BODY $.AuthTokenValue
    assertEquals(
        "$s.getParsedRequestBodyJson().get('AuthTokenValue').getAsString()",
        getTransformJexl(authTokenVar.getRules(0)));
    String mc1 = getMatchConditionJexl(authTokenVar.getRules(0));
    assertTrue(mc1.contains("$s.getPath() =~ '/api/v1/beta/.*'"));
    assertTrue(mc1.contains("$s.getServiceName().equals('beta-svc')"));

    // Rule 2: REQUEST_BODY $.authToken with pipeline (hash→to_string→split)
    assertEquals(
        "String.valueOf(hash($s.getParsedRequestBodyJson().get('authToken').getAsString(), 'SHA256')).split('.')",
        getTransformJexl(authTokenVar.getRules(1)));
    String mc2 = getMatchConditionJexl(authTokenVar.getRules(1));
    assertTrue(mc2.contains("$s.getPath() =~ '/api/v1/gamma/.*'"));
    assertTrue(mc2.contains("$s.getMethod().equals('GET')"));
    assertTrue(mc2.contains("$s.getRequestHeaders().get('x-mode').equals('secure')"));

    // --- auth_code: 2 derivation rules ---
    assertEquals(2, authCodeVar.getRulesCount());
    assertEquals(
        "$s.getParsedRequestBodyJson().get('AuthCode').getAsString()",
        getTransformJexl(authCodeVar.getRules(0)));
    assertEquals(
        "$s.getParsedRequestBodyJson().get('inputData').get('Request').get('AuthCode').getAsString()",
        getTransformJexl(authCodeVar.getRules(1)));

    // --- ip_address: 1 rule, no match_condition ---
    assertEquals(1, ipAddressVar.getRulesCount());
    assertEquals("$s.getIpAddress()", getTransformJexl(ipAddressVar.getRules(0)));
    assertFalse(ipAddressVar.getRules(0).hasMatchCondition());

    // --- custom_filter_header: filter-only entity registered as variable with extraction JEXL
    // (AAP-12668) ---
    assertEquals(1, filterVar.getRulesCount());
    assertEquals("$s.getRequestHeaders().get('fil17')", getTransformJexl(filterVar.getRules(0)));
    assertFalse(filterVar.getRules(0).hasMatchCondition());

    // --- Verify aggregate_threshold_rule ---
    AggregateThresholdRule aggRule = def.getAggregateThresholdRule();
    assertEquals(1, aggRule.getGroupByDimensionsCount());
    assertEquals("auth_code", aggRule.getGroupByDimensions(0).getName());
    assertEquals(5.0, aggRule.getValueAggregateThreshold().getStaticThreshold());
    assertEquals(300, aggRule.getTimeWindow().getSeconds());

    // match_condition: AND(auth_code CONTAINS, ip_address NOT_EQUALS, custom_filter_header EQUALS)
    // All filters reference entities by variable name — EDS must resolve rule_variables
    MatchCondition filterMc = aggRule.getMatchCondition();
    assertEquals(3, filterMc.getLogicalMatchCondition().getConditionsCount());
    assertEquals(
        "auth_code.contains('xK9mP2')",
        filterMc
            .getLogicalMatchCondition()
            .getConditions(0)
            .getGenericMatchCondition()
            .getJexlExpression()
            .getJexlExpression());
    assertEquals(
        "!ip_address.equals('10.0.0.1')",
        filterMc
            .getLogicalMatchCondition()
            .getConditions(1)
            .getGenericMatchCondition()
            .getJexlExpression()
            .getJexlExpression());
    assertEquals(
        "custom_filter_header.equals('val17')",
        filterMc
            .getLogicalMatchCondition()
            .getConditions(2)
            .getGenericMatchCondition()
            .getJexlExpression()
            .getJexlExpression());

    // --- Verify span attributes ---
    List<SpanAttributeDecoration> attrs = rule.getRuleDecision().getSpanAttributesList();
    assertEquals(5, attrs.size());
    // detection_signature uses variable name
    SpanAttributeDecoration sigAttr = attrs.get(4);
    assertEquals(
        "detection_signature", sigAttr.getSpanAttributeKey().getStaticValue().getStringValue());
    assertEquals(
        "'simple_aggregation - Auth Token Detect - ' + auth_code",
        sigAttr.getSpanAttributeValue().getJexlExpression().getJexlExpression());
  }

  // --- Helpers ---

  private EdgeDecisionEngineConfig convert(List<AbusePolicy> policies) {
    RequestContext requestContext = RequestContext.forTenantId("test-tenant");
    return requestContext.call(() -> converter.convert(requestContext, policies));
  }

  private VariableDerivationMapping findVariable(EdgeDecisionRuleDefinition def, String name) {
    return def.getRuleVariablesList().stream()
        .filter(v -> v.getName().equals(name))
        .findFirst()
        .orElseThrow(() -> new AssertionError("Variable not found: " + name));
  }

  private String getTransformJexl(DerivationRule rule) {
    return rule.getTransformationConfig().getJexlExpression().getJexlExpression();
  }

  private String getMatchConditionJexl(DerivationRule rule) {
    return rule.getMatchCondition()
        .getGenericMatchCondition()
        .getJexlExpression()
        .getJexlExpression();
  }

  private static Scope scope(String env, String apiId) {
    Scope.Builder b = Scope.newBuilder();
    if (env != null) {
      b.setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironments(env));
    }
    if (apiId != null) {
      b.setEntityScope(
          EntityScope.newBuilder().setEntityType(EntityType.ENTITY_TYPE_API).addEntityIds(apiId));
    }
    return b.build();
  }

  private static SpanBasedExtraction bodyExtraction(String key) {
    return SpanBasedExtraction.newBuilder()
        .setLocation(
            ExtractionLocation.newBuilder()
                .setLocationType(ExtractionLocationType.EXTRACTION_LOCATION_TYPE_REQUEST_BODY)
                .setKey(key))
        .build();
  }

  private static AbusePolicyDetectionFilter relationalFilter(
      String entityId, OperatorType op, String value) {
    return AbusePolicyDetectionFilter.newBuilder()
        .setRelationalFilter(
            AbusePolicyRelationalFilter.newBuilder()
                .setDerivedEntityId(entityId)
                .setOperator(op)
                .setLiteralValues(
                    AbusePolicyLiteralValues.newBuilder()
                        .addValues(Value.newBuilder().setStringValue(value))))
        .build();
  }

  private static ApiIdentifierEntity apiEntity(
      String id, String urlRegex, String method, String service) {
    return new ApiIdentifierEntity(id, id, "/" + id, List.of(urlRegex), List.of(), method, service);
  }
}
