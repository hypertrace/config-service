package ai.traceable.fraud.policy.config.service.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleScopeCondition;
import ai.traceable.entity.fetcher.cache.CachedApiMappingProvider;
import ai.traceable.entity.fetcher.cache.CachedApiMappingProvider.ApiIdentifierEntity;
import ai.traceable.fraud.policy.config.service.v1.AbuseApiIds;
import ai.traceable.fraud.policy.config.service.v1.AbuseApiLabels;
import ai.traceable.fraud.policy.config.service.v1.AbuseApiScope;
import java.util.Collections;
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

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ApiScopeResolverTest {

  private static final String TENANT_ID = "test-tenant";

  @Mock private CachedApiMappingProvider cachedApiMappingProvider;

  private ApiScopeResolver resolver;
  private RequestContext requestContext;

  @BeforeEach
  void setUp() {
    resolver = new ApiScopeResolver(cachedApiMappingProvider);
    requestContext = RequestContext.forTenantId(TENANT_ID);
  }

  @Test
  void resolveApiScope_nullScope_returnsEmpty() {
    List<EdgeDecisionRuleScopeCondition> result = resolver.resolveApiScope(requestContext, null);
    assertTrue(result.isEmpty());
  }

  @Test
  void resolveApiScope_emptyApiIds_returnsEmpty() {
    AbuseApiScope scope =
        AbuseApiScope.newBuilder().setApiIds(AbuseApiIds.getDefaultInstance()).build();

    List<EdgeDecisionRuleScopeCondition> result = resolver.resolveApiScope(requestContext, scope);

    assertTrue(result.isEmpty());
    verify(cachedApiMappingProvider, never()).getApiIdentifierEntities(any(), anySet());
  }

  @Test
  void resolveApiScope_withApiIds_returnsUrlRegexAndMethodAndService() {
    AbuseApiScope scope =
        AbuseApiScope.newBuilder().setApiIds(AbuseApiIds.newBuilder().addIds("api-1")).build();

    when(cachedApiMappingProvider.getApiIdentifierEntities(any(), anySet()))
        .thenReturn(
            Map.of(
                "api-1",
                Optional.of(
                    new ApiIdentifierEntity(
                        "api-1",
                        "name",
                        "/v1/users",
                        List.of("/v1/users.*"),
                        Collections.emptyList(),
                        "GET",
                        "user-service"))));

    List<EdgeDecisionRuleScopeCondition> result = resolver.resolveApiScope(requestContext, scope);

    assertEquals(3, result.size());
    assertTrue(
        result.stream()
            .anyMatch(
                c ->
                    c.hasUrlRegexScope()
                        && c.getUrlRegexScope().getUrlRegexesList().contains("/v1/users.*")));
    assertTrue(
        result.stream()
            .anyMatch(
                c ->
                    c.hasHttpMethodScope()
                        && c.getHttpMethodScope().getHttpMethodsList().contains("GET")));
    assertTrue(
        result.stream()
            .anyMatch(
                c ->
                    c.hasServiceScope()
                        && c.getServiceScope().getServiceNamesList().contains("user-service")));
  }

  @Test
  void resolveApiScope_multipleApiIds_aggregatesScopes() {
    AbuseApiScope scope =
        AbuseApiScope.newBuilder()
            .setApiIds(AbuseApiIds.newBuilder().addIds("api-1").addIds("api-2"))
            .build();

    when(cachedApiMappingProvider.getApiIdentifierEntities(any(), anySet()))
        .thenReturn(
            Map.of(
                "api-1",
                Optional.of(
                    new ApiIdentifierEntity(
                        "api-1",
                        "n1",
                        "/v1/a",
                        List.of("/v1/a.*"),
                        Collections.emptyList(),
                        "GET",
                        "svc-a")),
                "api-2",
                Optional.of(
                    new ApiIdentifierEntity(
                        "api-2",
                        "n2",
                        "/v1/b",
                        List.of("/v1/b.*"),
                        Collections.emptyList(),
                        "POST",
                        "svc-b"))));

    List<EdgeDecisionRuleScopeCondition> result = resolver.resolveApiScope(requestContext, scope);

    assertEquals(3, result.size());
    EdgeDecisionRuleScopeCondition urlScope =
        result.stream()
            .filter(EdgeDecisionRuleScopeCondition::hasUrlRegexScope)
            .findFirst()
            .orElseThrow();
    assertEquals(2, urlScope.getUrlRegexScope().getUrlRegexesCount());

    EdgeDecisionRuleScopeCondition methodScope =
        result.stream()
            .filter(EdgeDecisionRuleScopeCondition::hasHttpMethodScope)
            .findFirst()
            .orElseThrow();
    assertEquals(2, methodScope.getHttpMethodScope().getHttpMethodsCount());
  }

  @Test
  void resolveApiScope_withApiLabels_resolvesLabelsToApiIds() {
    // api_ids and api_labels are in a oneof, so only one can be set at a time
    AbuseApiScope scope =
        AbuseApiScope.newBuilder()
            .setApiLabels(AbuseApiLabels.newBuilder().addLabels("Critical"))
            .build();

    ApiIdentifierEntity labelEntity =
        new ApiIdentifierEntity(
            "api-from-label",
            "name",
            "/v1/critical",
            List.of("/v1/critical.*"),
            List.of("Critical"),
            "POST",
            "critical-svc");
    when(cachedApiMappingProvider.getApiIdentifierEntitiesHavingLabels(any(), anySet()))
        .thenReturn(Map.of("Critical", Set.of(labelEntity)));
    when(cachedApiMappingProvider.getApiIdentifierEntities(any(), anySet()))
        .thenReturn(Map.of("api-from-label", Optional.of(labelEntity)));

    List<EdgeDecisionRuleScopeCondition> result = resolver.resolveApiScope(requestContext, scope);

    assertEquals(3, result.size());
    verify(cachedApiMappingProvider).getApiIdentifierEntitiesHavingLabels(any(), anySet());
  }

  @Test
  void resolveApiScope_labelResolutionFails_returnsEmpty() {
    // api_ids and api_labels are in a oneof — when labels fail, there are no api_ids to fall back
    // on
    AbuseApiScope scope =
        AbuseApiScope.newBuilder()
            .setApiLabels(AbuseApiLabels.newBuilder().addLabels("BadLabel"))
            .build();

    when(cachedApiMappingProvider.getApiIdentifierEntitiesHavingLabels(any(), anySet()))
        .thenThrow(new RuntimeException("label resolution failed"));

    List<EdgeDecisionRuleScopeCondition> result = resolver.resolveApiScope(requestContext, scope);

    assertTrue(result.isEmpty());
  }

  @Test
  void resolveApiScope_fetchDetailsFails_returnsEmpty() {
    AbuseApiScope scope =
        AbuseApiScope.newBuilder().setApiIds(AbuseApiIds.newBuilder().addIds("api-1")).build();

    when(cachedApiMappingProvider.getApiIdentifierEntities(any(), anySet()))
        .thenThrow(new RuntimeException("entity service down"));

    List<EdgeDecisionRuleScopeCondition> result = resolver.resolveApiScope(requestContext, scope);

    assertTrue(result.isEmpty());
  }

  @Test
  void resolveApiScope_emptyHttpMethodAndService_onlyReturnsUrlRegex() {
    AbuseApiScope scope =
        AbuseApiScope.newBuilder().setApiIds(AbuseApiIds.newBuilder().addIds("api-1")).build();

    when(cachedApiMappingProvider.getApiIdentifierEntities(any(), anySet()))
        .thenReturn(
            Map.of(
                "api-1",
                Optional.of(
                    new ApiIdentifierEntity(
                        "api-1",
                        "name",
                        "/v1/users",
                        List.of("/v1/users.*"),
                        Collections.emptyList(),
                        "",
                        ""))));

    List<EdgeDecisionRuleScopeCondition> result = resolver.resolveApiScope(requestContext, scope);

    assertEquals(1, result.size());
    assertTrue(result.get(0).hasUrlRegexScope());
  }

  @Test
  void resolveApiScope_labelsResolveToNoApis_returnsEmpty() {
    AbuseApiScope scope =
        AbuseApiScope.newBuilder()
            .setApiLabels(AbuseApiLabels.newBuilder().addLabels("UnusedLabel"))
            .build();

    when(cachedApiMappingProvider.getApiIdentifierEntitiesHavingLabels(any(), anySet()))
        .thenReturn(Map.of("UnusedLabel", Set.of()));

    List<EdgeDecisionRuleScopeCondition> result = resolver.resolveApiScope(requestContext, scope);

    assertTrue(result.isEmpty());
    verify(cachedApiMappingProvider, never()).getApiIdentifierEntities(any(), anySet());
  }
}
