package ai.traceable.edge.config.service.supplier;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.edge.config.service.config.TraceableEdgeConfig;
import ai.traceable.edge.config.service.v1.AgentCapabilities;
import ai.traceable.edge.config.service.v1.ApiSecurityScheme;
import ai.traceable.edge.config.service.v1.ConfigRequestElement;
import ai.traceable.edge.config.service.v1.ConfigResponseElement;
import ai.traceable.edge.config.service.v1.SecuritySchemeConfig;
import ai.traceable.entity.fetcher.cache.SecuritySchemeProvider.UserRoleSecurityScheme;
import ai.traceable.entity.fetcher.cache.SecuritySchemeProvider.UserScopeSecurityScheme;
import ai.traceable.entity.fetcher.cache.StreamingSecuritySchemeProvider;
import ai.traceable.entity.fetcher.cache.StreamingSecuritySchemeProvider.ApiSecuritySchemeDetails;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Duration;
import java.util.Set;
import java.util.stream.Stream;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SecuritySchemeConfigSupplierTest {
  private static final String TEST_ENV = "test-env";

  @Mock private RequestContext requestContext;
  @Mock private ConfigRequestElement requestElement;
  @Mock private StreamingSecuritySchemeProvider securitySchemeProvider;
  @Mock private TraceableEdgeConfig config;

  private SecuritySchemeConfigSupplier supplier;

  @BeforeEach
  void setUp() {
    supplier =
        new SecuritySchemeConfigSupplier(securitySchemeProvider, config, new UuidGenerator());
  }

  @Test
  void testGetConfigs_emptySecuritySchemes_returnsEmptyPayload() {
    AgentCapabilities agentCapabilities =
        AgentCapabilities.newBuilder()
            .putAllAdditionalFields(ImmutableMap.of("serviceName", "test-service"))
            .build();
    when(config.getAgentPollingFrequency("SecuritySchemeConfig"))
        .thenReturn(Duration.newBuilder().setSeconds(600).build());

    when(securitySchemeProvider.getAllSecuritySchemes(
            eq(requestContext), eq("test-service"), any()))
        .thenReturn(Stream.empty());
    ConfigResponseElement response =
        supplier.getConfigs(requestContext, TEST_ENV, requestElement, agentCapabilities);
    assertNotNull(response);
    assertEquals("SecuritySchemeConfig", response.getConfigType());
    assertTrue(response.getConfigPayloads().getConfigBytesList().isEmpty());
  }

  @Test
  void testGetConfigs_withRolesAndScopes_buildsCorrectData() throws Exception {
    AgentCapabilities agentCapabilities =
        AgentCapabilities.newBuilder()
            .putAllAdditionalFields(ImmutableMap.of("serviceName", "test-service"))
            .build();
    when(config.getAgentPollingFrequency("SecuritySchemeConfig"))
        .thenReturn(Duration.newBuilder().setSeconds(600).build());

    Set<UserRoleSecurityScheme> roles =
        Set.of(
            new UserRoleSecurityScheme("role-1", "Admin", true),
            new UserRoleSecurityScheme("role-2", "User", false));

    Set<UserScopeSecurityScheme> scopes =
        Set.of(new UserScopeSecurityScheme("scope-1", "read:users", true));

    ApiSecuritySchemeDetails apiScheme = new ApiSecuritySchemeDetails("api-123", roles, scopes);

    when(securitySchemeProvider.getAllSecuritySchemes(
            eq(requestContext), eq("test-service"), any()))
        .thenReturn(Stream.of(apiScheme));
    ConfigResponseElement response =
        supplier.getConfigs(requestContext, TEST_ENV, requestElement, agentCapabilities);
    assertNotNull(response);
    assertEquals("SecuritySchemeConfig", response.getConfigType());

    SecuritySchemeConfig data =
        SecuritySchemeConfig.parseFrom(response.getConfigPayloads().getConfigBytes(0));

    assertTrue(data.containsApiSecuritySchemes("api-123"));
    ApiSecurityScheme details = data.getApiSecuritySchemesMap().get("api-123");
    assertEquals(2, details.getUserRolesCount());
    assertEquals(1, details.getUserScopesCount());

    assertTrue(details.getUserRolesList().stream().anyMatch(r -> r.getRoleId().equals("role-1")));
    assertTrue(details.getUserRolesList().stream().anyMatch(r -> r.getRoleName().equals("Admin")));

    assertTrue(
        details.getUserScopesList().stream().anyMatch(s -> s.getScopeId().equals("scope-1")));
    assertTrue(
        details.getUserScopesList().stream().anyMatch(s -> s.getScopeName().equals("read:users")));
  }

  @Test
  void testGetConfigs_multipleApis_buildsCorrectMapping() throws Exception {
    AgentCapabilities agentCapabilities =
        AgentCapabilities.newBuilder()
            .putAllAdditionalFields(ImmutableMap.of("serviceName", "test-service"))
            .build();
    when(config.getAgentPollingFrequency("SecuritySchemeConfig"))
        .thenReturn(Duration.newBuilder().setSeconds(600).build());

    ApiSecuritySchemeDetails api1 =
        new ApiSecuritySchemeDetails(
            "api-1", Set.of(new UserRoleSecurityScheme("role-1", "Admin", true)), Set.of());

    ApiSecuritySchemeDetails api2 =
        new ApiSecuritySchemeDetails(
            "api-2", Set.of(), Set.of(new UserScopeSecurityScheme("scope-1", "read:data", false)));

    when(securitySchemeProvider.getAllSecuritySchemes(
            eq(requestContext), eq("test-service"), any()))
        .thenReturn(Stream.of(api1, api2));

    ConfigResponseElement response =
        supplier.getConfigs(requestContext, TEST_ENV, requestElement, agentCapabilities);

    SecuritySchemeConfig data =
        SecuritySchemeConfig.parseFrom(response.getConfigPayloads().getConfigBytes(0));

    assertEquals(2, data.getApiSecuritySchemesMap().size());
    assertTrue(data.containsApiSecuritySchemes("api-1"));
    assertTrue(data.containsApiSecuritySchemes("api-2"));

    ApiSecurityScheme details1 = data.getApiSecuritySchemesMap().get("api-1");
    assertEquals(1, details1.getUserRolesCount());
    assertEquals(0, details1.getUserScopesCount());

    ApiSecurityScheme details2 = data.getApiSecuritySchemesMap().get("api-2");
    assertEquals(0, details2.getUserRolesCount());
    assertEquals(1, details2.getUserScopesCount());
  }

  @Test
  void testGetConfigs_missingServiceName_throwsException() {
    AgentCapabilities agentCapabilities = AgentCapabilities.newBuilder().build();
    assertThrows(
        Exception.class,
        () -> supplier.getConfigs(requestContext, TEST_ENV, requestElement, agentCapabilities));
  }

  @Test
  void testGetConfigType_returnsCorrectType() {
    assertEquals("SecuritySchemeConfig", supplier.getConfigType());
  }

  @Test
  void testGetConfigs_onlyRoles_buildsCorrectData() throws Exception {
    AgentCapabilities agentCapabilities =
        AgentCapabilities.newBuilder()
            .putAllAdditionalFields(ImmutableMap.of("serviceName", "test-service"))
            .build();
    when(config.getAgentPollingFrequency("SecuritySchemeConfig"))
        .thenReturn(Duration.newBuilder().setSeconds(600).build());

    Set<UserRoleSecurityScheme> roles = Set.of(new UserRoleSecurityScheme("role-1", "Admin", true));

    ApiSecuritySchemeDetails apiScheme = new ApiSecuritySchemeDetails("api-456", roles, Set.of());

    when(securitySchemeProvider.getAllSecuritySchemes(
            eq(requestContext), eq("test-service"), any()))
        .thenReturn(Stream.of(apiScheme));

    ConfigResponseElement response =
        supplier.getConfigs(requestContext, TEST_ENV, requestElement, agentCapabilities);
    SecuritySchemeConfig data =
        SecuritySchemeConfig.parseFrom(response.getConfigPayloads().getConfigBytes(0));

    assertTrue(data.containsApiSecuritySchemes("api-456"));
    ApiSecurityScheme details = data.getApiSecuritySchemesMap().get("api-456");
    assertEquals(1, details.getUserRolesCount());
    assertEquals(0, details.getUserScopesCount());
  }

  @Test
  void testGetConfigs_onlyScopes_buildsCorrectData() throws Exception {
    AgentCapabilities agentCapabilities =
        AgentCapabilities.newBuilder()
            .putAllAdditionalFields(ImmutableMap.of("serviceName", "test-service"))
            .build();
    when(config.getAgentPollingFrequency("SecuritySchemeConfig"))
        .thenReturn(Duration.newBuilder().setSeconds(600).build());

    Set<UserScopeSecurityScheme> scopes =
        Set.of(new UserScopeSecurityScheme("scope-1", "write:users", false));

    ApiSecuritySchemeDetails apiScheme = new ApiSecuritySchemeDetails("api-789", Set.of(), scopes);

    when(securitySchemeProvider.getAllSecuritySchemes(
            eq(requestContext), eq("test-service"), any()))
        .thenReturn(Stream.of(apiScheme));
    ConfigResponseElement response =
        supplier.getConfigs(requestContext, TEST_ENV, requestElement, agentCapabilities);
    SecuritySchemeConfig data =
        SecuritySchemeConfig.parseFrom(response.getConfigPayloads().getConfigBytes(0));

    assertTrue(data.containsApiSecuritySchemes("api-789"));
    ApiSecurityScheme details = data.getApiSecuritySchemesMap().get("api-789");
    assertEquals(0, details.getUserRolesCount());
    assertEquals(1, details.getUserScopesCount());
  }
}
