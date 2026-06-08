package ai.traceable.edge.config.service.supplier.api.resolution.url.pattern.trie;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.edge.config.service.config.TraceableEdgeConfig;
import ai.traceable.edge.config.service.v1.AgentCapabilities;
import ai.traceable.edge.config.service.v1.ApiIdResolverData;
import ai.traceable.edge.config.service.v1.ConfigRequestElement;
import ai.traceable.edge.config.service.v1.ConfigResponseElement;
import ai.traceable.edge.config.service.v1.HttpDetail;
import ai.traceable.edge.config.service.v1.SegmentType;
import ai.traceable.edge.config.service.v1.UrlSegmentTrieNode;
import ai.traceable.entity.fetcher.cache.StreamingApiMappingProvider;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ApiIdResolverConfigSupplierTest {
  private static final String TEST_ENV = "test-env";

  @Mock private RequestContext requestContext;
  @Mock private ConfigRequestElement requestElement;
  @Mock private StreamingApiMappingProvider apiMappingProvider;
  @Mock private TraceableEdgeConfig config;
  @Mock private FeatureCachingClient featureCachingClient;

  private ApiIdResolverConfigSupplier supplier;

  @BeforeEach
  void setUp() {
    when(config.getAgentPollingFrequency("ApiIdResolverConfig"))
        .thenReturn(com.google.protobuf.Duration.newBuilder().setSeconds(600).build());
    lenient()
        .when(featureCachingClient.isProtectionEngineApiIdResolverConfigEnabledForTenant(any()))
        .thenReturn(true);
    supplier =
        new ApiIdResolverConfigSupplier(
            new ObjectMapper(),
            apiMappingProvider,
            config,
            new UuidGenerator(),
            featureCachingClient);
  }

  @Test
  void testGetConfigs_featureFlagDisabled_returnsEmptyPayload() {
    when(featureCachingClient.isProtectionEngineApiIdResolverConfigEnabledForTenant(any()))
        .thenReturn(false);
    AgentCapabilities agentCapabilities =
        AgentCapabilities.newBuilder()
            .putAllAdditionalFields(
                ImmutableMap.of("serviceName", "test-service", "apiType", "API_TYPE_HTTP"))
            .build();

    ConfigResponseElement response =
        supplier.getConfigs(requestContext, TEST_ENV, requestElement, agentCapabilities);

    assertEquals("ApiIdResolverConfig", response.getConfigType());
    assertTrue(response.getConfigPayloads().getConfigBytesList().isEmpty());
  }

  @Test
  void testGetConfigs_emptyApiEndpoints_returnsEmptyArray() throws Exception {
    // Arrange
    AgentCapabilities agentCapabilities =
        AgentCapabilities.newBuilder()
            .putAllAdditionalFields(
                ImmutableMap.of("serviceName", "test-service", "apiType", "API_TYPE_HTTP"))
            .build();

    when(apiMappingProvider.getAllHttpApiDetails(eq(requestContext), eq("test-service"), any()))
        .thenReturn(Stream.empty());

    // Act
    ConfigResponseElement response =
        supplier.getConfigs(requestContext, TEST_ENV, requestElement, agentCapabilities);

    // Assert
    assertNotNull(response);
    assertEquals("ApiIdResolverConfig", response.getConfigType());
    assertTrue(response.getConfigPayloads().getConfigBytesList().isEmpty());
  }

  @Test
  void testGetConfigs_singleLiteralPath_buildsCorrectTrie() throws Exception {
    // Arrange
    AgentCapabilities agentCapabilities =
        AgentCapabilities.newBuilder()
            .putAllAdditionalFields(
                ImmutableMap.of("serviceName", "test-service", "apiType", "API_TYPE_HTTP"))
            .build();

    StreamingApiMappingProvider.HttpApiDetails httpApiDetails =
        new StreamingApiMappingProvider.HttpApiDetails(
            "api-123", "GET", Collections.singletonList("/users"));
    when(apiMappingProvider.getAllHttpApiDetails(eq(requestContext), eq("test-service"), any()))
        .thenReturn(Stream.of(httpApiDetails));

    // Act
    ConfigResponseElement response =
        supplier.getConfigs(requestContext, TEST_ENV, requestElement, agentCapabilities);

    // Assert
    assertNotNull(response);
    ApiIdResolverData data =
        ApiIdResolverData.parseFrom(response.getConfigPayloads().getConfigBytes(0));
    List<UrlSegmentTrieNode> topLevel = data.getHttpApiIdResolverData().getTrieList();
    assertEquals(1, topLevel.size());
    UrlSegmentTrieNode userNode = topLevel.get(0);
    assertEquals("users", userNode.getKey());
    assertEquals(SegmentType.SEGMENT_TYPE_LITERAL, userNode.getType());
    assertEquals(0, userNode.getChildrenCount());
    String apiIdForGet = apiIdForMethod(userNode, "GET");
    assertEquals("api-123", apiIdForGet);
  }

  @Test
  void testGetConfigs_pathWithRegexPattern_buildsCorrectTrie() throws Exception {
    // Arrange
    AgentCapabilities agentCapabilities =
        AgentCapabilities.newBuilder()
            .putAllAdditionalFields(
                ImmutableMap.of("serviceName", "test-service", "apiType", "API_TYPE_HTTP"))
            .build();

    StreamingApiMappingProvider.HttpApiDetails httpApiDetails =
        new StreamingApiMappingProvider.HttpApiDetails(
            "api-456", "GET", Collections.singletonList("/users/\\d+"));
    when(apiMappingProvider.getAllHttpApiDetails(eq(requestContext), eq("test-service"), any()))
        .thenReturn(Stream.of(httpApiDetails));

    // Act
    ConfigResponseElement response =
        supplier.getConfigs(requestContext, TEST_ENV, requestElement, agentCapabilities);

    // Assert
    ApiIdResolverData data =
        ApiIdResolverData.parseFrom(response.getConfigPayloads().getConfigBytes(0));
    List<UrlSegmentTrieNode> topLevel = data.getHttpApiIdResolverData().getTrieList();
    assertEquals(1, topLevel.size());
    UrlSegmentTrieNode userNode = topLevel.get(0);
    assertEquals("users", userNode.getKey());
    assertEquals(SegmentType.SEGMENT_TYPE_LITERAL, userNode.getType());
    assertEquals(0, userNode.getApiDetails().getHttpDetails().getHttpDetailsCount());
    assertEquals(1, userNode.getChildrenCount());
    UrlSegmentTrieNode idNode = userNode.getChildren(0);
    assertEquals("\\d+", idNode.getKey());
    assertEquals(SegmentType.SEGMENT_TYPE_PATTERN, idNode.getType());
    String getApiId = apiIdForMethod(idNode, "GET");
    assertEquals("api-456", getApiId);
  }

  @Test
  void testGetConfigs_complexUuidPattern_buildsCorrectTrie() throws Exception {
    // Arrange
    AgentCapabilities agentCapabilities =
        AgentCapabilities.newBuilder()
            .putAllAdditionalFields(
                ImmutableMap.of("serviceName", "test-service", "apiType", "API_TYPE_HTTP"))
            .build();

    String uuidPattern =
        "/user/.*((\\{){0,1}[0-9a-fA-F]{8}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{12}(\\}){0,1}).*|\\d+|.*\\d.*\\d.*\\d.*\\d.*\\d.*/order";
    StreamingApiMappingProvider.HttpApiDetails httpApiDetails =
        new StreamingApiMappingProvider.HttpApiDetails(
            "api-789", "GET", Collections.singletonList(uuidPattern));
    when(apiMappingProvider.getAllHttpApiDetails(eq(requestContext), eq("test-service"), any()))
        .thenReturn(Stream.of(httpApiDetails));

    // Act
    ConfigResponseElement response =
        supplier.getConfigs(requestContext, TEST_ENV, requestElement, agentCapabilities);

    // Assert
    ApiIdResolverData data =
        ApiIdResolverData.parseFrom(response.getConfigPayloads().getConfigBytes(0));
    List<UrlSegmentTrieNode> topLevel = data.getHttpApiIdResolverData().getTrieList();
    assertEquals(1, topLevel.size());
    UrlSegmentTrieNode userNode = topLevel.get(0);
    assertEquals("user", userNode.getKey());
    assertEquals(SegmentType.SEGMENT_TYPE_LITERAL, userNode.getType());
    assertEquals(1, userNode.getChildrenCount());
    UrlSegmentTrieNode patternNode = userNode.getChildren(0);
    assertEquals(SegmentType.SEGMENT_TYPE_PATTERN, patternNode.getType());
    assertTrue(patternNode.getKey().contains(".*"));
  }

  @Test
  void testGetConfigs_multipleEndpoints_buildsSharedTrie() throws Exception {
    // Arrange
    AgentCapabilities agentCapabilities =
        AgentCapabilities.newBuilder()
            .putAllAdditionalFields(
                ImmutableMap.of("serviceName", "test-service", "apiType", "API_TYPE_HTTP"))
            .build();

    StreamingApiMappingProvider.HttpApiDetails api1 =
        new StreamingApiMappingProvider.HttpApiDetails(
            "api-1", "GET", Collections.singletonList("/users/\\d+"));
    StreamingApiMappingProvider.HttpApiDetails api2 =
        new StreamingApiMappingProvider.HttpApiDetails(
            "api-2", "GET", Collections.singletonList("/users/profile"));
    StreamingApiMappingProvider.HttpApiDetails api3 =
        new StreamingApiMappingProvider.HttpApiDetails(
            "api-3", "GET", Collections.singletonList("/orders/\\d+"));

    when(apiMappingProvider.getAllHttpApiDetails(eq(requestContext), eq("test-service"), any()))
        .thenReturn(Stream.of(api1, api2, api3));

    // Act
    ConfigResponseElement response =
        supplier.getConfigs(requestContext, TEST_ENV, requestElement, agentCapabilities);

    // Assert
    ApiIdResolverData data =
        ApiIdResolverData.parseFrom(response.getConfigPayloads().getConfigBytes(0));
    List<UrlSegmentTrieNode> topLevel = data.getHttpApiIdResolverData().getTrieList();
    assertEquals(2, topLevel.size());

    // Find users node
    UrlSegmentTrieNode usersNode =
        topLevel.stream().filter(node -> "users".equals(node.getKey())).findFirst().orElseThrow();

    List<UrlSegmentTrieNode> usersChildren = usersNode.getChildrenList();
    assertEquals(2, usersChildren.size()); // \d+ and profile

    // Verify pattern and literal children exist
    boolean hasPatternChild =
        usersChildren.stream()
            .anyMatch(
                child ->
                    child.getType() == SegmentType.SEGMENT_TYPE_PATTERN
                        && "\\d+".equals(child.getKey()));
    boolean hasLiteralChild =
        usersChildren.stream()
            .anyMatch(
                child ->
                    child.getType() == SegmentType.SEGMENT_TYPE_LITERAL
                        && "profile".equals(child.getKey()));

    assertTrue(hasPatternChild);
    assertTrue(hasLiteralChild);
  }

  @Test
  void testGetConfigs_rootPath_setsMethodsOnRoot() throws Exception {
    // Arrange
    AgentCapabilities agentCapabilities =
        AgentCapabilities.newBuilder()
            .putAllAdditionalFields(
                ImmutableMap.of("serviceName", "test-service", "apiType", "API_TYPE_HTTP"))
            .build();

    StreamingApiMappingProvider.HttpApiDetails httpApiDetails =
        new StreamingApiMappingProvider.HttpApiDetails(
            "root-api", "GET", Collections.singletonList("/"));
    when(apiMappingProvider.getAllHttpApiDetails(eq(requestContext), eq("test-service"), any()))
        .thenReturn(Stream.of(httpApiDetails));

    // Act
    ConfigResponseElement response =
        supplier.getConfigs(requestContext, TEST_ENV, requestElement, agentCapabilities);

    // Assert
    ApiIdResolverData data =
        ApiIdResolverData.parseFrom(response.getConfigPayloads().getConfigBytes(0));
    // Root path should create a "/" top-level node with the methods map
    List<UrlSegmentTrieNode> topLevel = data.getHttpApiIdResolverData().getTrieList();
    assertEquals(1, topLevel.size());
    UrlSegmentTrieNode rootPathNode = topLevel.get(0);
    assertEquals("/", rootPathNode.getKey());
    assertEquals(SegmentType.SEGMENT_TYPE_LITERAL, rootPathNode.getType());
    String rootGetApiId = apiIdForMethod(rootPathNode, "GET");
    assertEquals("root-api", rootGetApiId);
    assertEquals(0, rootPathNode.getChildrenCount());
  }

  @Test
  void testBuildUrlSegmentTrieFromPattern_emptyPattern_setsRootMethods() throws Exception {
    // This tests the private method indirectly through trie construction
    AgentCapabilities agentCapabilities =
        AgentCapabilities.newBuilder()
            .putAllAdditionalFields(
                ImmutableMap.of("serviceName", "test-service", "apiType", "API_TYPE_HTTP"))
            .build();

    // I am treating an empty pattern as a root path
    StreamingApiMappingProvider.HttpApiDetails httpApiDetails =
        new StreamingApiMappingProvider.HttpApiDetails(
            "empty-api", "GET", Collections.singletonList(""));
    when(apiMappingProvider.getAllHttpApiDetails(eq(requestContext), eq("test-service"), any()))
        .thenReturn(Stream.of(httpApiDetails));

    // Should not throw exception
    ConfigResponseElement response =
        assertDoesNotThrow(
            () -> supplier.getConfigs(requestContext, TEST_ENV, requestElement, agentCapabilities));
    ApiIdResolverData data =
        ApiIdResolverData.parseFrom(response.getConfigPayloads().getConfigBytes(0));
    // Root path should create a "/" top-level node with the methods map
    List<UrlSegmentTrieNode> topLevel = data.getHttpApiIdResolverData().getTrieList();
    assertEquals(1, topLevel.size());
    UrlSegmentTrieNode rootPathNode = topLevel.get(0);
    assertEquals("/", rootPathNode.getKey());
    assertEquals(SegmentType.SEGMENT_TYPE_LITERAL, rootPathNode.getType());
    String emptyGetApiId = apiIdForMethod(rootPathNode, "GET");
    assertEquals("empty-api", emptyGetApiId);
    assertEquals(0, rootPathNode.getChildrenCount());
  }

  @Test
  void testIsRegexPattern_detectsPatterns() throws Exception {
    // Test through actual trie building since isRegexPattern is private
    AgentCapabilities agentCapabilities =
        AgentCapabilities.newBuilder()
            .putAllAdditionalFields(
                ImmutableMap.of("serviceName", "test-service", "apiType", "API_TYPE_HTTP"))
            .build();

    StreamingApiMappingProvider.HttpApiDetails[] patterns = {
      new StreamingApiMappingProvider.HttpApiDetails(
          "wildcard", "GET", Collections.singletonList("/path/.*")),
      new StreamingApiMappingProvider.HttpApiDetails(
          "digit", "GET", Collections.singletonList("/path/\\d+")),
      new StreamingApiMappingProvider.HttpApiDetails(
          "bracket", "GET", Collections.singletonList("/path/[0-9]+")),
      new StreamingApiMappingProvider.HttpApiDetails(
          "group", "GET", Collections.singletonList("/path/(a|b)")),
      new StreamingApiMappingProvider.HttpApiDetails(
          "literal", "GET", Collections.singletonList("/path/literal")),
      new StreamingApiMappingProvider.HttpApiDetails(
          "literal", "GET", Collections.singletonList("/path/{id}"))
    };

    when(apiMappingProvider.getAllHttpApiDetails(eq(requestContext), eq("test-service"), any()))
        .thenReturn(Stream.of(patterns));

    ConfigResponseElement response =
        supplier.getConfigs(requestContext, TEST_ENV, requestElement, agentCapabilities);
    ApiIdResolverData data =
        ApiIdResolverData.parseFrom(response.getConfigPayloads().getConfigBytes(0));

    // Verify we have path node with children
    UrlSegmentTrieNode pathNode =
        data.getHttpApiIdResolverData().getTrieList().stream()
            .filter(node -> "path".equals(node.getKey()))
            .findFirst()
            .orElseThrow();
    List<UrlSegmentTrieNode> children = pathNode.getChildrenList();
    boolean hasWildcard = children.stream().anyMatch(child -> ".*".equals(child.getKey()));
    boolean hasDigit = children.stream().anyMatch(child -> "\\d+".equals(child.getKey()));
    boolean hasBracket = children.stream().anyMatch(child -> "[0-9]+".equals(child.getKey()));
    boolean hasGroup = children.stream().anyMatch(child -> "(a|b)".equals(child.getKey()));
    boolean hasLiteral = children.stream().anyMatch(child -> "literal".equals(child.getKey()));
    assertTrue(hasWildcard && hasDigit && hasBracket && hasGroup && hasLiteral);

    // Count pattern vs literal types
    long patternCount =
        children.stream().filter(c -> c.getType() == SegmentType.SEGMENT_TYPE_PATTERN).count();
    long literalCount =
        children.stream().filter(c -> c.getType() == SegmentType.SEGMENT_TYPE_LITERAL).count();

    assertEquals(4, patternCount); // .*, \d+, [0-9]+, (a|b)
    assertEquals(2, literalCount); // {id}, literal
  }

  @Test
  void testGetConfigs_jsonSerializationFormat() throws Exception {
    // Arrange
    AgentCapabilities agentCapabilities =
        AgentCapabilities.newBuilder()
            .putAllAdditionalFields(
                ImmutableMap.of("serviceName", "test-service", "apiType", "API_TYPE_HTTP"))
            .build();

    StreamingApiMappingProvider.HttpApiDetails httpApiDetails =
        new StreamingApiMappingProvider.HttpApiDetails(
            "api-test", "GET", Collections.singletonList("/users/\\d+/profile"));
    when(apiMappingProvider.getAllHttpApiDetails(eq(requestContext), eq("test-service"), any()))
        .thenReturn(Stream.of(httpApiDetails));

    // Act
    ConfigResponseElement response =
        supplier.getConfigs(requestContext, TEST_ENV, requestElement, agentCapabilities);

    // Assert proto structure
    ApiIdResolverData data =
        ApiIdResolverData.parseFrom(response.getConfigPayloads().getConfigBytes(0));
    List<UrlSegmentTrieNode> topLevel = data.getHttpApiIdResolverData().getTrieList();
    assertEquals(1, topLevel.size());
    UrlSegmentTrieNode node = topLevel.get(0);
    assertNotNull(node.getKey());
    assertNotNull(node.getType());
    assertNotNull(node.getChildrenList());
    assertNotNull(node.getApiDetails().getHttpDetails().getHttpDetailsList());
  }

  @Test
  void testConfigResponseElementFormat() {
    // Arrange
    AgentCapabilities agentCapabilities =
        AgentCapabilities.newBuilder()
            .putAllAdditionalFields(
                ImmutableMap.of("serviceName", "test-service", "apiType", "API_TYPE_HTTP"))
            .build();

    when(apiMappingProvider.getAllHttpApiDetails(eq(requestContext), eq("test-service"), any()))
        .thenReturn(Stream.empty());

    // Act
    ConfigResponseElement response =
        supplier.getConfigs(requestContext, TEST_ENV, requestElement, agentCapabilities);

    // Assert
    assertEquals("ApiIdResolverConfig", response.getConfigType());
    assertTrue(response.hasConfigPayloads());
    assertEquals(0, response.getConfigPayloads().getConfigBytesCount());
  }

  @Test
  void testGetConfigs_nestedPaths_buildsDeepTrie() throws Exception {
    // Arrange
    AgentCapabilities agentCapabilities =
        AgentCapabilities.newBuilder()
            .putAllAdditionalFields(
                ImmutableMap.of("serviceName", "test-service", "apiType", "API_TYPE_HTTP"))
            .build();

    StreamingApiMappingProvider.HttpApiDetails httpApiDetails =
        new StreamingApiMappingProvider.HttpApiDetails(
            "deep-api", "GET", Collections.singletonList("/api/v1/users/\\d+/orders/.*"));
    when(apiMappingProvider.getAllHttpApiDetails(eq(requestContext), eq("test-service"), any()))
        .thenReturn(Stream.of(httpApiDetails));

    // Act
    ConfigResponseElement response =
        supplier.getConfigs(requestContext, TEST_ENV, requestElement, agentCapabilities);

    // Assert
    ApiIdResolverData data =
        ApiIdResolverData.parseFrom(response.getConfigPayloads().getConfigBytes(0));

    // Navigate through the nested structure
    List<UrlSegmentTrieNode> topLevel = data.getHttpApiIdResolverData().getTrieList();
    assertEquals(1, topLevel.size());
    UrlSegmentTrieNode apiNode = topLevel.get(0);
    assertEquals("api", apiNode.getKey());

    assertEquals(1, apiNode.getChildrenCount());
    assertEquals("v1", apiNode.getChildren(0).getKey());

    // Should be able to navigate all the way down to the leaf
    UrlSegmentTrieNode currentNode = apiNode.getChildren(0);
    String[] expectedPath = {"users", "\\d+", "orders", ".*"};
    SegmentType[] expectedTypes = {
      SegmentType.SEGMENT_TYPE_LITERAL,
      SegmentType.SEGMENT_TYPE_PATTERN,
      SegmentType.SEGMENT_TYPE_LITERAL,
      SegmentType.SEGMENT_TYPE_PATTERN
    };

    for (int i = 0; i < expectedPath.length; i++) {
      assertEquals(1, currentNode.getChildrenCount());
      currentNode = currentNode.getChildren(0);
      assertEquals(expectedPath[i], currentNode.getKey());
      assertEquals(expectedTypes[i], currentNode.getType());
    }

    // Final node should have an HttpDetail entry with GET -> deep-api
    String deepApiGet = apiIdForMethod(currentNode, "GET");
    assertEquals("deep-api", deepApiGet);
  }

  @Test
  void testGetConfigs_duplicatePaths_lastWriteWinsForSameMethod() throws Exception {
    // we don't expect to get duplicate paths but in case we do, this is how we handle it

    // Arrange
    AgentCapabilities agentCapabilities =
        AgentCapabilities.newBuilder()
            .putAllAdditionalFields(
                ImmutableMap.of("serviceName", "test-service", "apiType", "API_TYPE_HTTP"))
            .build();

    StreamingApiMappingProvider.HttpApiDetails api1 =
        new StreamingApiMappingProvider.HttpApiDetails(
            "api-1", "GET", Collections.singletonList("/users/\\d+"));
    StreamingApiMappingProvider.HttpApiDetails api2 =
        new StreamingApiMappingProvider.HttpApiDetails(
            "api-2", "GET", Collections.singletonList("/users/\\d+")); // Same pattern, different ID

    when(apiMappingProvider.getAllHttpApiDetails(eq(requestContext), eq("test-service"), any()))
        .thenReturn(Stream.of(api1, api2));

    // Act
    ConfigResponseElement response =
        supplier.getConfigs(requestContext, TEST_ENV, requestElement, agentCapabilities);

    // Assert
    ApiIdResolverData data =
        ApiIdResolverData.parseFrom(response.getConfigPayloads().getConfigBytes(0));
    List<UrlSegmentTrieNode> topLevel = data.getHttpApiIdResolverData().getTrieList();
    assertEquals(1, topLevel.size());
    UrlSegmentTrieNode usersNode = topLevel.get(0);
    List<UrlSegmentTrieNode> children = usersNode.getChildrenList();
    assertEquals(1, children.size()); // Should share the same node

    UrlSegmentTrieNode patternNode = children.get(0);
    // For the same method, last write wins: only one detail for GET should exist and be api-2
    long getCount =
        patternNode.getApiDetails().getHttpDetails().getHttpDetailsList().stream()
            .filter(d -> d.getHttpMethod().equals("GET"))
            .count();
    assertEquals(1, getCount);
    String apiId = apiIdForMethod(patternNode, "GET");
    assertEquals("api-2", apiId);
  }

  private String apiIdForMethod(UrlSegmentTrieNode node, String method) {
    return node.getApiDetails().getHttpDetails().getHttpDetailsList().stream()
        .filter(d -> d.getHttpMethod().equals(method))
        .map(HttpDetail::getApiId)
        .findFirst()
        .orElse(null);
  }
}
