package ai.traceable.edge.config.service.supplier;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.edge.config.service.config.TraceableEdgeConfig;
import ai.traceable.edge.config.service.v1.AgentCapabilities;
import ai.traceable.edge.config.service.v1.AiEndpointMetadataConfig;
import ai.traceable.edge.config.service.v1.ConfigRequestElement;
import ai.traceable.edge.config.service.v1.ConfigResponseElement;
import ai.traceable.entity.fetcher.cache.StreamingAiEndpointMetadataProvider;
import ai.traceable.entity.fetcher.cache.StreamingAiEndpointMetadataProvider.ApiAiEndpointMetadataDetails;
import ai.traceable.protection.data.context.v1.AiEndpointMetadata;
import ai.traceable.protection.data.context.v1.PromptAttributeKey;
import ai.traceable.protection.processing.common.v1.AttributeType;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import java.util.stream.Stream;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AiEndpointMetadataConfigSupplierTest {
  private static final String TEST_ENV = "test-env";

  @Mock private RequestContext requestContext;
  @Mock private ConfigRequestElement requestElement;
  @Mock private StreamingAiEndpointMetadataProvider aiEndpointMetadataProvider;
  @Mock private TraceableEdgeConfig config;
  @Mock private FeatureCachingClient featureCachingClient;

  private AiEndpointMetadataConfigSupplier supplier;

  @BeforeEach
  void setUp() {
    lenient()
        .when(featureCachingClient.isProtectionEngineAiAppProtectionEnabledForTenant(any()))
        .thenReturn(true);
    supplier =
        new AiEndpointMetadataConfigSupplier(
            aiEndpointMetadataProvider, config, new UuidGenerator(), featureCachingClient);
  }

  @Test
  void testGetConfigs_emptyMetadata_returnsEmptyPayload() {
    AgentCapabilities agentCapabilities =
        AgentCapabilities.newBuilder()
            .putAllAdditionalFields(ImmutableMap.of("serviceName", "test-service"))
            .build();
    when(config.getAgentPollingFrequency("AiEndpointMetadataConfig"))
        .thenReturn(Duration.newBuilder().setSeconds(600).build());

    when(aiEndpointMetadataProvider.getAllAiEndpointMetadata(
            eq(requestContext), eq("test-service"), any()))
        .thenReturn(Stream.empty());
    ConfigResponseElement response =
        supplier.getConfigs(requestContext, TEST_ENV, requestElement, agentCapabilities);
    assertNotNull(response);
    assertEquals("AiEndpointMetadataConfig", response.getConfigType());
    assertTrue(response.getConfigPayloads().getConfigBytesList().isEmpty());
  }

  @Test
  void testGetConfigs_withModelsAndPromptKeys_buildsCorrectData() throws Exception {
    AgentCapabilities agentCapabilities =
        AgentCapabilities.newBuilder()
            .putAllAdditionalFields(ImmutableMap.of("serviceName", "test-service"))
            .build();
    when(config.getAgentPollingFrequency("AiEndpointMetadataConfig"))
        .thenReturn(Duration.newBuilder().setSeconds(600).build());

    AiEndpointMetadata engineMetadata =
        AiEndpointMetadata.newBuilder()
            .addAssociatedAiModels("gpt-4")
            .addAssociatedAiModels("custom-model-v1")
            .addPromptAttributeKeys(
                PromptAttributeKey.newBuilder()
                    .setAttributeType(AttributeType.ATTRIBUTE_TYPE_BODY_PARAM)
                    .setAttributeKey("prompt")
                    .build())
            .build();

    ApiAiEndpointMetadataDetails apiMetadata =
        new ApiAiEndpointMetadataDetails("api-123", engineMetadata);

    when(aiEndpointMetadataProvider.getAllAiEndpointMetadata(
            eq(requestContext), eq("test-service"), any()))
        .thenReturn(Stream.of(apiMetadata));
    ConfigResponseElement response =
        supplier.getConfigs(requestContext, TEST_ENV, requestElement, agentCapabilities);
    assertNotNull(response);
    assertEquals("AiEndpointMetadataConfig", response.getConfigType());

    AiEndpointMetadataConfig data =
        AiEndpointMetadataConfig.parseFrom(response.getConfigPayloads().getConfigBytes(0));

    assertTrue(data.containsAiEndpointMetadata("api-123"));
    ByteString metadataBytes = data.getAiEndpointMetadataMap().get("api-123");
    AiEndpointMetadata details = AiEndpointMetadata.parseFrom(metadataBytes);
    assertEquals(2, details.getAssociatedAiModelsCount());
    assertEquals(1, details.getPromptAttributeKeysCount());

    assertTrue(details.getAssociatedAiModelsList().contains("gpt-4"));
    assertTrue(details.getAssociatedAiModelsList().contains("custom-model-v1"));

    PromptAttributeKey promptKey = details.getPromptAttributeKeys(0);
    assertEquals(AttributeType.ATTRIBUTE_TYPE_BODY_PARAM, promptKey.getAttributeType());
    assertEquals("prompt", promptKey.getAttributeKey());
  }

  @Test
  void testGetConfigs_multipleApis_buildsCorrectMapping() throws Exception {
    AgentCapabilities agentCapabilities =
        AgentCapabilities.newBuilder()
            .putAllAdditionalFields(ImmutableMap.of("serviceName", "test-service"))
            .build();
    when(config.getAgentPollingFrequency("AiEndpointMetadataConfig"))
        .thenReturn(Duration.newBuilder().setSeconds(600).build());

    ApiAiEndpointMetadataDetails api1 =
        new ApiAiEndpointMetadataDetails(
            "api-1", AiEndpointMetadata.newBuilder().addAssociatedAiModels("gpt-4").build());

    ApiAiEndpointMetadataDetails api2 =
        new ApiAiEndpointMetadataDetails(
            "api-2",
            AiEndpointMetadata.newBuilder()
                .addPromptAttributeKeys(
                    PromptAttributeKey.newBuilder()
                        .setAttributeType(AttributeType.ATTRIBUTE_TYPE_HEADER)
                        .setAttributeKey("x-prompt")
                        .build())
                .build());

    when(aiEndpointMetadataProvider.getAllAiEndpointMetadata(
            eq(requestContext), eq("test-service"), any()))
        .thenReturn(Stream.of(api1, api2));

    ConfigResponseElement response =
        supplier.getConfigs(requestContext, TEST_ENV, requestElement, agentCapabilities);

    AiEndpointMetadataConfig data =
        AiEndpointMetadataConfig.parseFrom(response.getConfigPayloads().getConfigBytes(0));

    assertEquals(2, data.getAiEndpointMetadataCount());
    assertTrue(data.containsAiEndpointMetadata("api-1"));
    assertTrue(data.containsAiEndpointMetadata("api-2"));

    AiEndpointMetadata details1 =
        AiEndpointMetadata.parseFrom(data.getAiEndpointMetadataMap().get("api-1"));
    assertEquals(1, details1.getAssociatedAiModelsCount());
    assertEquals(0, details1.getPromptAttributeKeysCount());

    AiEndpointMetadata details2 =
        AiEndpointMetadata.parseFrom(data.getAiEndpointMetadataMap().get("api-2"));
    assertEquals(0, details2.getAssociatedAiModelsCount());
    assertEquals(1, details2.getPromptAttributeKeysCount());
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
    assertEquals("AiEndpointMetadataConfig", supplier.getConfigType());
  }

  @Test
  void testGetConfigs_onlyModels_buildsCorrectData() throws Exception {
    AgentCapabilities agentCapabilities =
        AgentCapabilities.newBuilder()
            .putAllAdditionalFields(ImmutableMap.of("serviceName", "test-service"))
            .build();
    when(config.getAgentPollingFrequency("AiEndpointMetadataConfig"))
        .thenReturn(Duration.newBuilder().setSeconds(600).build());

    ApiAiEndpointMetadataDetails apiMetadata =
        new ApiAiEndpointMetadataDetails(
            "api-456", AiEndpointMetadata.newBuilder().addAssociatedAiModels("claude-3").build());

    when(aiEndpointMetadataProvider.getAllAiEndpointMetadata(
            eq(requestContext), eq("test-service"), any()))
        .thenReturn(Stream.of(apiMetadata));

    ConfigResponseElement response =
        supplier.getConfigs(requestContext, TEST_ENV, requestElement, agentCapabilities);
    AiEndpointMetadataConfig data =
        AiEndpointMetadataConfig.parseFrom(response.getConfigPayloads().getConfigBytes(0));

    assertTrue(data.containsAiEndpointMetadata("api-456"));
    AiEndpointMetadata details =
        AiEndpointMetadata.parseFrom(data.getAiEndpointMetadataMap().get("api-456"));
    assertEquals(1, details.getAssociatedAiModelsCount());
    assertEquals(0, details.getPromptAttributeKeysCount());
  }

  @Test
  void testGetConfigs_onlyPromptKeys_buildsCorrectData() throws Exception {
    AgentCapabilities agentCapabilities =
        AgentCapabilities.newBuilder()
            .putAllAdditionalFields(ImmutableMap.of("serviceName", "test-service"))
            .build();
    when(config.getAgentPollingFrequency("AiEndpointMetadataConfig"))
        .thenReturn(Duration.newBuilder().setSeconds(600).build());

    AiEndpointMetadata engineMetadata =
        AiEndpointMetadata.newBuilder()
            .addPromptAttributeKeys(
                PromptAttributeKey.newBuilder()
                    .setAttributeType(AttributeType.ATTRIBUTE_TYPE_BODY_PARAM)
                    .setAttributeKey("user_input.text")
                    .build())
            .addPromptAttributeKeys(
                PromptAttributeKey.newBuilder()
                    .setAttributeType(AttributeType.ATTRIBUTE_TYPE_BODY_PARAM)
                    .setAttributeKey("completion")
                    .build())
            .build();

    ApiAiEndpointMetadataDetails apiMetadata =
        new ApiAiEndpointMetadataDetails("api-789", engineMetadata);

    when(aiEndpointMetadataProvider.getAllAiEndpointMetadata(
            eq(requestContext), eq("test-service"), any()))
        .thenReturn(Stream.of(apiMetadata));
    ConfigResponseElement response =
        supplier.getConfigs(requestContext, TEST_ENV, requestElement, agentCapabilities);
    AiEndpointMetadataConfig data =
        AiEndpointMetadataConfig.parseFrom(response.getConfigPayloads().getConfigBytes(0));

    assertTrue(data.containsAiEndpointMetadata("api-789"));
    AiEndpointMetadata details =
        AiEndpointMetadata.parseFrom(data.getAiEndpointMetadataMap().get("api-789"));
    assertEquals(0, details.getAssociatedAiModelsCount());
    assertEquals(2, details.getPromptAttributeKeysCount());
  }
}
