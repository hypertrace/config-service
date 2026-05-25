package ai.traceable.edge.config.service.supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.edge.config.service.config.TraceableEdgeConfig;
import ai.traceable.edge.config.service.v1.AgentCapabilities;
import ai.traceable.edge.config.service.v1.ApiEndpointModelConfig;
import ai.traceable.edge.config.service.v1.ConfigPayloads;
import ai.traceable.edge.config.service.v1.ConfigRequestElement;
import ai.traceable.edge.config.service.v1.ConfigResponseElement;
import ai.traceable.entity.fetcher.cache.StreamingApiEndpointModelProvider;
import ai.traceable.entity.fetcher.cache.StreamingApiEndpointModelProvider.ApiEndpointModelDetails;
import ai.traceable.entity.fetcher.cache.StreamingApiEndpointModelProvider.ModelTypeFilter;
import ai.traceable.entity.fetcher.cache.config.ApiEndpointModelFetchConfig;
import ai.traceable.protection.data.context.v1.ApiDetectionModel;
import ai.traceable.protection.data.context.v1.HttpApiDetectionModel;
import ai.traceable.protection.processing.common.v1.HttpMethod;
import com.google.protobuf.Duration;
import io.grpc.StatusRuntimeException;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ApiEndpointModelConfigSupplierTest {

  private static final String TENANT_ID = "test-tenant";
  private static final String SERVICE_NAME = "test-service";
  private static final String ENVIRONMENT = "staging";
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);

  private StreamingApiEndpointModelProvider apiEndpointModelProvider;
  private TraceableEdgeConfig traceableEdgeConfig;
  private UuidGenerator uuidGenerator;
  private ApiEndpointModelFetchConfig fetchConfig;
  private ApiEndpointModelConfigSupplier supplier;

  @BeforeEach
  void setUp() {
    apiEndpointModelProvider = mock(StreamingApiEndpointModelProvider.class);
    traceableEdgeConfig = mock(TraceableEdgeConfig.class);
    uuidGenerator = mock(UuidGenerator.class);
    fetchConfig = new ApiEndpointModelFetchConfig(Collections.emptyList(), Collections.emptyList());

    when(traceableEdgeConfig.getAgentPollingFrequency("ApiEndpointModelConfig"))
        .thenReturn(Duration.newBuilder().setSeconds(60).build());
    when(uuidGenerator.generateId(ConfigPayloads.getDefaultInstance())).thenReturn("empty-hash");

    supplier =
        new ApiEndpointModelConfigSupplier(
            apiEndpointModelProvider, traceableEdgeConfig, uuidGenerator, fetchConfig);
  }

  @Test
  void getConfigType_returnsApiEndpointModelConfig() {
    assertEquals("ApiEndpointModelConfig", supplier.getConfigType());
  }

  @Test
  void getConfigs_missingServiceName_throwsInvalidArgument() {
    AgentCapabilities capabilities = AgentCapabilities.newBuilder().build();
    ConfigRequestElement requestElement = ConfigRequestElement.getDefaultInstance();

    assertThrows(
        StatusRuntimeException.class,
        () -> supplier.getConfigs(REQUEST_CONTEXT, ENVIRONMENT, requestElement, capabilities));
  }

  @Test
  void getConfigs_emptyModels_returnsDefaultPayload() {
    AgentCapabilities capabilities =
        AgentCapabilities.newBuilder().putAdditionalFields("serviceName", SERVICE_NAME).build();
    ConfigRequestElement requestElement = ConfigRequestElement.getDefaultInstance();

    when(apiEndpointModelProvider.getApiEndpointModels(
            eq(REQUEST_CONTEXT),
            eq(SERVICE_NAME),
            eq(ENVIRONMENT),
            any(ApiEndpointModelFetchConfig.class),
            any(ModelTypeFilter.class)))
        .thenReturn(Stream.empty());

    ConfigResponseElement response =
        supplier.getConfigs(REQUEST_CONTEXT, ENVIRONMENT, requestElement, capabilities);

    assertEquals("ApiEndpointModelConfig", response.getConfigType());
    assertEquals(ConfigPayloads.getDefaultInstance(), response.getConfigPayloads());
    assertTrue(response.getEnabled());
  }

  @Test
  void getConfigs_withModels_returnsSerializedApiDetectionModel() throws Exception {
    AgentCapabilities capabilities =
        AgentCapabilities.newBuilder().putAdditionalFields("serviceName", SERVICE_NAME).build();
    ConfigRequestElement requestElement = ConfigRequestElement.getDefaultInstance();

    ApiDetectionModel model =
        ApiDetectionModel.newBuilder()
            .setHttpApiDetectionModel(
                HttpApiDetectionModel.newBuilder()
                    .setHttpMethod(HttpMethod.HTTP_METHOD_GET)
                    .build())
            .build();
    ApiEndpointModelDetails details = new ApiEndpointModelDetails("api-1", model);

    when(apiEndpointModelProvider.getApiEndpointModels(
            eq(REQUEST_CONTEXT),
            eq(SERVICE_NAME),
            eq(ENVIRONMENT),
            any(ApiEndpointModelFetchConfig.class),
            any(ModelTypeFilter.class)))
        .thenReturn(Stream.of(details));

    ConfigPayloads expectedPayloads =
        ConfigPayloads.newBuilder()
            .addConfigBytes(
                ApiEndpointModelConfig.newBuilder()
                    .putApiEndpointModels("api-1", model.toByteString())
                    .build()
                    .toByteString())
            .build();
    when(uuidGenerator.generateId(expectedPayloads)).thenReturn("model-hash");

    ConfigResponseElement response =
        supplier.getConfigs(REQUEST_CONTEXT, ENVIRONMENT, requestElement, capabilities);

    assertEquals("ApiEndpointModelConfig", response.getConfigType());
    assertEquals("model-hash", response.getHash());

    ApiEndpointModelConfig parsed =
        ApiEndpointModelConfig.parseFrom(response.getConfigPayloads().getConfigBytes(0));
    assertEquals(1, parsed.getApiEndpointModelsMap().size());
    assertTrue(parsed.getApiEndpointModelsMap().containsKey("api-1"));

    ApiDetectionModel parsedModel =
        ApiDetectionModel.parseFrom(parsed.getApiEndpointModelsMap().get("api-1"));
    assertEquals(
        HttpMethod.HTTP_METHOD_GET, parsedModel.getHttpApiDetectionModel().getHttpMethod());
  }

  @Test
  void getConfigs_modelTypeFilterBoth_passedToProvider() {
    AgentCapabilities capabilities =
        AgentCapabilities.newBuilder()
            .putAdditionalFields("serviceName", SERVICE_NAME)
            .putAdditionalFields("modelTypeFilter", "BOTH")
            .build();
    ConfigRequestElement requestElement = ConfigRequestElement.getDefaultInstance();

    when(apiEndpointModelProvider.getApiEndpointModels(
            eq(REQUEST_CONTEXT),
            eq(SERVICE_NAME),
            eq(ENVIRONMENT),
            any(ApiEndpointModelFetchConfig.class),
            any(ModelTypeFilter.class)))
        .thenReturn(Stream.empty());

    ConfigResponseElement response =
        supplier.getConfigs(REQUEST_CONTEXT, ENVIRONMENT, requestElement, capabilities);

    assertEquals("ApiEndpointModelConfig", response.getConfigType());
    assertEquals(ConfigPayloads.getDefaultInstance(), response.getConfigPayloads());
  }

  @Test
  void getConfigs_multipleModels_allSerializedIntoConfig() throws Exception {
    AgentCapabilities capabilities =
        AgentCapabilities.newBuilder().putAdditionalFields("serviceName", SERVICE_NAME).build();
    ConfigRequestElement requestElement = ConfigRequestElement.getDefaultInstance();

    ApiDetectionModel model1 =
        ApiDetectionModel.newBuilder()
            .setHttpApiDetectionModel(
                HttpApiDetectionModel.newBuilder()
                    .setHttpMethod(HttpMethod.HTTP_METHOD_GET)
                    .build())
            .build();
    ApiDetectionModel model2 =
        ApiDetectionModel.newBuilder()
            .setHttpApiDetectionModel(
                HttpApiDetectionModel.newBuilder()
                    .setHttpMethod(HttpMethod.HTTP_METHOD_POST)
                    .build())
            .build();

    when(apiEndpointModelProvider.getApiEndpointModels(
            eq(REQUEST_CONTEXT),
            eq(SERVICE_NAME),
            eq(ENVIRONMENT),
            any(ApiEndpointModelFetchConfig.class),
            any(ModelTypeFilter.class)))
        .thenReturn(
            Stream.of(
                new ApiEndpointModelDetails("api-1", model1),
                new ApiEndpointModelDetails("api-2", model2)));

    when(uuidGenerator.generateId(org.mockito.ArgumentMatchers.any(ConfigPayloads.class)))
        .thenReturn("multi-hash");

    ConfigResponseElement response =
        supplier.getConfigs(REQUEST_CONTEXT, ENVIRONMENT, requestElement, capabilities);

    ApiEndpointModelConfig parsed =
        ApiEndpointModelConfig.parseFrom(response.getConfigPayloads().getConfigBytes(0));
    assertEquals(2, parsed.getApiEndpointModelsMap().size());

    Map<String, com.google.protobuf.ByteString> modelsMap = parsed.getApiEndpointModelsMap();
    assertEquals(
        HttpMethod.HTTP_METHOD_GET,
        ApiDetectionModel.parseFrom(modelsMap.get("api-1"))
            .getHttpApiDetectionModel()
            .getHttpMethod());
    assertEquals(
        HttpMethod.HTTP_METHOD_POST,
        ApiDetectionModel.parseFrom(modelsMap.get("api-2"))
            .getHttpApiDetectionModel()
            .getHttpMethod());
  }

  @Test
  void getConfigs_returnsAgentCapabilitiesAndPollingFrequency() {
    AgentCapabilities capabilities =
        AgentCapabilities.newBuilder().putAdditionalFields("serviceName", SERVICE_NAME).build();
    ConfigRequestElement requestElement = ConfigRequestElement.getDefaultInstance();

    when(apiEndpointModelProvider.getApiEndpointModels(
            eq(REQUEST_CONTEXT),
            eq(SERVICE_NAME),
            eq(ENVIRONMENT),
            any(ApiEndpointModelFetchConfig.class),
            any(ModelTypeFilter.class)))
        .thenReturn(Stream.empty());

    ConfigResponseElement response =
        supplier.getConfigs(REQUEST_CONTEXT, ENVIRONMENT, requestElement, capabilities);

    assertEquals(1, response.getSupportedAgentCapabilitiesCount());
    assertEquals(capabilities, response.getSupportedAgentCapabilities(0));
    assertEquals(Duration.newBuilder().setSeconds(60).build(), response.getRefreshAfterDuration());
  }
}
