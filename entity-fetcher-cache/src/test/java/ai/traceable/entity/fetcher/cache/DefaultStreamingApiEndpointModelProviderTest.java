package ai.traceable.entity.fetcher.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.entity.fetcher.cache.StreamingApiEndpointModelProvider.ApiEndpointModelDetails;
import ai.traceable.entity.fetcher.cache.StreamingApiEndpointModelProvider.ModelTypeFilter;
import ai.traceable.entity.fetcher.cache.StreamingApiMappingProvider.HttpApiDetails;
import ai.traceable.entity.fetcher.cache.config.ApiEndpointModelFetchConfig;
import ai.traceable.platform.insights.models.api.v1.ApiDefinitionModelMetadata;
import ai.traceable.platform.insights.models.api.v1.ApiModelForDetection;
import ai.traceable.platform.insights.models.api.v1.ContentTypeInfo;
import ai.traceable.platform.insights.models.api.v1.GetApiModelFilter;
import ai.traceable.platform.insights.models.api.v1.GetApiModelForDetectionRequest;
import ai.traceable.platform.insights.models.api.v1.GetApiModelForDetectionResponse;
import ai.traceable.platform.insights.models.api.v1.HttpApiModelForDetection;
import ai.traceable.platform.insights.models.api.v1.HttpMethod;
import ai.traceable.platform.insights.models.api.v1.LearntMetadata;
import ai.traceable.platform.insights.models.api.v1.LearntModelFilter;
import ai.traceable.platform.insights.models.api.v1.LearntModelMetadata;
import ai.traceable.platform.insights.models.api.v1.LearntModelTypeFilter;
import ai.traceable.platform.insights.models.api.v1.ParameterValueMetadata;
import ai.traceable.platform.insights.models.api.v1.ParameterValueType;
import ai.traceable.platform.insights.models.api.v1.RequestInfo;
import ai.traceable.platform.insights.models.api.v1.RequestParameterInfo;
import ai.traceable.platform.insights.models.api.v1.RequestParameterLocation;
import ai.traceable.platform.insights.models.api.v1.ResponseBodyInfo;
import ai.traceable.platform.insights.models.api.v1.ResponseInfo;
import ai.traceable.platform.insights.models.api.v1.ResponseParameterInfo;
import ai.traceable.platform.insights.models.api.v1.ResponseParameterLocation;
import ai.traceable.platform.insights.models.api.v1.TrainingModelServiceGrpc;
import ai.traceable.platform.insights.models.api.v1.UserDefinedMetadata;
import ai.traceable.platform.insights.models.api.v1.UserDefinedModelFilter;
import ai.traceable.protection.data.context.v1.ApiDetectionModel;
import ai.traceable.protection.data.context.v1.HttpApiDetectionModel;
import ai.traceable.protection.processing.common.v1.ParameterLocation;
import com.google.protobuf.Value;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DefaultStreamingApiEndpointModelProviderTest {

  private static final String TENANT_ID = "test-tenant";
  private static final String SERVICE_NAME = "test-service";
  private static final String ENVIRONMENT = "test-env";
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);

  private EntityQueryServiceClient entityQueryServiceClient;
  private TrainingModelServiceGrpc.TrainingModelServiceBlockingStub trainingModelServiceStub;
  private DefaultStreamingApiEndpointModelProvider provider;

  @BeforeEach
  void setUp() {
    entityQueryServiceClient = mock(EntityQueryServiceClient.class);
    trainingModelServiceStub =
        mock(TrainingModelServiceGrpc.TrainingModelServiceBlockingStub.class);
    provider =
        new DefaultStreamingApiEndpointModelProvider(
            entityQueryServiceClient, trainingModelServiceStub);
  }

  @Test
  void getApiEndpointModels_emptyApiIds_returnsEmptyStream() {
    when(entityQueryServiceClient.getAllHttpApiEndpoints(
            REQUEST_CONTEXT, SERVICE_NAME, ENVIRONMENT))
        .thenReturn(Stream.empty());

    ApiEndpointModelFetchConfig config =
        new ApiEndpointModelFetchConfig(Collections.emptyList(), Collections.emptyList());

    List<ApiEndpointModelDetails> result =
        provider
            .getApiEndpointModels(
                REQUEST_CONTEXT, SERVICE_NAME, ENVIRONMENT, config, ModelTypeFilter.LEARNT)
            .collect(Collectors.toList());

    assertTrue(result.isEmpty());
    verify(trainingModelServiceStub, never()).getApiModelForDetection(any());
  }

  @Test
  void getApiEndpointModels_learntModelOnly_returnsMappedApiDetectionModel() {
    when(entityQueryServiceClient.getAllHttpApiEndpoints(
            REQUEST_CONTEXT, SERVICE_NAME, ENVIRONMENT))
        .thenReturn(Stream.of(new HttpApiDetails("api-1", "GET", List.of("/api/v1/users"))));

    ApiEndpointModelFetchConfig config =
        new ApiEndpointModelFetchConfig(
            List.of(new ApiEndpointModelFetchConfig.KeyMatchClauseConfig(null, ".request")),
            List.of(new ApiEndpointModelFetchConfig.KeyMatchClauseConfig(null, ".response")));

    GetApiModelForDetectionRequest expectedRequest =
        GetApiModelForDetectionRequest.newBuilder()
            .addApiIds("api-1")
            .addFilter(buildLearntFilter(".request", ".response"))
            .build();

    GetApiModelForDetectionResponse response =
        GetApiModelForDetectionResponse.newBuilder()
            .addDetectionApiModels(buildLearntApiModel("api-1", "GET"))
            .build();

    when(trainingModelServiceStub.getApiModelForDetection(expectedRequest)).thenReturn(response);

    List<ApiEndpointModelDetails> result =
        provider
            .getApiEndpointModels(
                REQUEST_CONTEXT, SERVICE_NAME, ENVIRONMENT, config, ModelTypeFilter.LEARNT)
            .collect(Collectors.toList());

    assertEquals(1, result.size());
    ApiEndpointModelDetails details = result.get(0);
    assertEquals("api-1", details.getApiId());

    ApiDetectionModel model = details.getApiDetectionModel();
    assertTrue(model.hasHttpApiDetectionModel());
    HttpApiDetectionModel httpModel = model.getHttpApiDetectionModel();
    assertEquals(1, httpModel.getHttpApiDetectionModelDataCount());
    assertTrue(httpModel.getHttpApiDetectionModelData(0).hasLearntModelData());
  }

  @Test
  void getApiEndpointModels_includeUserDefined_addsUserDefinedFilter() {
    when(entityQueryServiceClient.getAllHttpApiEndpoints(
            REQUEST_CONTEXT, SERVICE_NAME, ENVIRONMENT))
        .thenReturn(Stream.of(new HttpApiDetails("api-2", "POST", List.of("/api/v1/orders"))));

    ApiEndpointModelFetchConfig config =
        new ApiEndpointModelFetchConfig(Collections.emptyList(), Collections.emptyList());

    GetApiModelForDetectionRequest expectedRequest =
        GetApiModelForDetectionRequest.newBuilder()
            .addApiIds("api-2")
            .addFilter(buildEmptyLearntFilter())
            .addFilter(
                GetApiModelFilter.newBuilder()
                    .setUserDefinedModelFilter(UserDefinedModelFilter.getDefaultInstance())
                    .build())
            .build();

    GetApiModelForDetectionResponse response =
        GetApiModelForDetectionResponse.newBuilder()
            .addDetectionApiModels(buildUserDefinedApiModel("api-2", "POST"))
            .build();

    when(trainingModelServiceStub.getApiModelForDetection(expectedRequest)).thenReturn(response);

    List<ApiEndpointModelDetails> result =
        provider
            .getApiEndpointModels(
                REQUEST_CONTEXT, SERVICE_NAME, ENVIRONMENT, config, ModelTypeFilter.BOTH)
            .collect(Collectors.toList());

    assertEquals(1, result.size());
    ApiEndpointModelDetails details = result.get(0);
    assertEquals("api-2", details.getApiId());
    assertTrue(
        details
            .getApiDetectionModel()
            .getHttpApiDetectionModel()
            .getHttpApiDetectionModelData(0)
            .hasUserDefinedModelData());
  }

  @Test
  void getApiEndpointModels_insightsServiceThrows_returnsEmptyStream() {
    when(entityQueryServiceClient.getAllHttpApiEndpoints(
            REQUEST_CONTEXT, SERVICE_NAME, ENVIRONMENT))
        .thenReturn(Stream.of(new HttpApiDetails("api-1", "GET", List.of("/api/v1/test"))));

    when(trainingModelServiceStub.getApiModelForDetection(any()))
        .thenThrow(new RuntimeException("gRPC connection failed"));

    ApiEndpointModelFetchConfig config =
        new ApiEndpointModelFetchConfig(Collections.emptyList(), Collections.emptyList());

    List<ApiEndpointModelDetails> result =
        provider
            .getApiEndpointModels(
                REQUEST_CONTEXT, SERVICE_NAME, ENVIRONMENT, config, ModelTypeFilter.LEARNT)
            .collect(Collectors.toList());

    assertTrue(result.isEmpty());
  }

  @Test
  void getApiEndpointModels_parameterLocationsMappedCorrectly() {
    when(entityQueryServiceClient.getAllHttpApiEndpoints(
            REQUEST_CONTEXT, SERVICE_NAME, ENVIRONMENT))
        .thenReturn(Stream.of(new HttpApiDetails("api-3", "GET", List.of("/api/v1/items"))));

    ApiEndpointModelFetchConfig config =
        new ApiEndpointModelFetchConfig(Collections.emptyList(), Collections.emptyList());

    GetApiModelForDetectionResponse response =
        GetApiModelForDetectionResponse.newBuilder()
            .addDetectionApiModels(buildApiModelWithParameters("api-3"))
            .build();

    when(trainingModelServiceStub.getApiModelForDetection(any())).thenReturn(response);

    List<ApiEndpointModelDetails> result =
        provider
            .getApiEndpointModels(
                REQUEST_CONTEXT, SERVICE_NAME, ENVIRONMENT, config, ModelTypeFilter.BOTH)
            .collect(Collectors.toList());

    assertEquals(1, result.size());
    HttpApiDetectionModel httpModel =
        result.get(0).getApiDetectionModel().getHttpApiDetectionModel();

    var userDefinedData = httpModel.getHttpApiDetectionModelData(0).getUserDefinedModelData();

    assertEquals(1, userDefinedData.getRequestInfo().getParametersCount());
    assertEquals(
        ParameterLocation.PARAMETER_LOCATION_QUERY,
        userDefinedData.getRequestInfo().getParameters(0).getParameterLocation());

    assertEquals(1, userDefinedData.getResponseInfo().getResponseBodyInfosCount());
    var responseParam =
        userDefinedData.getResponseInfo().getResponseBodyInfos(0).getResponseParameters(0);
    assertEquals(ParameterLocation.PARAMETER_LOCATION_BODY, responseParam.getParameterLocation());
  }

  @Test
  void getApiEndpointModels_responseParameterLocationHeader_mappedToHeader() {
    when(entityQueryServiceClient.getAllHttpApiEndpoints(
            REQUEST_CONTEXT, SERVICE_NAME, ENVIRONMENT))
        .thenReturn(Stream.of(new HttpApiDetails("api-4", "GET", List.of("/api/v1/headers"))));

    ApiEndpointModelFetchConfig config =
        new ApiEndpointModelFetchConfig(Collections.emptyList(), Collections.emptyList());

    GetApiModelForDetectionResponse response =
        GetApiModelForDetectionResponse.newBuilder()
            .addDetectionApiModels(buildApiModelWithResponseHeaderParam("api-4"))
            .build();

    when(trainingModelServiceStub.getApiModelForDetection(any())).thenReturn(response);

    List<ApiEndpointModelDetails> result =
        provider
            .getApiEndpointModels(
                REQUEST_CONTEXT, SERVICE_NAME, ENVIRONMENT, config, ModelTypeFilter.BOTH)
            .collect(Collectors.toList());

    assertEquals(1, result.size());
    var responseParam =
        result
            .get(0)
            .getApiDetectionModel()
            .getHttpApiDetectionModel()
            .getHttpApiDetectionModelData(0)
            .getUserDefinedModelData()
            .getResponseInfo()
            .getResponseBodyInfos(0)
            .getResponseParameters(0);

    assertEquals(ParameterLocation.PARAMETER_LOCATION_HEADER, responseParam.getParameterLocation());
  }

  @Test
  void getApiEndpointModels_duplicateApiIds_deduplicatedBeforeRequest() {
    when(entityQueryServiceClient.getAllHttpApiEndpoints(
            REQUEST_CONTEXT, SERVICE_NAME, ENVIRONMENT))
        .thenReturn(
            Stream.of(
                new HttpApiDetails("api-1", "GET", List.of("/v1")),
                new HttpApiDetails("api-1", "POST", List.of("/v1"))));

    ApiEndpointModelFetchConfig config =
        new ApiEndpointModelFetchConfig(Collections.emptyList(), Collections.emptyList());

    GetApiModelForDetectionRequest expectedRequest =
        GetApiModelForDetectionRequest.newBuilder()
            .addApiIds("api-1")
            .addFilter(buildEmptyLearntFilter())
            .build();

    when(trainingModelServiceStub.getApiModelForDetection(expectedRequest))
        .thenReturn(
            GetApiModelForDetectionResponse.newBuilder()
                .addDetectionApiModels(buildLearntApiModel("api-1", "GET"))
                .build());

    List<ApiEndpointModelDetails> result =
        provider
            .getApiEndpointModels(
                REQUEST_CONTEXT, SERVICE_NAME, ENVIRONMENT, config, ModelTypeFilter.LEARNT)
            .collect(Collectors.toList());

    assertEquals(1, result.size());
  }

  private GetApiModelFilter buildLearntFilter(String requestSuffix, String responseSuffix) {
    return GetApiModelFilter.newBuilder()
        .setLearntModelFilter(
            LearntModelFilter.newBuilder()
                .addLearntModelTypeFilters(
                    LearntModelTypeFilter.newBuilder()
                        .setApiDefinitionModelFilter(
                            ai.traceable.platform.insights.models.api.v1.ApiDefinitionModelFilter
                                .newBuilder()
                                .addRequestKeyMatchClauses(
                                    ai.traceable.platform.insights.models.api.v1.KeyMatchClause
                                        .newBuilder()
                                        .setSuffix(requestSuffix)
                                        .build())
                                .addResponseKeyMatchClauses(
                                    ai.traceable.platform.insights.models.api.v1.KeyMatchClause
                                        .newBuilder()
                                        .setSuffix(responseSuffix)
                                        .build())
                                .build())
                        .build())
                .build())
        .build();
  }

  private GetApiModelFilter buildEmptyLearntFilter() {
    return GetApiModelFilter.newBuilder()
        .setLearntModelFilter(
            LearntModelFilter.newBuilder()
                .addLearntModelTypeFilters(
                    LearntModelTypeFilter.newBuilder()
                        .setApiDefinitionModelFilter(
                            ai.traceable.platform.insights.models.api.v1.ApiDefinitionModelFilter
                                .getDefaultInstance())
                        .build())
                .build())
        .build();
  }

  private ApiModelForDetection buildLearntApiModel(String apiId, String method) {
    return ApiModelForDetection.newBuilder()
        .setApiId(apiId)
        .setHttpApiModelForDetection(
            HttpApiModelForDetection.newBuilder()
                .setHttpMethod(HttpMethod.HTTP_METHOD_GET)
                .setLearntMetadata(
                    LearntMetadata.newBuilder()
                        .addLearntModels(
                            LearntModelMetadata.newBuilder()
                                .setApiDefinitionModelMetadata(
                                    ApiDefinitionModelMetadata.newBuilder()
                                        .putRequestMetadata(
                                            "body.userId",
                                            Value.newBuilder().setStringValue("string").build())
                                        .build())
                                .build())
                        .build())
                .build())
        .build();
  }

  private ApiModelForDetection buildUserDefinedApiModel(String apiId, String method) {
    return ApiModelForDetection.newBuilder()
        .setApiId(apiId)
        .setHttpApiModelForDetection(
            HttpApiModelForDetection.newBuilder()
                .setHttpMethod(HttpMethod.HTTP_METHOD_POST)
                .setUserDefinedMetadata(
                    UserDefinedMetadata.newBuilder()
                        .setRequestInfo(
                            RequestInfo.newBuilder()
                                .addContentTypes(
                                    ContentTypeInfo.newBuilder()
                                        .setMimeType("application/json")
                                        .build())
                                .build())
                        .setResponseInfo(ResponseInfo.getDefaultInstance())
                        .build())
                .build())
        .build();
  }

  private ApiModelForDetection buildApiModelWithParameters(String apiId) {
    return ApiModelForDetection.newBuilder()
        .setApiId(apiId)
        .setHttpApiModelForDetection(
            HttpApiModelForDetection.newBuilder()
                .setHttpMethod(HttpMethod.HTTP_METHOD_GET)
                .setUserDefinedMetadata(
                    UserDefinedMetadata.newBuilder()
                        .setRequestInfo(
                            RequestInfo.newBuilder()
                                .addParameters(
                                    RequestParameterInfo.newBuilder()
                                        .setFlattenedName("q")
                                        .setRequired(false)
                                        .setParameterLocation(
                                            RequestParameterLocation
                                                .REQUEST_PARAMETER_LOCATION_QUERY)
                                        .setParameterValueMetadata(
                                            ParameterValueMetadata.newBuilder()
                                                .setParameterValueType(
                                                    ParameterValueType.PARAMETER_VALUE_TYPE_STRING)
                                                .build())
                                        .build())
                                .build())
                        .setResponseInfo(
                            ResponseInfo.newBuilder()
                                .addResponseBodyInfos(
                                    ResponseBodyInfo.newBuilder()
                                        .setStatusCode(200)
                                        .addResponseParameters(
                                            ResponseParameterInfo.newBuilder()
                                                .setFlattenedName("body.id")
                                                .setRequired(true)
                                                .setParameterLocation(
                                                    ResponseParameterLocation
                                                        .RESPONSE_PARAMETER_LOCATION_BODY)
                                                .setParameterValueMetadata(
                                                    ParameterValueMetadata.newBuilder()
                                                        .setParameterValueType(
                                                            ParameterValueType
                                                                .PARAMETER_VALUE_TYPE_INTEGER)
                                                        .build())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build())
        .build();
  }

  private ApiModelForDetection buildApiModelWithResponseHeaderParam(String apiId) {
    return ApiModelForDetection.newBuilder()
        .setApiId(apiId)
        .setHttpApiModelForDetection(
            HttpApiModelForDetection.newBuilder()
                .setHttpMethod(HttpMethod.HTTP_METHOD_GET)
                .setUserDefinedMetadata(
                    UserDefinedMetadata.newBuilder()
                        .setRequestInfo(RequestInfo.getDefaultInstance())
                        .setResponseInfo(
                            ResponseInfo.newBuilder()
                                .addResponseBodyInfos(
                                    ResponseBodyInfo.newBuilder()
                                        .setStatusCode(200)
                                        .addResponseParameters(
                                            ResponseParameterInfo.newBuilder()
                                                .setFlattenedName("x-request-id")
                                                .setParameterLocation(
                                                    ResponseParameterLocation
                                                        .RESPONSE_PARAMETER_LOCATION_HEADER)
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build())
        .build();
  }
}
