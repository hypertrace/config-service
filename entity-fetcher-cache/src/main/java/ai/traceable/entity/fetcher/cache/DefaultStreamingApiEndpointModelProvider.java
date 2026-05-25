package ai.traceable.entity.fetcher.cache;

import ai.traceable.entity.fetcher.cache.StreamingApiMappingProvider.HttpApiDetails;
import ai.traceable.entity.fetcher.cache.config.ApiEndpointModelFetchConfig;
import ai.traceable.platform.insights.models.api.v1.ApiDefinitionModelFilter;
import ai.traceable.platform.insights.models.api.v1.ApiDefinitionModelMetadata;
import ai.traceable.platform.insights.models.api.v1.ApiModelForDetection;
import ai.traceable.platform.insights.models.api.v1.GetApiModelFilter;
import ai.traceable.platform.insights.models.api.v1.GetApiModelForDetectionRequest;
import ai.traceable.platform.insights.models.api.v1.GetApiModelForDetectionResponse;
import ai.traceable.platform.insights.models.api.v1.HttpApiModelForDetection;
import ai.traceable.platform.insights.models.api.v1.HttpMethod;
import ai.traceable.platform.insights.models.api.v1.KeyMatchClause;
import ai.traceable.platform.insights.models.api.v1.LearntMetadata;
import ai.traceable.platform.insights.models.api.v1.LearntModelFilter;
import ai.traceable.platform.insights.models.api.v1.LearntModelTypeFilter;
import ai.traceable.platform.insights.models.api.v1.ResponseParameterLocation;
import ai.traceable.platform.insights.models.api.v1.TrainingModelServiceGrpc;
import ai.traceable.platform.insights.models.api.v1.UserDefinedModelFilter;
import ai.traceable.protection.data.context.v1.ApiDefinitionModel;
import ai.traceable.protection.data.context.v1.ApiDetectionModel;
import ai.traceable.protection.data.context.v1.ArrayTypeDetails;
import ai.traceable.protection.data.context.v1.ContentTypeInfo;
import ai.traceable.protection.data.context.v1.EnumTypeDetails;
import ai.traceable.protection.data.context.v1.HttpApiDetectionModel;
import ai.traceable.protection.data.context.v1.LearntEndpointModel;
import ai.traceable.protection.data.context.v1.LearntEndpointModelData;
import ai.traceable.protection.data.context.v1.ParameterValueMetadata;
import ai.traceable.protection.data.context.v1.RequestInfo;
import ai.traceable.protection.data.context.v1.RequestParameterInfo;
import ai.traceable.protection.data.context.v1.ResponseBodyInfo;
import ai.traceable.protection.data.context.v1.ResponseInfo;
import ai.traceable.protection.data.context.v1.ResponseParameterInfo;
import ai.traceable.protection.data.context.v1.UserDefinedEndpointModelData;
import ai.traceable.protection.processing.common.v1.ParameterLocation;
import ai.traceable.protection.processing.common.v1.ParameterValueType;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@Singleton
@AllArgsConstructor(onConstructor_ = @Inject)
public class DefaultStreamingApiEndpointModelProvider implements StreamingApiEndpointModelProvider {

  private final EntityQueryServiceClient entityQueryServiceClient;
  private final TrainingModelServiceGrpc.TrainingModelServiceBlockingStub trainingModelServiceStub;

  @Override
  public Stream<ApiEndpointModelDetails> getApiEndpointModels(
      RequestContext requestContext,
      String serviceName,
      String environment,
      ApiEndpointModelFetchConfig config,
      ModelTypeFilter modelTypeFilter) {

    List<String> apiIds =
        entityQueryServiceClient
            .getAllHttpApiEndpoints(requestContext, serviceName, environment)
            .map(HttpApiDetails::getApiId)
            .distinct()
            .collect(Collectors.toUnmodifiableList());

    if (apiIds.isEmpty()) {
      return Stream.empty();
    }

    GetApiModelForDetectionRequest.Builder requestBuilder =
        GetApiModelForDetectionRequest.newBuilder().addAllApiIds(apiIds);

    if (modelTypeFilter == ModelTypeFilter.LEARNT || modelTypeFilter == ModelTypeFilter.BOTH) {
      requestBuilder.addFilter(buildLearntModelFilter(config));
    }

    if (modelTypeFilter == ModelTypeFilter.USER_DEFINED
        || modelTypeFilter == ModelTypeFilter.BOTH) {
      requestBuilder.addFilter(
          GetApiModelFilter.newBuilder()
              .setUserDefinedModelFilter(UserDefinedModelFilter.getDefaultInstance())
              .build());
    }

    GetApiModelForDetectionResponse response;
    try {
      response =
          requestContext.call(
              () -> trainingModelServiceStub.getApiModelForDetection(requestBuilder.build()));
    } catch (Exception e) {
      log.error(
          "Error fetching API endpoint models from insights service for service={}",
          serviceName,
          e);
      return Stream.empty();
    }

    return response.getDetectionApiModelsList().stream()
        .filter(ApiModelForDetection::hasHttpApiModelForDetection)
        .map(this::toApiEndpointModelDetails);
  }

  private GetApiModelFilter buildLearntModelFilter(ApiEndpointModelFetchConfig config) {
    ApiDefinitionModelFilter.Builder filterBuilder = ApiDefinitionModelFilter.newBuilder();
    config
        .getRequestKeyMatchClauses()
        .forEach(clause -> filterBuilder.addRequestKeyMatchClauses(toKeyMatchClause(clause)));
    config
        .getResponseKeyMatchClauses()
        .forEach(clause -> filterBuilder.addResponseKeyMatchClauses(toKeyMatchClause(clause)));
    return GetApiModelFilter.newBuilder()
        .setLearntModelFilter(
            LearntModelFilter.newBuilder()
                .addLearntModelTypeFilters(
                    LearntModelTypeFilter.newBuilder()
                        .setApiDefinitionModelFilter(filterBuilder.build())
                        .build())
                .build())
        .build();
  }

  private KeyMatchClause toKeyMatchClause(ApiEndpointModelFetchConfig.KeyMatchClauseConfig clause) {
    KeyMatchClause.Builder builder = KeyMatchClause.newBuilder();
    if (clause.getPrefix() != null) {
      builder.setPrefix(clause.getPrefix());
    }
    if (clause.getSuffix() != null) {
      builder.setSuffix(clause.getSuffix());
    }
    return builder.build();
  }

  private ApiEndpointModelDetails toApiEndpointModelDetails(ApiModelForDetection source) {
    HttpApiModelForDetection httpModel = source.getHttpApiModelForDetection();
    HttpApiDetectionModel.Builder httpDetectionModelBuilder =
        HttpApiDetectionModel.newBuilder().setHttpMethod(getHttpMethod(httpModel.getHttpMethod()));

    if (httpModel.hasLearntMetadata()) {
      httpDetectionModelBuilder.addHttpApiDetectionModelData(
          toLearntModelData(httpModel.getLearntMetadata()));
    }

    if (httpModel.hasUserDefinedMetadata()) {
      String specId =
          source.hasModelTypeDetails() && source.getModelTypeDetails().hasUserDefinedModelDetails()
              ? source.getModelTypeDetails().getUserDefinedModelDetails().getSpecId()
              : "";
      httpDetectionModelBuilder.addHttpApiDetectionModelData(
          toUserDefinedModelData(httpModel.getUserDefinedMetadata(), specId));
    }

    ApiDetectionModel apiDetectionModel =
        ApiDetectionModel.newBuilder()
            .setHttpApiDetectionModel(httpDetectionModelBuilder.build())
            .build();
    return new ApiEndpointModelDetails(source.getApiId(), apiDetectionModel);
  }

  private HttpApiDetectionModel.HttpApiDetectionModelData toLearntModelData(
      LearntMetadata learntMetadata) {
    LearntEndpointModelData.Builder learntData = LearntEndpointModelData.newBuilder();
    learntMetadata
        .getLearntModelsList()
        .forEach(
            learntModelMetadata -> {
              if (learntModelMetadata.hasApiDefinitionModelMetadata()) {
                ApiDefinitionModelMetadata metadata =
                    learntModelMetadata.getApiDefinitionModelMetadata();
                learntData.addLearntModels(
                    LearntEndpointModel.newBuilder()
                        .setApiDefinitionModel(
                            ApiDefinitionModel.newBuilder()
                                .putAllRequestMetadata(metadata.getRequestMetadataMap())
                                .putAllResponseMetadata(metadata.getResponseMetadataMap())
                                .build())
                        .build());
              }
            });
    return HttpApiDetectionModel.HttpApiDetectionModelData.newBuilder()
        .setLearntModelData(learntData.build())
        .build();
  }

  private HttpApiDetectionModel.HttpApiDetectionModelData toUserDefinedModelData(
      ai.traceable.platform.insights.models.api.v1.UserDefinedMetadata userDefinedMetadata,
      String specId) {
    UserDefinedEndpointModelData userDefinedData =
        UserDefinedEndpointModelData.newBuilder()
            .setSpecId(specId)
            .setRequestInfo(toRequestInfo(userDefinedMetadata.getRequestInfo()))
            .setResponseInfo(toResponseInfo(userDefinedMetadata.getResponseInfo()))
            .build();
    return HttpApiDetectionModel.HttpApiDetectionModelData.newBuilder()
        .setUserDefinedModelData(userDefinedData)
        .build();
  }

  private RequestInfo toRequestInfo(
      ai.traceable.platform.insights.models.api.v1.RequestInfo source) {
    RequestInfo.Builder builder = RequestInfo.newBuilder();
    source
        .getContentTypesList()
        .forEach(
            ct ->
                builder.addContentTypes(
                    ContentTypeInfo.newBuilder().setMimeType(ct.getMimeType()).build()));
    source
        .getParametersList()
        .forEach(param -> builder.addParameters(toRequestParameterInfo(param)));
    return builder.build();
  }

  private RequestParameterInfo toRequestParameterInfo(
      ai.traceable.platform.insights.models.api.v1.RequestParameterInfo source) {
    return RequestParameterInfo.newBuilder()
        .setFlattenedName(source.getFlattenedName())
        .setRequired(source.getRequired())
        .setParameterLocation(ParameterLocation.forNumber(source.getParameterLocationValue()))
        .setParameterValueMetadata(toParameterValueMetadata(source.getParameterValueMetadata()))
        .build();
  }

  private ResponseInfo toResponseInfo(
      ai.traceable.platform.insights.models.api.v1.ResponseInfo source) {
    ResponseInfo.Builder builder = ResponseInfo.newBuilder();
    source
        .getContentTypesList()
        .forEach(
            ct ->
                builder.addContentTypes(
                    ContentTypeInfo.newBuilder().setMimeType(ct.getMimeType()).build()));
    source
        .getResponseBodyInfosList()
        .forEach(bodyInfo -> builder.addResponseBodyInfos(toResponseBodyInfo(bodyInfo)));
    return builder.build();
  }

  private ResponseBodyInfo toResponseBodyInfo(
      ai.traceable.platform.insights.models.api.v1.ResponseBodyInfo source) {
    ResponseBodyInfo.Builder builder =
        ResponseBodyInfo.newBuilder().setStatusCode(source.getStatusCode());
    source
        .getResponseParametersList()
        .forEach(param -> builder.addResponseParameters(toResponseParameterInfo(param)));
    return builder.build();
  }

  private ResponseParameterInfo toResponseParameterInfo(
      ai.traceable.platform.insights.models.api.v1.ResponseParameterInfo source) {
    return ResponseParameterInfo.newBuilder()
        .setFlattenedName(source.getFlattenedName())
        .setRequired(source.getRequired())
        .setParameterLocation(toParameterLocationFromResponse(source.getParameterLocation()))
        .setParameterValueMetadata(toParameterValueMetadata(source.getParameterValueMetadata()))
        .build();
  }

  private ParameterLocation toParameterLocationFromResponse(ResponseParameterLocation location) {
    switch (location) {
      case RESPONSE_PARAMETER_LOCATION_BODY:
        return ParameterLocation.PARAMETER_LOCATION_BODY;
      case RESPONSE_PARAMETER_LOCATION_HEADER:
        return ParameterLocation.PARAMETER_LOCATION_HEADER;
      default:
        return ParameterLocation.PARAMETER_LOCATION_UNSPECIFIED;
    }
  }

  private ParameterValueMetadata toParameterValueMetadata(
      ai.traceable.platform.insights.models.api.v1.ParameterValueMetadata source) {
    ParameterValueMetadata.Builder builder =
        ParameterValueMetadata.newBuilder()
            .setParameterValueType(
                ParameterValueType.forNumber(source.getParameterValueTypeValue()));
    if (source.hasEnumTypeDetails()) {
      builder.setEnumTypeDetails(
          EnumTypeDetails.newBuilder()
              .addAllPossibleValues(source.getEnumTypeDetails().getPossibleValuesList())
              .build());
    } else if (source.hasArrayTypeDetails()) {
      builder.setArrayTypeDetails(
          ArrayTypeDetails.newBuilder()
              .setItems(toParameterValueMetadata(source.getArrayTypeDetails().getItems()))
              .build());
    }
    return builder.build();
  }

  private ai.traceable.protection.processing.common.v1.HttpMethod getHttpMethod(
      HttpMethod httpMethod) {
    switch (httpMethod) {
      case HTTP_METHOD_GET:
        return ai.traceable.protection.processing.common.v1.HttpMethod.HTTP_METHOD_GET;
      case HTTP_METHOD_POST:
        return ai.traceable.protection.processing.common.v1.HttpMethod.HTTP_METHOD_POST;
      case HTTP_METHOD_PUT:
        return ai.traceable.protection.processing.common.v1.HttpMethod.HTTP_METHOD_PUT;
      case HTTP_METHOD_DELETE:
        return ai.traceable.protection.processing.common.v1.HttpMethod.HTTP_METHOD_DELETE;
      case HTTP_METHOD_PATCH:
        return ai.traceable.protection.processing.common.v1.HttpMethod.HTTP_METHOD_PATCH;
      case HTTP_METHOD_OPTIONS:
        return ai.traceable.protection.processing.common.v1.HttpMethod.HTTP_METHOD_OPTIONS;
      case HTTP_METHOD_HEAD:
        return ai.traceable.protection.processing.common.v1.HttpMethod.HTTP_METHOD_HEAD;
      case HTTP_METHOD_TRACE:
        return ai.traceable.protection.processing.common.v1.HttpMethod.HTTP_METHOD_TRACE;
      default:
        return ai.traceable.protection.processing.common.v1.HttpMethod.HTTP_METHOD_UNSPECIFIED;
    }
  }
}
