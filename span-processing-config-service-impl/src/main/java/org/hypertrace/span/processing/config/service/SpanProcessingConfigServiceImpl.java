package org.hypertrace.span.processing.config.service;

import com.google.inject.Inject;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.span.processing.config.service.store.ApiNamingRulesConfigStore;
import org.hypertrace.span.processing.config.service.store.ExcludeSpanRulesConfigStore;
import org.hypertrace.span.processing.config.service.store.IncludeSpanRulesConfigStore;
import org.hypertrace.span.processing.config.service.store.SamplingConfigsConfigStore;
import org.hypertrace.span.processing.config.service.utils.TimestampConverter;
import org.hypertrace.span.processing.config.service.v1.ApiNamingRule;
import org.hypertrace.span.processing.config.service.v1.ApiNamingRuleConfig;
import org.hypertrace.span.processing.config.service.v1.ApiNamingRuleDetails;
import org.hypertrace.span.processing.config.service.v1.ApiNamingRuleInfo;
import org.hypertrace.span.processing.config.service.v1.ApiNamingRuleMetadata;
import org.hypertrace.span.processing.config.service.v1.CreateApiNamingRuleRequest;
import org.hypertrace.span.processing.config.service.v1.CreateApiNamingRuleResponse;
import org.hypertrace.span.processing.config.service.v1.CreateExcludeSpanRuleRequest;
import org.hypertrace.span.processing.config.service.v1.CreateExcludeSpanRuleResponse;
import org.hypertrace.span.processing.config.service.v1.CreateIncludeSpanRuleRequest;
import org.hypertrace.span.processing.config.service.v1.CreateIncludeSpanRuleResponse;
import org.hypertrace.span.processing.config.service.v1.CreateSamplingConfigRequest;
import org.hypertrace.span.processing.config.service.v1.CreateSamplingConfigResponse;
import org.hypertrace.span.processing.config.service.v1.DeleteApiNamingRuleRequest;
import org.hypertrace.span.processing.config.service.v1.DeleteApiNamingRuleResponse;
import org.hypertrace.span.processing.config.service.v1.DeleteExcludeSpanRuleRequest;
import org.hypertrace.span.processing.config.service.v1.DeleteExcludeSpanRuleResponse;
import org.hypertrace.span.processing.config.service.v1.DeleteIncludeSpanRuleRequest;
import org.hypertrace.span.processing.config.service.v1.DeleteIncludeSpanRuleResponse;
import org.hypertrace.span.processing.config.service.v1.DeleteSamplingConfigRequest;
import org.hypertrace.span.processing.config.service.v1.DeleteSamplingConfigResponse;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRule;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRuleDetails;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRuleInfo;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRuleMetadata;
import org.hypertrace.span.processing.config.service.v1.GetAllApiNamingRulesRequest;
import org.hypertrace.span.processing.config.service.v1.GetAllApiNamingRulesResponse;
import org.hypertrace.span.processing.config.service.v1.GetAllExcludeSpanRulesRequest;
import org.hypertrace.span.processing.config.service.v1.GetAllExcludeSpanRulesResponse;
import org.hypertrace.span.processing.config.service.v1.GetAllIncludeSpanRulesRequest;
import org.hypertrace.span.processing.config.service.v1.GetAllIncludeSpanRulesResponse;
import org.hypertrace.span.processing.config.service.v1.GetAllSamplingConfigsRequest;
import org.hypertrace.span.processing.config.service.v1.GetAllSamplingConfigsResponse;
import org.hypertrace.span.processing.config.service.v1.IncludeSpanRule;
import org.hypertrace.span.processing.config.service.v1.IncludeSpanRuleDetails;
import org.hypertrace.span.processing.config.service.v1.IncludeSpanRuleInfo;
import org.hypertrace.span.processing.config.service.v1.IncludeSpanRuleMetadata;
import org.hypertrace.span.processing.config.service.v1.SamplingConfig;
import org.hypertrace.span.processing.config.service.v1.SamplingConfigDetails;
import org.hypertrace.span.processing.config.service.v1.SamplingConfigInfo;
import org.hypertrace.span.processing.config.service.v1.SamplingConfigMetadata;
import org.hypertrace.span.processing.config.service.v1.SpanProcessingConfigServiceGrpc;
import org.hypertrace.span.processing.config.service.v1.UpdateApiNamingRule;
import org.hypertrace.span.processing.config.service.v1.UpdateApiNamingRuleRequest;
import org.hypertrace.span.processing.config.service.v1.UpdateApiNamingRuleResponse;
import org.hypertrace.span.processing.config.service.v1.UpdateExcludeSpanRule;
import org.hypertrace.span.processing.config.service.v1.UpdateExcludeSpanRuleRequest;
import org.hypertrace.span.processing.config.service.v1.UpdateExcludeSpanRuleResponse;
import org.hypertrace.span.processing.config.service.v1.UpdateIncludeSpanRule;
import org.hypertrace.span.processing.config.service.v1.UpdateIncludeSpanRuleRequest;
import org.hypertrace.span.processing.config.service.v1.UpdateIncludeSpanRuleResponse;
import org.hypertrace.span.processing.config.service.v1.UpdateSamplingConfig;
import org.hypertrace.span.processing.config.service.v1.UpdateSamplingConfigRequest;
import org.hypertrace.span.processing.config.service.v1.UpdateSamplingConfigResponse;
import org.hypertrace.span.processing.config.service.validation.SpanProcessingConfigRequestValidator;

@Slf4j
class SpanProcessingConfigServiceImpl
    extends SpanProcessingConfigServiceGrpc.SpanProcessingConfigServiceImplBase {
  private final SpanProcessingConfigRequestValidator validator;
  private final ExcludeSpanRulesConfigStore excludeSpanRulesConfigStore;
  private final IncludeSpanRulesConfigStore includeSpanRulesConfigStore;
  private final ApiNamingRulesConfigStore apiNamingRulesConfigStore;
  private final SamplingConfigsConfigStore samplingConfigsConfigStore;
  private final TimestampConverter timestampConverter;

  @Inject
  SpanProcessingConfigServiceImpl(
      ExcludeSpanRulesConfigStore excludeSpanRulesConfigStore,
      IncludeSpanRulesConfigStore includeSpanRulesConfigStore,
      ApiNamingRulesConfigStore apiNamingRulesConfigStore,
      SamplingConfigsConfigStore samplingConfigsConfigStore,
      SpanProcessingConfigRequestValidator requestValidator,
      TimestampConverter timestampConverter) {
    this.validator = requestValidator;
    this.excludeSpanRulesConfigStore = excludeSpanRulesConfigStore;
    this.includeSpanRulesConfigStore = includeSpanRulesConfigStore;
    this.apiNamingRulesConfigStore = apiNamingRulesConfigStore;
    this.samplingConfigsConfigStore = samplingConfigsConfigStore;
    this.timestampConverter = timestampConverter;
  }

  @Override
  public void getAllExcludeSpanRules(
      GetAllExcludeSpanRulesRequest request,
      StreamObserver<GetAllExcludeSpanRulesResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      responseObserver.onNext(
          GetAllExcludeSpanRulesResponse.newBuilder()
              .addAllRuleDetails(excludeSpanRulesConfigStore.getAllData(requestContext))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to get all exclude span rules for request: {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void createExcludeSpanRule(
      CreateExcludeSpanRuleRequest request,
      StreamObserver<CreateExcludeSpanRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      // TODO: need to handle priorities
      ExcludeSpanRule newRule =
          ExcludeSpanRule.newBuilder()
              .setId(UUID.randomUUID().toString())
              .setRuleInfo(request.getRuleInfo())
              .build();

      responseObserver.onNext(
          CreateExcludeSpanRuleResponse.newBuilder()
              .setRuleDetails(
                  buildExcludeSpanRuleDetails(
                      this.excludeSpanRulesConfigStore.upsertObject(requestContext, newRule)))
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error creating exclude span rule {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void updateExcludeSpanRule(
      UpdateExcludeSpanRuleRequest request,
      StreamObserver<UpdateExcludeSpanRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      UpdateExcludeSpanRule updateExcludeSpanRule = request.getRule();
      ExcludeSpanRule existingRule =
          this.excludeSpanRulesConfigStore
              .getData(requestContext, updateExcludeSpanRule.getId())
              .orElseThrow(Status.NOT_FOUND::asException);
      ExcludeSpanRule updatedRule = buildUpdatedRule(existingRule, updateExcludeSpanRule);

      responseObserver.onNext(
          UpdateExcludeSpanRuleResponse.newBuilder()
              .setRuleDetails(
                  buildExcludeSpanRuleDetails(
                      this.excludeSpanRulesConfigStore.upsertObject(requestContext, updatedRule)))
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error updating exclude span rule: {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void deleteExcludeSpanRule(
      DeleteExcludeSpanRuleRequest request,
      StreamObserver<DeleteExcludeSpanRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      // TODO: need to handle priorities
      this.excludeSpanRulesConfigStore
          .deleteObject(requestContext, request.getId())
          .orElseThrow(Status.NOT_FOUND::asRuntimeException);

      responseObserver.onNext(DeleteExcludeSpanRuleResponse.newBuilder().build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error deleting exclude span rule: {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  private ExcludeSpanRule buildUpdatedRule(
      ExcludeSpanRule existingRule, UpdateExcludeSpanRule updateExcludeSpanRule) {
    return ExcludeSpanRule.newBuilder(existingRule)
        .setRuleInfo(
            ExcludeSpanRuleInfo.newBuilder()
                .setName(updateExcludeSpanRule.getName())
                .setFilter(updateExcludeSpanRule.getFilter())
                .setDisabled(updateExcludeSpanRule.getDisabled())
                .build())
        .build();
  }

  private ExcludeSpanRuleDetails buildExcludeSpanRuleDetails(
      ContextualConfigObject<ExcludeSpanRule> configObject) {
    return ExcludeSpanRuleDetails.newBuilder()
        .setRule(configObject.getData())
        .setMetadata(
            ExcludeSpanRuleMetadata.newBuilder()
                .setCreationTimestamp(
                    timestampConverter.convert(configObject.getCreationTimestamp()))
                .setLastUpdatedTimestamp(
                    timestampConverter.convert(configObject.getLastUpdatedTimestamp()))
                .build())
        .build();
  }

  @Override
  public void getAllApiNamingRules(
      GetAllApiNamingRulesRequest request,
      StreamObserver<GetAllApiNamingRulesResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      responseObserver.onNext(
          GetAllApiNamingRulesResponse.newBuilder()
              .addAllRuleDetails(apiNamingRulesConfigStore.getAllData(requestContext))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to get all api naming rules for request: {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void createApiNamingRule(
      CreateApiNamingRuleRequest request,
      StreamObserver<CreateApiNamingRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      // TODO: need to handle priorities
      ApiNamingRule newRule =
          ApiNamingRule.newBuilder()
              .setId(UUID.randomUUID().toString())
              .setRuleInfo(request.getRuleInfo())
              .build();

      responseObserver.onNext(
          CreateApiNamingRuleResponse.newBuilder()
              .setRuleDetails(
                  buildApiNamingRuleDetails(
                      this.apiNamingRulesConfigStore.upsertObject(requestContext, newRule)))
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error creating api naming rule {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void updateApiNamingRule(
      UpdateApiNamingRuleRequest request,
      StreamObserver<UpdateApiNamingRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      UpdateApiNamingRule updateApiNamingRule = request.getRule();
      ApiNamingRule existingRule =
          this.apiNamingRulesConfigStore
              .getData(requestContext, updateApiNamingRule.getId())
              .orElseThrow(Status.NOT_FOUND::asException);
      ApiNamingRule updatedRule = buildUpdatedRule(existingRule, updateApiNamingRule);

      responseObserver.onNext(
          UpdateApiNamingRuleResponse.newBuilder()
              .setRuleDetails(
                  buildApiNamingRuleDetails(
                      this.apiNamingRulesConfigStore.upsertObject(requestContext, updatedRule)))
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error updating api naming rule: {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void deleteApiNamingRule(
      DeleteApiNamingRuleRequest request,
      StreamObserver<DeleteApiNamingRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      // TODO: need to handle priorities
      this.apiNamingRulesConfigStore
          .deleteObject(requestContext, request.getId())
          .orElseThrow(Status.NOT_FOUND::asRuntimeException);

      responseObserver.onNext(DeleteApiNamingRuleResponse.newBuilder().build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error deleting api naming rule: {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  private ApiNamingRule buildUpdatedRule(
      ApiNamingRule existingRule, UpdateApiNamingRule updateApiNamingRule) {
    return ApiNamingRule.newBuilder(existingRule)
        .setRuleInfo(
            ApiNamingRuleInfo.newBuilder()
                .setName(updateApiNamingRule.getName())
                .setFilter(updateApiNamingRule.getFilter())
                .setDisabled(updateApiNamingRule.getDisabled())
                .setRuleConfig(
                    ApiNamingRuleConfig.newBuilder(updateApiNamingRule.getRuleConfig()).build())
                .build())
        .build();
  }

  private ApiNamingRuleDetails buildApiNamingRuleDetails(
      ContextualConfigObject<ApiNamingRule> configObject) {
    return ApiNamingRuleDetails.newBuilder()
        .setRule(configObject.getData())
        .setMetadata(
            ApiNamingRuleMetadata.newBuilder()
                .setCreationTimestamp(
                    timestampConverter.convert(configObject.getCreationTimestamp()))
                .setLastUpdatedTimestamp(
                    timestampConverter.convert(configObject.getLastUpdatedTimestamp()))
                .build())
        .build();
  }

  @Override
  public void getAllIncludeSpanRules(
      GetAllIncludeSpanRulesRequest request,
      StreamObserver<GetAllIncludeSpanRulesResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      responseObserver.onNext(
          GetAllIncludeSpanRulesResponse.newBuilder()
              .addAllRuleDetails(includeSpanRulesConfigStore.getAllData(requestContext))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to get all include span rules for request: {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void createIncludeSpanRule(
      CreateIncludeSpanRuleRequest request,
      StreamObserver<CreateIncludeSpanRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      // TODO: need to handle priorities
      IncludeSpanRule newRule =
          IncludeSpanRule.newBuilder()
              .setId(UUID.randomUUID().toString())
              .setRuleInfo(request.getRuleInfo())
              .build();

      responseObserver.onNext(
          CreateIncludeSpanRuleResponse.newBuilder()
              .setRuleDetails(
                  buildIncludeSpanRuleDetails(
                      this.includeSpanRulesConfigStore.upsertObject(requestContext, newRule)))
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error creating include span rule {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void updateIncludeSpanRule(
      UpdateIncludeSpanRuleRequest request,
      StreamObserver<UpdateIncludeSpanRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      UpdateIncludeSpanRule updateIncludeSpanRule = request.getRule();
      IncludeSpanRule existingRule =
          this.includeSpanRulesConfigStore
              .getData(requestContext, updateIncludeSpanRule.getId())
              .orElseThrow(Status.NOT_FOUND::asException);
      IncludeSpanRule updatedRule = buildUpdatedRule(existingRule, updateIncludeSpanRule);

      responseObserver.onNext(
          UpdateIncludeSpanRuleResponse.newBuilder()
              .setRuleDetails(
                  buildIncludeSpanRuleDetails(
                      this.includeSpanRulesConfigStore.upsertObject(requestContext, updatedRule)))
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error updating include span rule: {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void deleteIncludeSpanRule(
      DeleteIncludeSpanRuleRequest request,
      StreamObserver<DeleteIncludeSpanRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      // TODO: need to handle priorities
      this.includeSpanRulesConfigStore
          .deleteObject(requestContext, request.getId())
          .orElseThrow(Status.NOT_FOUND::asRuntimeException);

      responseObserver.onNext(DeleteIncludeSpanRuleResponse.newBuilder().build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error deleting include span rule: {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  private IncludeSpanRuleDetails buildIncludeSpanRuleDetails(
      ContextualConfigObject<IncludeSpanRule> configObject) {
    return IncludeSpanRuleDetails.newBuilder()
        .setRule(configObject.getData())
        .setMetadata(
            IncludeSpanRuleMetadata.newBuilder()
                .setCreationTimestamp(
                    timestampConverter.convert(configObject.getCreationTimestamp()))
                .setLastUpdatedTimestamp(
                    timestampConverter.convert(configObject.getLastUpdatedTimestamp()))
                .build())
        .build();
  }

  private IncludeSpanRule buildUpdatedRule(
      IncludeSpanRule existingRule, UpdateIncludeSpanRule updateIncludeSpanRule) {
    return IncludeSpanRule.newBuilder(existingRule)
        .setRuleInfo(
            IncludeSpanRuleInfo.newBuilder()
                .setName(updateIncludeSpanRule.getName())
                .setFilter(updateIncludeSpanRule.getFilter())
                .setDisabled(updateIncludeSpanRule.getDisabled())
                .build())
        .build();
  }

  @Override
  public void getAllSamplingConfigs(
      GetAllSamplingConfigsRequest request,
      StreamObserver<GetAllSamplingConfigsResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      responseObserver.onNext(
          GetAllSamplingConfigsResponse.newBuilder()
              .addAllSamplingConfigDetails(samplingConfigsConfigStore.getAllData(requestContext))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to get all sampling configs for request: {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void createSamplingConfig(
      CreateSamplingConfigRequest request,
      StreamObserver<CreateSamplingConfigResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      // TODO: need to handle priorities
      SamplingConfig newSamplingConfig =
          SamplingConfig.newBuilder()
              .setId(UUID.randomUUID().toString())
              .setSamplingConfigInfo(request.getSamplingConfigInfo())
              .build();

      responseObserver.onNext(
          CreateSamplingConfigResponse.newBuilder()
              .setSamplingConfigDetails(
                  buildSamplingConfigDetails(
                      this.samplingConfigsConfigStore.upsertObject(
                          requestContext, newSamplingConfig)))
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error creating sampling config {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void updateSamplingConfig(
      UpdateSamplingConfigRequest request,
      StreamObserver<UpdateSamplingConfigResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      UpdateSamplingConfig updateSamplingConfig = request.getSamplingConfig();
      SamplingConfig existingSamplingConfig =
          this.samplingConfigsConfigStore
              .getData(requestContext, updateSamplingConfig.getId())
              .orElseThrow(Status.NOT_FOUND::asException);
      SamplingConfig updatedSamplingConfig =
          buildUpdatedSamplingConfig(existingSamplingConfig, updateSamplingConfig);

      responseObserver.onNext(
          UpdateSamplingConfigResponse.newBuilder()
              .setSamplingConfigDetails(
                  buildSamplingConfigDetails(
                      this.samplingConfigsConfigStore.upsertObject(
                          requestContext, updatedSamplingConfig)))
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error updating include span rule: {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void deleteSamplingConfig(
      DeleteSamplingConfigRequest request,
      StreamObserver<DeleteSamplingConfigResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      // TODO: need to handle priorities
      this.samplingConfigsConfigStore
          .deleteObject(requestContext, request.getId())
          .orElseThrow(Status.NOT_FOUND::asRuntimeException);

      responseObserver.onNext(DeleteSamplingConfigResponse.newBuilder().build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error deleting sampling config: {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  private SamplingConfigDetails buildSamplingConfigDetails(
      ContextualConfigObject<SamplingConfig> configObject) {
    return SamplingConfigDetails.newBuilder()
        .setSamplingConfig(configObject.getData())
        .setMetadata(
            SamplingConfigMetadata.newBuilder()
                .setCreationTimestamp(
                    timestampConverter.convert(configObject.getCreationTimestamp()))
                .setLastUpdatedTimestamp(
                    timestampConverter.convert(configObject.getLastUpdatedTimestamp()))
                .build())
        .build();
  }

  private SamplingConfig buildUpdatedSamplingConfig(
      SamplingConfig existingSamplingConfig, UpdateSamplingConfig updateSamplingConfig) {
    return SamplingConfig.newBuilder(existingSamplingConfig)
        .setSamplingConfigInfo(
            SamplingConfigInfo.newBuilder()
                .setRateLimitConfig(updateSamplingConfig.getRateLimitConfig())
                .setFilter(updateSamplingConfig.getFilter())
                .build())
        .build();
  }
}
