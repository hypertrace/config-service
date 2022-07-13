package org.hypertrace.span.processing.config.service;

import com.google.inject.Inject;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.span.processing.config.service.store.ApiNamingRulesConfigStore;
import org.hypertrace.span.processing.config.service.store.ExcludeSpanRulesConfigStore;
import org.hypertrace.span.processing.config.service.utils.TimestampConverter;
import org.hypertrace.span.processing.config.service.v1.ApiNamingRule;
import org.hypertrace.span.processing.config.service.v1.ApiNamingRuleConfig;
import org.hypertrace.span.processing.config.service.v1.ApiNamingRuleDetails;
import org.hypertrace.span.processing.config.service.v1.ApiNamingRuleInfo;
import org.hypertrace.span.processing.config.service.v1.ApiNamingRuleMetadata;
import org.hypertrace.span.processing.config.service.v1.CreateApiNamingRuleRequest;
import org.hypertrace.span.processing.config.service.v1.CreateApiNamingRuleResponse;
import org.hypertrace.span.processing.config.service.v1.CreateApiNamingRulesRequest;
import org.hypertrace.span.processing.config.service.v1.CreateApiNamingRulesResponse;
import org.hypertrace.span.processing.config.service.v1.CreateExcludeSpanRuleRequest;
import org.hypertrace.span.processing.config.service.v1.CreateExcludeSpanRuleResponse;
import org.hypertrace.span.processing.config.service.v1.DeleteApiNamingRuleRequest;
import org.hypertrace.span.processing.config.service.v1.DeleteApiNamingRuleResponse;
import org.hypertrace.span.processing.config.service.v1.DeleteExcludeSpanRuleRequest;
import org.hypertrace.span.processing.config.service.v1.DeleteExcludeSpanRuleResponse;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRule;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRuleDetails;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRuleInfo;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRuleMetadata;
import org.hypertrace.span.processing.config.service.v1.GetAllApiNamingRulesRequest;
import org.hypertrace.span.processing.config.service.v1.GetAllApiNamingRulesResponse;
import org.hypertrace.span.processing.config.service.v1.GetAllExcludeSpanRulesRequest;
import org.hypertrace.span.processing.config.service.v1.GetAllExcludeSpanRulesResponse;
import org.hypertrace.span.processing.config.service.v1.SpanProcessingConfigServiceGrpc;
import org.hypertrace.span.processing.config.service.v1.UpdateApiNamingRule;
import org.hypertrace.span.processing.config.service.v1.UpdateApiNamingRuleRequest;
import org.hypertrace.span.processing.config.service.v1.UpdateApiNamingRuleResponse;
import org.hypertrace.span.processing.config.service.v1.UpdateExcludeSpanRule;
import org.hypertrace.span.processing.config.service.v1.UpdateExcludeSpanRuleRequest;
import org.hypertrace.span.processing.config.service.v1.UpdateExcludeSpanRuleResponse;
import org.hypertrace.span.processing.config.service.validation.SpanProcessingConfigRequestValidator;

@Slf4j
class SpanProcessingConfigServiceImpl
    extends SpanProcessingConfigServiceGrpc.SpanProcessingConfigServiceImplBase {
  private final SpanProcessingConfigRequestValidator validator;
  private final ExcludeSpanRulesConfigStore excludeSpanRulesConfigStore;
  private final ApiNamingRulesConfigStore apiNamingRulesConfigStore;
  private final TimestampConverter timestampConverter;

  @Inject
  SpanProcessingConfigServiceImpl(
      ExcludeSpanRulesConfigStore excludeSpanRulesConfigStore,
      ApiNamingRulesConfigStore apiNamingRulesConfigStore,
      SpanProcessingConfigRequestValidator requestValidator,
      TimestampConverter timestampConverter) {
    this.validator = requestValidator;
    this.excludeSpanRulesConfigStore = excludeSpanRulesConfigStore;
    this.apiNamingRulesConfigStore = apiNamingRulesConfigStore;
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
  public void createApiNamingRules(
      CreateApiNamingRulesRequest request,
      StreamObserver<CreateApiNamingRulesResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      // TODO: need to handle priorities
      List<ApiNamingRule> apiNamingRules =
          request.getRulesInfoList().stream()
              .map(this::buildApiNamingRule)
              .collect(Collectors.toUnmodifiableList());

      responseObserver.onNext(
          CreateApiNamingRulesResponse.newBuilder()
              .addAllRulesDetails(
                  buildApiNamingRuleDetails(
                      this.apiNamingRulesConfigStore.upsertObjects(requestContext, apiNamingRules)))
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error creating api naming rules {}", request, exception);
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

  private ApiNamingRule buildApiNamingRule(ApiNamingRuleInfo apiNamingRuleInfo) {
    return ApiNamingRule.newBuilder()
        .setId(UUID.randomUUID().toString())
        .setRuleInfo(apiNamingRuleInfo)
        .build();
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

  private List<ApiNamingRuleDetails> buildApiNamingRuleDetails(
      List<ContextualConfigObject<ApiNamingRule>> contextualConfigObjects) {
    return contextualConfigObjects.stream()
        .map(this::buildApiNamingRuleDetails)
        .collect(Collectors.toUnmodifiableList());
  }
}
