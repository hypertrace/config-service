package ai.traceable.span.processing.config.service;

import ai.traceable.span.processing.config.service.apinamingrules.ApiNamingRulesManager;
import ai.traceable.span.processing.config.service.protectionspanrules.ProtectionSpanRulesManager;
import ai.traceable.span.processing.config.service.samplingconfigs.SamplingConfigManager;
import ai.traceable.span.processing.config.service.servicenaming.ServiceNamingRulesManager;
import ai.traceable.span.processing.config.service.spaningestionrules.SpanIngestionRulesManager;
import ai.traceable.span.processing.config.service.store.DefaultProtectionSpanRuleEvaluationStatusConfigStore;
import ai.traceable.span.processing.config.service.v1.CreateApiNamingRuleRequest;
import ai.traceable.span.processing.config.service.v1.CreateApiNamingRuleResponse;
import ai.traceable.span.processing.config.service.v1.CreateApiNamingRulesRequest;
import ai.traceable.span.processing.config.service.v1.CreateApiNamingRulesResponse;
import ai.traceable.span.processing.config.service.v1.CreateProtectionSpanRuleRequest;
import ai.traceable.span.processing.config.service.v1.CreateProtectionSpanRuleResponse;
import ai.traceable.span.processing.config.service.v1.CreateSamplingConfigRequest;
import ai.traceable.span.processing.config.service.v1.CreateSamplingConfigResponse;
import ai.traceable.span.processing.config.service.v1.CreateServiceNamingRuleRequest;
import ai.traceable.span.processing.config.service.v1.CreateServiceNamingRuleResponse;
import ai.traceable.span.processing.config.service.v1.CreateSpanIngestionRuleRequest;
import ai.traceable.span.processing.config.service.v1.CreateSpanIngestionRuleResponse;
import ai.traceable.span.processing.config.service.v1.DefaultProtectionSpanRuleEvaluationStatus;
import ai.traceable.span.processing.config.service.v1.DeleteApiNamingRuleRequest;
import ai.traceable.span.processing.config.service.v1.DeleteApiNamingRuleResponse;
import ai.traceable.span.processing.config.service.v1.DeleteApiNamingRulesRequest;
import ai.traceable.span.processing.config.service.v1.DeleteApiNamingRulesResponse;
import ai.traceable.span.processing.config.service.v1.DeleteProtectionSpanRuleRequest;
import ai.traceable.span.processing.config.service.v1.DeleteProtectionSpanRuleResponse;
import ai.traceable.span.processing.config.service.v1.DeleteSamplingConfigRequest;
import ai.traceable.span.processing.config.service.v1.DeleteSamplingConfigResponse;
import ai.traceable.span.processing.config.service.v1.DeleteServiceNamingRuleRequest;
import ai.traceable.span.processing.config.service.v1.DeleteServiceNamingRuleResponse;
import ai.traceable.span.processing.config.service.v1.DeleteSpanIngestionRuleRequest;
import ai.traceable.span.processing.config.service.v1.DeleteSpanIngestionRuleResponse;
import ai.traceable.span.processing.config.service.v1.GetAllApiNamingRulesRequest;
import ai.traceable.span.processing.config.service.v1.GetAllApiNamingRulesResponse;
import ai.traceable.span.processing.config.service.v1.GetAllProtectionSpanRulesRequest;
import ai.traceable.span.processing.config.service.v1.GetAllProtectionSpanRulesResponse;
import ai.traceable.span.processing.config.service.v1.GetAllResolvedProtectionSpanRulesRequest;
import ai.traceable.span.processing.config.service.v1.GetAllResolvedProtectionSpanRulesResponse;
import ai.traceable.span.processing.config.service.v1.GetAllResolvedSamplingConfigsRequest;
import ai.traceable.span.processing.config.service.v1.GetAllResolvedSamplingConfigsResponse;
import ai.traceable.span.processing.config.service.v1.GetAllSamplingConfigsRequest;
import ai.traceable.span.processing.config.service.v1.GetAllSamplingConfigsResponse;
import ai.traceable.span.processing.config.service.v1.GetApiNamingRulesRequest;
import ai.traceable.span.processing.config.service.v1.GetApiNamingRulesResponse;
import ai.traceable.span.processing.config.service.v1.GetDefaultProtectionSpanRuleEvaluationStatusRequest;
import ai.traceable.span.processing.config.service.v1.GetDefaultProtectionSpanRuleEvaluationStatusResponse;
import ai.traceable.span.processing.config.service.v1.GetServiceNamingRulesRequest;
import ai.traceable.span.processing.config.service.v1.GetServiceNamingRulesResponse;
import ai.traceable.span.processing.config.service.v1.GetSpanIngestionConfigRequest;
import ai.traceable.span.processing.config.service.v1.GetSpanIngestionConfigResponse;
import ai.traceable.span.processing.config.service.v1.RankSpanIngestionRuleRequest;
import ai.traceable.span.processing.config.service.v1.RankSpanIngestionRuleResponse;
import ai.traceable.span.processing.config.service.v1.SpanProcessingConfigServiceGrpc;
import ai.traceable.span.processing.config.service.v1.UpdateApiNamingRuleRequest;
import ai.traceable.span.processing.config.service.v1.UpdateApiNamingRuleResponse;
import ai.traceable.span.processing.config.service.v1.UpdateApiNamingRulesRequest;
import ai.traceable.span.processing.config.service.v1.UpdateApiNamingRulesResponse;
import ai.traceable.span.processing.config.service.v1.UpdateDefaultProtectionSpanRuleEvaluationStatusRequest;
import ai.traceable.span.processing.config.service.v1.UpdateDefaultProtectionSpanRuleEvaluationStatusResponse;
import ai.traceable.span.processing.config.service.v1.UpdateProtectionSpanRuleRequest;
import ai.traceable.span.processing.config.service.v1.UpdateProtectionSpanRuleResponse;
import ai.traceable.span.processing.config.service.v1.UpdateSamplingConfigRequest;
import ai.traceable.span.processing.config.service.v1.UpdateSamplingConfigResponse;
import ai.traceable.span.processing.config.service.v1.UpdateServiceNamingRuleRequest;
import ai.traceable.span.processing.config.service.v1.UpdateServiceNamingRuleResponse;
import ai.traceable.span.processing.config.service.v1.UpdateSpanIngestionRuleRequest;
import ai.traceable.span.processing.config.service.v1.UpdateSpanIngestionRuleResponse;
import ai.traceable.span.processing.config.service.validation.SpanProcessingConfigRequestValidator;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import javax.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@RequiredArgsConstructor(onConstructor_ = @Inject)
public class SpanProcessingConfigServiceImpl
    extends SpanProcessingConfigServiceGrpc.SpanProcessingConfigServiceImplBase {

  private final SpanProcessingConfigRequestValidator validator;
  private final SamplingConfigManager samplingConfigManager;
  private final ApiNamingRulesManager apiNamingRulesManager;
  private final ProtectionSpanRulesManager protectionSpanRulesManager;
  private final DefaultProtectionSpanRuleEvaluationStatusConfigStore
      defaultProtectionSpanRuleEvaluationStatusStore;
  private final ServiceNamingRulesManager serviceNamingRulesManager;
  private final SpanIngestionRulesManager spanIngestionRulesManager;
  private static final boolean DEFAULT_PROTECTION_RULE_EVALUATION_STATUS = false;

  @Override
  public void getAllResolvedProtectionSpanRules(
      GetAllResolvedProtectionSpanRulesRequest request,
      StreamObserver<GetAllResolvedProtectionSpanRulesResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      responseObserver.onNext(
          GetAllResolvedProtectionSpanRulesResponse.newBuilder()
              .addAllRules(
                  this.protectionSpanRulesManager.getAllResolvedProtectionSpanRule(requestContext))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to get all resolved protection span rules for request: {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getAllProtectionSpanRules(
      GetAllProtectionSpanRulesRequest request,
      StreamObserver<GetAllProtectionSpanRulesResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      responseObserver.onNext(
          GetAllProtectionSpanRulesResponse.newBuilder()
              .addAllRuleDetails(
                  this.protectionSpanRulesManager.getAllProtectionSpanRuleDetails(requestContext))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to get all protection span rules for request: {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void createProtectionSpanRule(
      CreateProtectionSpanRuleRequest request,
      StreamObserver<CreateProtectionSpanRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      responseObserver.onNext(
          CreateProtectionSpanRuleResponse.newBuilder()
              .setRuleDetails(
                  this.protectionSpanRulesManager.createProtectionSpanRule(requestContext, request))
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error creating protection span rule {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void updateProtectionSpanRule(
      UpdateProtectionSpanRuleRequest request,
      StreamObserver<UpdateProtectionSpanRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      responseObserver.onNext(
          UpdateProtectionSpanRuleResponse.newBuilder()
              .setRuleDetails(
                  this.protectionSpanRulesManager.updateProtectionSpanRule(requestContext, request))
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error updating protection span rule: {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void deleteProtectionSpanRule(
      DeleteProtectionSpanRuleRequest request,
      StreamObserver<DeleteProtectionSpanRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      this.protectionSpanRulesManager.deleteProtectionSpanRule(requestContext, request);

      responseObserver.onNext(DeleteProtectionSpanRuleResponse.newBuilder().build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error deleting protection span rule: {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void getAllResolvedSamplingConfigs(
      GetAllResolvedSamplingConfigsRequest request,
      StreamObserver<GetAllResolvedSamplingConfigsResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      responseObserver.onNext(
          GetAllResolvedSamplingConfigsResponse.newBuilder()
              .addAllSamplingConfigs(
                  this.samplingConfigManager.getAllResolvedSamplingConfigs(requestContext))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to get all resolved sampling configs for request: {}", request, e);
      responseObserver.onError(e);
    }
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
              .addAllSamplingConfigDetails(
                  this.samplingConfigManager.getAllSamplingConfigsDetails(requestContext))
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

      responseObserver.onNext(
          CreateSamplingConfigResponse.newBuilder()
              .setSamplingConfigDetails(
                  this.samplingConfigManager.createSamplingConfig(requestContext, request))
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

      responseObserver.onNext(
          UpdateSamplingConfigResponse.newBuilder()
              .setSamplingConfigDetails(
                  this.samplingConfigManager.updateSamplingConfig(requestContext, request))
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error updating sampling config: {}", request, exception);
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

      this.samplingConfigManager.deleteSamplingConfig(requestContext, request);

      responseObserver.onNext(DeleteSamplingConfigResponse.newBuilder().build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error deleting sampling config: {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void getDefaultProtectionSpanRuleEvaluationStatus(
      GetDefaultProtectionSpanRuleEvaluationStatusRequest request,
      StreamObserver<GetDefaultProtectionSpanRuleEvaluationStatusResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      responseObserver.onNext(
          GetDefaultProtectionSpanRuleEvaluationStatusResponse.newBuilder()
              .setStatus(
                  this.defaultProtectionSpanRuleEvaluationStatusStore
                      .getData(requestContext)
                      .orElse(
                          DefaultProtectionSpanRuleEvaluationStatus.newBuilder()
                              .setEnabled(DEFAULT_PROTECTION_RULE_EVALUATION_STATUS)
                              .build()))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Unable to get default protection span rule evaluation status for request: {}",
          request,
          e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateDefaultProtectionSpanRuleEvaluationStatus(
      UpdateDefaultProtectionSpanRuleEvaluationStatusRequest request,
      StreamObserver<UpdateDefaultProtectionSpanRuleEvaluationStatusResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      DefaultProtectionSpanRuleEvaluationStatus status =
          this.defaultProtectionSpanRuleEvaluationStatusStore
              .upsertObject(requestContext, request.getStatus())
              .getData();

      responseObserver.onNext(
          UpdateDefaultProtectionSpanRuleEvaluationStatusResponse.newBuilder()
              .setStatus(status)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Unable to get update default protection span rule evaluation status for request: {}",
          request,
          e);
      responseObserver.onError(e);
    }
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
              .addAllRuleDetails(apiNamingRulesManager.getAllApiNamingRuleDetails(requestContext))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to get all api naming rules for request: {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getApiNamingRules(
      GetApiNamingRulesRequest request,
      StreamObserver<GetApiNamingRulesResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      responseObserver.onNext(
          GetApiNamingRulesResponse.newBuilder()
              .addAllRuleDetails(
                  apiNamingRulesManager.getApiNamingRuleDetails(
                      requestContext, request.getApiNamingRulesFilter()))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to get api naming rules for request: {}", request, e);
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

      responseObserver.onNext(
          CreateApiNamingRuleResponse.newBuilder()
              .setRuleDetails(apiNamingRulesManager.createApiNamingRule(requestContext, request))
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

      responseObserver.onNext(
          CreateApiNamingRulesResponse.newBuilder()
              .addAllRulesDetails(
                  apiNamingRulesManager.createApiNamingRules(requestContext, request))
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

      responseObserver.onNext(
          UpdateApiNamingRuleResponse.newBuilder()
              .setRuleDetails(apiNamingRulesManager.updateApiNamingRule(requestContext, request))
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error updating api naming rule: {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void updateApiNamingRules(
      UpdateApiNamingRulesRequest request,
      StreamObserver<UpdateApiNamingRulesResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      responseObserver.onNext(
          UpdateApiNamingRulesResponse.newBuilder()
              .addAllRulesDetails(
                  apiNamingRulesManager.updateApiNamingRules(requestContext, request))
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error updating api naming rules: {}", request, exception);
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

      apiNamingRulesManager.deleteApiNamingRule(requestContext, request);

      responseObserver.onNext(DeleteApiNamingRuleResponse.newBuilder().build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error deleting api naming rule: {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void deleteApiNamingRules(
      DeleteApiNamingRulesRequest request,
      StreamObserver<DeleteApiNamingRulesResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.validator.validateOrThrow(requestContext, request);

      apiNamingRulesManager.deleteApiNamingRules(requestContext, request);

      responseObserver.onNext(DeleteApiNamingRulesResponse.newBuilder().build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error deleting api naming rules: {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void getServiceNamingRules(
      GetServiceNamingRulesRequest request,
      StreamObserver<GetServiceNamingRulesResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      responseObserver.onNext(
          GetServiceNamingRulesResponse.newBuilder()
              .addAllRules(
                  this.serviceNamingRulesManager.getRules(requestContext, request.getFilter()))
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.warn("Failed to fetch service naming rules: {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void createServiceNamingRule(
      CreateServiceNamingRuleRequest request,
      StreamObserver<CreateServiceNamingRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      responseObserver.onNext(
          CreateServiceNamingRuleResponse.newBuilder()
              .setRule(this.serviceNamingRulesManager.createRule(requestContext, request))
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.warn("Failed to create service naming rule: {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void updateServiceNamingRule(
      UpdateServiceNamingRuleRequest request,
      StreamObserver<UpdateServiceNamingRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      responseObserver.onNext(
          UpdateServiceNamingRuleResponse.newBuilder()
              .setRule(this.serviceNamingRulesManager.updateRule(requestContext, request))
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.warn("Failed to update service naming rule: {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void deleteServiceNamingRule(
      DeleteServiceNamingRuleRequest request,
      StreamObserver<DeleteServiceNamingRuleResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.serviceNamingRulesManager.deleteRule(requestContext, request.getId());
      responseObserver.onNext(DeleteServiceNamingRuleResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.warn("Failed to delete service naming rule: {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void getSpanIngestionConfig(
      GetSpanIngestionConfigRequest request,
      StreamObserver<GetSpanIngestionConfigResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      responseObserver.onNext(this.spanIngestionRulesManager.getRuleSet(requestContext, request));
      responseObserver.onCompleted();
    } catch (Exception exception) {
      Exception decoratedException = decorateException(requestContext, exception);
      log.warn(
          "Failed to get span ingestion config for request: {} with context: {}",
          request,
          requestContext,
          decoratedException);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void createSpanIngestionRule(
      CreateSpanIngestionRuleRequest request,
      StreamObserver<CreateSpanIngestionRuleResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      responseObserver.onNext(
          CreateSpanIngestionRuleResponse.newBuilder()
              .setRule(this.spanIngestionRulesManager.createRule(requestContext, request))
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      Exception decoratedException = decorateException(requestContext, exception);
      log.warn(
          "Failed to create span ingestion rule for request: {} with context: {}",
          request,
          requestContext,
          decoratedException);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void updateSpanIngestionRule(
      UpdateSpanIngestionRuleRequest request,
      StreamObserver<UpdateSpanIngestionRuleResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      responseObserver.onNext(
          UpdateSpanIngestionRuleResponse.newBuilder()
              .setRule(this.spanIngestionRulesManager.updateRule(requestContext, request))
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      Exception decoratedException = decorateException(requestContext, exception);
      log.warn(
          "Failed to update span ingestion rule for request: {} with context: {}",
          request,
          requestContext,
          decoratedException);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void deleteSpanIngestionRule(
      DeleteSpanIngestionRuleRequest request,
      StreamObserver<DeleteSpanIngestionRuleResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      this.spanIngestionRulesManager.deleteRule(requestContext, request);
      responseObserver.onNext(DeleteSpanIngestionRuleResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      Exception decoratedException = decorateException(requestContext, exception);
      log.warn(
          "Failed to delete span ingestion rule for request: {} with context: {}",
          request,
          requestContext,
          decoratedException);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void rankSpanIngestionRule(
      RankSpanIngestionRuleRequest request,
      StreamObserver<RankSpanIngestionRuleResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      this.spanIngestionRulesManager.rankRules(requestContext, request);
      responseObserver.onNext(RankSpanIngestionRuleResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      Exception decoratedException = decorateException(requestContext, exception);
      log.warn(
          "Failed to rank span ingestion rule for request: {} with context: {}",
          request,
          requestContext,
          decoratedException);
      responseObserver.onError(exception);
    }
  }

  private Exception decorateException(RequestContext requestContext, Exception exception) {
    return Status.fromThrowable(exception)
        .withCause(exception)
        .asRuntimeException(requestContext.buildTrailers());
  }
}
