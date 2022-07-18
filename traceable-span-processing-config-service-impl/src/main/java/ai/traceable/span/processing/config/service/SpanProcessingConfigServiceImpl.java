package ai.traceable.span.processing.config.service;

import ai.traceable.span.processing.config.service.protectionspanrules.ProtectionSpanRulesManager;
import ai.traceable.span.processing.config.service.samplingconfigs.SamplingConfigManager;
import ai.traceable.span.processing.config.service.v1.CreateProtectionSpanRuleRequest;
import ai.traceable.span.processing.config.service.v1.CreateProtectionSpanRuleResponse;
import ai.traceable.span.processing.config.service.v1.CreateSamplingConfigRequest;
import ai.traceable.span.processing.config.service.v1.CreateSamplingConfigResponse;
import ai.traceable.span.processing.config.service.v1.DeleteProtectionSpanRuleRequest;
import ai.traceable.span.processing.config.service.v1.DeleteProtectionSpanRuleResponse;
import ai.traceable.span.processing.config.service.v1.DeleteSamplingConfigRequest;
import ai.traceable.span.processing.config.service.v1.DeleteSamplingConfigResponse;
import ai.traceable.span.processing.config.service.v1.GetAllProtectionSpanRulesRequest;
import ai.traceable.span.processing.config.service.v1.GetAllProtectionSpanRulesResponse;
import ai.traceable.span.processing.config.service.v1.GetAllResolvedProtectionSpanRulesRequest;
import ai.traceable.span.processing.config.service.v1.GetAllResolvedProtectionSpanRulesResponse;
import ai.traceable.span.processing.config.service.v1.GetAllResolvedSamplingConfigsRequest;
import ai.traceable.span.processing.config.service.v1.GetAllResolvedSamplingConfigsResponse;
import ai.traceable.span.processing.config.service.v1.GetAllSamplingConfigsRequest;
import ai.traceable.span.processing.config.service.v1.GetAllSamplingConfigsResponse;
import ai.traceable.span.processing.config.service.v1.SpanProcessingConfigServiceGrpc;
import ai.traceable.span.processing.config.service.v1.UpdateProtectionSpanRuleRequest;
import ai.traceable.span.processing.config.service.v1.UpdateProtectionSpanRuleResponse;
import ai.traceable.span.processing.config.service.v1.UpdateSamplingConfigRequest;
import ai.traceable.span.processing.config.service.v1.UpdateSamplingConfigResponse;
import ai.traceable.span.processing.config.service.validation.SpanProcessingConfigRequestValidator;
import com.google.inject.Inject;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class SpanProcessingConfigServiceImpl
    extends SpanProcessingConfigServiceGrpc.SpanProcessingConfigServiceImplBase {

  private final SpanProcessingConfigRequestValidator validator;
  private final SamplingConfigManager samplingConfigManager;
  private final ProtectionSpanRulesManager protectionSpanRulesManager;

  @Inject
  public SpanProcessingConfigServiceImpl(
      SamplingConfigManager samplingConfigManager,
      ProtectionSpanRulesManager protectionSpanRulesManager,
      SpanProcessingConfigRequestValidator requestValidator) {
    this.validator = requestValidator;
    this.protectionSpanRulesManager = protectionSpanRulesManager;
    this.samplingConfigManager = samplingConfigManager;
  }

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
}
