package ai.traceable.threatmanagement.config.service;

import ai.traceable.threatmanagement.config.service.anomalyscore.AnomalyScoreContributionManager;
import ai.traceable.threatmanagement.config.service.eventscore.SecurityEventScoreContributionManager;
import ai.traceable.threatmanagement.config.service.eventtype.SecurityEventTypeContributionManager;
import ai.traceable.threatmanagement.config.service.threatautoblocking.ThreatAutoBlockingManager;
import ai.traceable.threatmanagement.config.service.threatscore.ThreatScoreManager;
import ai.traceable.threatmanagement.config.service.v1.AnomalyScoreContribution;
import ai.traceable.threatmanagement.config.service.v1.GetAnomalyScoreContributionRequest;
import ai.traceable.threatmanagement.config.service.v1.GetAnomalyScoreContributionResponse;
import ai.traceable.threatmanagement.config.service.v1.GetSecurityEventScoreContributionRequest;
import ai.traceable.threatmanagement.config.service.v1.GetSecurityEventScoreContributionResponse;
import ai.traceable.threatmanagement.config.service.v1.GetSecurityEventTypeContributionRequest;
import ai.traceable.threatmanagement.config.service.v1.GetSecurityEventTypeContributionResponse;
import ai.traceable.threatmanagement.config.service.v1.GetThreatAutoBlockingConfigRequest;
import ai.traceable.threatmanagement.config.service.v1.GetThreatAutoBlockingConfigResponse;
import ai.traceable.threatmanagement.config.service.v1.GetThreatScoreBoundRequest;
import ai.traceable.threatmanagement.config.service.v1.GetThreatScoreBoundResponse;
import ai.traceable.threatmanagement.config.service.v1.SecurityEventScoreContribution;
import ai.traceable.threatmanagement.config.service.v1.SecurityEventTypeContribution;
import ai.traceable.threatmanagement.config.service.v1.ThreatAutoBlockingActionConfig;
import ai.traceable.threatmanagement.config.service.v1.ThreatManagementConfigServiceGrpc.ThreatManagementConfigServiceImplBase;
import ai.traceable.threatmanagement.config.service.v1.ThreatScoreBound;
import ai.traceable.threatmanagement.config.service.v1.UpdateAnomalyScoreContributionRequest;
import ai.traceable.threatmanagement.config.service.v1.UpdateAnomalyScoreContributionResponse;
import ai.traceable.threatmanagement.config.service.v1.UpdateSecurityEventScoreContributionRequest;
import ai.traceable.threatmanagement.config.service.v1.UpdateSecurityEventScoreContributionResponse;
import ai.traceable.threatmanagement.config.service.v1.UpdateSecurityEventTypeContributionRequest;
import ai.traceable.threatmanagement.config.service.v1.UpdateSecurityEventTypeContributionResponse;
import ai.traceable.threatmanagement.config.service.v1.UpdateThreatAutoBlockingConfigRequest;
import ai.traceable.threatmanagement.config.service.v1.UpdateThreatAutoBlockingConfigResponse;
import ai.traceable.threatmanagement.config.service.v1.UpdateThreatScoreBoundRequest;
import ai.traceable.threatmanagement.config.service.v1.UpdateThreatScoreBoundResponse;
import com.google.inject.Inject;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class ThreatManagementConfigServiceImpl extends ThreatManagementConfigServiceImplBase {
  private final ThreatManagementConfigRequestValidator requestValidator;
  private final ThreatScoreManager threatScoreManager;
  private final SecurityEventScoreContributionManager securityEventScoreContributionManager;
  private final AnomalyScoreContributionManager anomalyScoreContributionManager;
  private final SecurityEventTypeContributionManager securityEventTypeContributionManager;
  private final ThreatAutoBlockingManager threatAutoBlockingManager;

  @Inject
  ThreatManagementConfigServiceImpl(
      ThreatManagementConfigRequestValidator requestValidator,
      ThreatScoreManager threatScoreManager,
      SecurityEventScoreContributionManager securityEventScoreContributionManager,
      AnomalyScoreContributionManager anomalyScoreContributionManager,
      SecurityEventTypeContributionManager securityEventTypeContributionManager,
      ThreatAutoBlockingManager threatAutoBlockingManager) {
    this.requestValidator = requestValidator;
    this.threatScoreManager = threatScoreManager;
    this.securityEventScoreContributionManager = securityEventScoreContributionManager;
    this.anomalyScoreContributionManager = anomalyScoreContributionManager;
    this.securityEventTypeContributionManager = securityEventTypeContributionManager;
    this.threatAutoBlockingManager = threatAutoBlockingManager;
  }

  @Override
  public void getThreatScoreBound(
      GetThreatScoreBoundRequest request,
      StreamObserver<GetThreatScoreBoundResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      requestValidator.validateOrThrow(requestContext, request);

      ThreatScoreBound threatScoreBound = threatScoreManager.getThreatScoreBound(requestContext);
      ThreatScoreBound defaultThreatScoreBound = threatScoreManager.getDefaultThreatScoreBound();

      responseObserver.onNext(
          GetThreatScoreBoundResponse.newBuilder()
              .setThreatScoreBound(threatScoreBound)
              .setDefaultThreatScoreBound(defaultThreatScoreBound)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to get threat score bound for request: {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateThreatScoreBound(
      UpdateThreatScoreBoundRequest request,
      StreamObserver<UpdateThreatScoreBoundResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      requestValidator.validateOrThrow(requestContext, request);

      ThreatScoreBound threatScoreBound =
          threatScoreManager.upsertThreatScoreBound(requestContext, request.getThreatScoreBound());
      ThreatScoreBound defaultThreatScoreBound = threatScoreManager.getDefaultThreatScoreBound();

      responseObserver.onNext(
          UpdateThreatScoreBoundResponse.newBuilder()
              .setThreatScoreBound(threatScoreBound)
              .setDefaultThreatScoreBound(defaultThreatScoreBound)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to update threat score bound for request {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getSecurityEventScoreContribution(
      GetSecurityEventScoreContributionRequest request,
      StreamObserver<GetSecurityEventScoreContributionResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      requestValidator.validateOrThrow(requestContext, request);

      SecurityEventScoreContribution securityEventScoreContribution =
          securityEventScoreContributionManager.getSecurityEventScoreContribution(requestContext);
      SecurityEventScoreContribution defaultSecurityEventScoreContribution =
          securityEventScoreContributionManager.getDefaultSecurityEventScoreContribution();

      responseObserver.onNext(
          GetSecurityEventScoreContributionResponse.newBuilder()
              .setSecurityEventScoreContribution(securityEventScoreContribution)
              .setDefaultSecurityEventScoreContribution(defaultSecurityEventScoreContribution)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to get security event contribution for request: {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateSecurityEventScoreContribution(
      UpdateSecurityEventScoreContributionRequest request,
      StreamObserver<UpdateSecurityEventScoreContributionResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      requestValidator.validateOrThrow(requestContext, request);

      SecurityEventScoreContribution securityEventScoreContribution =
          securityEventScoreContributionManager.upsertSecurityEventScoreContribution(
              requestContext, request.getSecurityEventScoreContribution());
      SecurityEventScoreContribution defaultSecurityEventScoreContribution =
          securityEventScoreContributionManager.getDefaultSecurityEventScoreContribution();

      responseObserver.onNext(
          UpdateSecurityEventScoreContributionResponse.newBuilder()
              .setSecurityEventScoreContribution(securityEventScoreContribution)
              .setDefaultSecurityEventScoreContribution(defaultSecurityEventScoreContribution)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to update security event score contribution for request {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getAnomalyScoreContribution(
      GetAnomalyScoreContributionRequest request,
      StreamObserver<GetAnomalyScoreContributionResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      requestValidator.validateOrThrow(requestContext, request);

      AnomalyScoreContribution anomalyScoreContribution =
          anomalyScoreContributionManager.getAnomalyScoreContribution(requestContext);
      AnomalyScoreContribution defaultAnomalyScoreContribution =
          anomalyScoreContributionManager.getDefaultAnomalyScoreContribution();

      responseObserver.onNext(
          GetAnomalyScoreContributionResponse.newBuilder()
              .setAnomalyScoreContribution(anomalyScoreContribution)
              .setDefaultAnomalyScoreContribution(defaultAnomalyScoreContribution)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to get anomaly contribution for request: {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateAnomalyScoreContribution(
      UpdateAnomalyScoreContributionRequest request,
      StreamObserver<UpdateAnomalyScoreContributionResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      requestValidator.validateOrThrow(requestContext, request);

      AnomalyScoreContribution anomalyScoreContribution =
          anomalyScoreContributionManager.upsertAnomalyScoreContribution(
              requestContext, request.getAnomalyScoreContribution());
      AnomalyScoreContribution defaultAnomalyScoreContribution =
          anomalyScoreContributionManager.getDefaultAnomalyScoreContribution();

      responseObserver.onNext(
          UpdateAnomalyScoreContributionResponse.newBuilder()
              .setAnomalyScoreContribution(anomalyScoreContribution)
              .setDefaultAnomalyScoreContribution(defaultAnomalyScoreContribution)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to update anomaly score contribution for request {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getSecurityEventTypeContribution(
      GetSecurityEventTypeContributionRequest request,
      StreamObserver<GetSecurityEventTypeContributionResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      requestValidator.validateOrThrow(requestContext, request);

      SecurityEventTypeContribution securityEventTypeContribution =
          securityEventTypeContributionManager.getSecurityEventTypeContribution(requestContext);

      responseObserver.onNext(
          GetSecurityEventTypeContributionResponse.newBuilder()
              .setSecurityEventTypeContribution(securityEventTypeContribution)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to get security type contribution for request: {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateSecurityEventTypeContribution(
      UpdateSecurityEventTypeContributionRequest request,
      StreamObserver<UpdateSecurityEventTypeContributionResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      requestValidator.validateOrThrow(requestContext, request);

      SecurityEventTypeContribution securityEventTypeContribution =
          securityEventTypeContributionManager.upsertSecurityEventTypeContribution(
              requestContext, request.getSecurityEventTypeContribution());

      responseObserver.onNext(
          UpdateSecurityEventTypeContributionResponse.newBuilder()
              .setSecurityEventTypeContribution(securityEventTypeContribution)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to update security event type contribution for request {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getThreatAutoBlockingConfig(
      GetThreatAutoBlockingConfigRequest request,
      StreamObserver<GetThreatAutoBlockingConfigResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      requestValidator.validateOrThrow(requestContext, request);

      ThreatAutoBlockingActionConfig threatAutoBlockingActionConfig =
          threatAutoBlockingManager.getThreatAutoBlockingAction(requestContext);

      responseObserver.onNext(
          GetThreatAutoBlockingConfigResponse.newBuilder()
              .setAutoBlockingActionConfig(threatAutoBlockingActionConfig)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to get threat auto blocking action config for request: {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateThreatAutoBlockingConfig(
      UpdateThreatAutoBlockingConfigRequest request,
      StreamObserver<UpdateThreatAutoBlockingConfigResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      requestValidator.validateOrThrow(requestContext, request);

      ThreatAutoBlockingActionConfig threatAutoBlockingActionConfig =
          threatAutoBlockingManager.upsertThreatAutoBlockingAction(requestContext, request);

      responseObserver.onNext(
          UpdateThreatAutoBlockingConfigResponse.newBuilder()
              .setAutoBlockingActionConfig(threatAutoBlockingActionConfig)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unable to update threat auto blocking action config for request {}", request, e);
      responseObserver.onError(e);
    }
  }
}
