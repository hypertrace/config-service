package ai.traceable.threatmanagement.config.service;

import ai.traceable.threatmanagement.config.service.eventscore.SecurityEventScoreContributionManager;
import ai.traceable.threatmanagement.config.service.threatscore.ThreatScoreManager;
import ai.traceable.threatmanagement.config.service.v1.GetSecurityEventScoreContributionRequest;
import ai.traceable.threatmanagement.config.service.v1.GetSecurityEventScoreContributionResponse;
import ai.traceable.threatmanagement.config.service.v1.GetThreatScoreBoundRequest;
import ai.traceable.threatmanagement.config.service.v1.GetThreatScoreBoundResponse;
import ai.traceable.threatmanagement.config.service.v1.SecurityEventScoreContribution;
import ai.traceable.threatmanagement.config.service.v1.ThreatManagementConfigServiceGrpc.ThreatManagementConfigServiceImplBase;
import ai.traceable.threatmanagement.config.service.v1.ThreatScoreBound;
import ai.traceable.threatmanagement.config.service.v1.UpdateSecurityEventScoreContributionRequest;
import ai.traceable.threatmanagement.config.service.v1.UpdateSecurityEventScoreContributionResponse;
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

  @Inject
  ThreatManagementConfigServiceImpl(
      ThreatManagementConfigRequestValidator requestValidator,
      ThreatScoreManager threatScoreManager,
      SecurityEventScoreContributionManager securityEventScoreContributionManager) {
    this.requestValidator = requestValidator;
    this.threatScoreManager = threatScoreManager;
    this.securityEventScoreContributionManager = securityEventScoreContributionManager;
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
    } catch (RuntimeException e) {
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
    } catch (RuntimeException e) {
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
    } catch (RuntimeException e) {
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
    } catch (RuntimeException e) {
      log.error("Unable to update security event contribution for request {}", request, e);
      responseObserver.onError(e);
    }
  }
}
