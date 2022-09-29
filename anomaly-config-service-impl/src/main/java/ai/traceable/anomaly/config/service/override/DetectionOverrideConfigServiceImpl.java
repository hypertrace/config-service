package ai.traceable.anomaly.config.service.override;

import ai.traceable.anomaly.config.service.override.exclusion.ExclusionRulesManager;
import ai.traceable.anomaly.config.service.override.exclusion.ExclusionRulesValidator;
import ai.traceable.anomaly.config.service.v1.override.CreateDetectionExclusionRuleRequest;
import ai.traceable.anomaly.config.service.v1.override.CreateDetectionExclusionRuleResponse;
import ai.traceable.anomaly.config.service.v1.override.DeleteDetectionExclusionRuleRequest;
import ai.traceable.anomaly.config.service.v1.override.DeleteDetectionExclusionRuleResponse;
import ai.traceable.anomaly.config.service.v1.override.DetectionOverrideConfigServiceGrpc.DetectionOverrideConfigServiceImplBase;
import ai.traceable.anomaly.config.service.v1.override.GetDetectionExclusionRulesRequest;
import ai.traceable.anomaly.config.service.v1.override.GetDetectionExclusionRulesResponse;
import ai.traceable.anomaly.config.service.v1.override.UpdateDetectionExclusionRuleRequest;
import ai.traceable.anomaly.config.service.v1.override.UpdateDetectionExclusionRuleResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class DetectionOverrideConfigServiceImpl extends DetectionOverrideConfigServiceImplBase {
  private final ExclusionRulesValidator rulesValidator;
  private final ExclusionRulesManager rulesManager;

  @Inject
  DetectionOverrideConfigServiceImpl(
      ExclusionRulesValidator rulesValidator, ExclusionRulesManager rulesManager) {
    this.rulesValidator = rulesValidator;
    this.rulesManager = rulesManager;
  }

  @Override
  public void getDetectionExclusionRules(
      GetDetectionExclusionRulesRequest request,
      StreamObserver<GetDetectionExclusionRulesResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      responseObserver.onNext(
          GetDetectionExclusionRulesResponse.newBuilder()
              .addAllRules(
                  rulesManager.getDetectionExclusionRules(requestContext, request.getFilter()))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Error in fetching detection exclusion rules for request context {} :",
          requestContext.toString(),
          e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void createDetectionExclusionRule(
      CreateDetectionExclusionRuleRequest request,
      StreamObserver<CreateDetectionExclusionRuleResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      Status status = rulesValidator.validate(request);
      if (!status.isOk()) {
        log.error(
            "Create Detection Exclusion Rule Request for request context {} is not valid {}",
            requestContext.toString(),
            status.getDescription());
        responseObserver.onError(status.asException());
        return;
      }

      CreateDetectionExclusionRuleResponse response =
          CreateDetectionExclusionRuleResponse.newBuilder()
              .setRule(rulesManager.createDetectionExclusionRule(requestContext, request))
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error(
          "Error in creating detection exclusion rule for request context {}",
          requestContext.toString(),
          exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void updateDetectionExclusionRule(
      UpdateDetectionExclusionRuleRequest request,
      StreamObserver<UpdateDetectionExclusionRuleResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      Status status = rulesValidator.validate(request);
      if (!status.isOk()) {
        log.error(
            "Update Detection Exclusion Rule Request for request context {} is not valid {}",
            requestContext.toString(),
            status.getDescription());
        responseObserver.onError(status.asException());
        return;
      }

      UpdateDetectionExclusionRuleResponse response =
          UpdateDetectionExclusionRuleResponse.newBuilder()
              .setRule(rulesManager.updateDetectionExclusionRule(requestContext, request.getRule()))
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error(
          "Error in updating detection exclusion rule for request context {}",
          requestContext.toString(),
          exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void deleteDetectionExclusionRule(
      DeleteDetectionExclusionRuleRequest request,
      StreamObserver<DeleteDetectionExclusionRuleResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      Status status = rulesValidator.validate(request);
      if (!status.isOk()) {
        log.error(
            "Delete Detection Exclusion Rule Request for request context {} is not valid {}",
            requestContext.toString(),
            status.getDescription());
        responseObserver.onError(status.asException());
        return;
      }

      rulesManager.deleteDetectionExclusionRule(requestContext, request.getRuleId());

      responseObserver.onNext(DeleteDetectionExclusionRuleResponse.newBuilder().build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Unable to delete detection exclusion rule with id {} for request context {}:",
          request.getRuleId(),
          requestContext.toString(),
          e);
      responseObserver.onError(e);
    }
  }
}
