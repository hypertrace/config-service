package ai.traceable.anomaly.config.service.exclusion;

import ai.traceable.anomaly.config.service.exclusion.handlers.CreateAnomalyExclusionRuleHandler;
import ai.traceable.anomaly.config.service.exclusion.handlers.DeleteAnomalyExclusionRuleHandler;
import ai.traceable.anomaly.config.service.exclusion.handlers.GetAnomalyExclusionRuleHandler;
import ai.traceable.anomaly.config.service.exclusion.handlers.UpdateAnomalyExclusionRuleHandler;
import ai.traceable.anomaly.config.service.exclusion.validators.CreateAnomalyExclusionRequestValidator;
import ai.traceable.anomaly.config.service.exclusion.validators.DeleteExclusionRuleRequestValidator;
import ai.traceable.anomaly.config.service.exclusion.validators.GetAnomalyExclusionRequestValidator;
import ai.traceable.anomaly.config.service.exclusion.validators.UpdateAnomalyExclusionRequestValidator;
import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyExclusionConfigServiceGrpc.AnomalyExclusionConfigServiceImplBase;
import ai.traceable.anomaly.config.service.v1.exclusion.CreateAnomalyExclusionRuleRequest;
import ai.traceable.anomaly.config.service.v1.exclusion.CreateAnomalyExclusionRuleResponse;
import ai.traceable.anomaly.config.service.v1.exclusion.DeleteAnomalyExclusionRuleRequest;
import ai.traceable.anomaly.config.service.v1.exclusion.DeleteAnomalyExclusionRuleResponse;
import ai.traceable.anomaly.config.service.v1.exclusion.GetAnomalyExclusionRulesRequest;
import ai.traceable.anomaly.config.service.v1.exclusion.GetAnomalyExclusionRulesResponse;
import ai.traceable.anomaly.config.service.v1.exclusion.UpdateAnomalyExclusionRuleRequest;
import ai.traceable.anomaly.config.service.v1.exclusion.UpdateAnomalyExclusionRuleResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class AnomalyExclusionConfigServiceImpl extends AnomalyExclusionConfigServiceImplBase {
  private final CreateAnomalyExclusionRuleHandler createAnomalyExclusionRuleHandler;
  private final CreateAnomalyExclusionRequestValidator createAnomalyExclusionRequestValidator;
  private final UpdateAnomalyExclusionRuleHandler updateAnomalyExclusionRuleHandler;
  private final UpdateAnomalyExclusionRequestValidator updateAnomalyExclusionRequestValidator;
  private final DeleteAnomalyExclusionRuleHandler deleteAnomalyExclusionRuleHandler;
  private final DeleteExclusionRuleRequestValidator deleteExclusionRuleRequestValidator;
  private final GetAnomalyExclusionRuleHandler getAnomalyExclusionRuleHandler;
  private final GetAnomalyExclusionRequestValidator getAnomalyExclusionRequestValidator;

  @Inject
  public AnomalyExclusionConfigServiceImpl(
      CreateAnomalyExclusionRuleHandler createAnomalyExclusionRuleHandler,
      CreateAnomalyExclusionRequestValidator createAnomalyExclusionRequestValidator,
      UpdateAnomalyExclusionRuleHandler updateAnomalyExclusionRuleHandler,
      UpdateAnomalyExclusionRequestValidator updateAnomalyExclusionRequestValidator,
      DeleteAnomalyExclusionRuleHandler deleteAnomalyExclusionRuleHandler,
      DeleteExclusionRuleRequestValidator deleteExclusionRuleRequestValidator,
      GetAnomalyExclusionRuleHandler getAnomalyExclusionRuleHandler,
      GetAnomalyExclusionRequestValidator getAnomalyExclusionRequestValidator) {
    this.createAnomalyExclusionRuleHandler = createAnomalyExclusionRuleHandler;
    this.createAnomalyExclusionRequestValidator = createAnomalyExclusionRequestValidator;
    this.updateAnomalyExclusionRuleHandler = updateAnomalyExclusionRuleHandler;
    this.updateAnomalyExclusionRequestValidator = updateAnomalyExclusionRequestValidator;
    this.deleteAnomalyExclusionRuleHandler = deleteAnomalyExclusionRuleHandler;
    this.deleteExclusionRuleRequestValidator = deleteExclusionRuleRequestValidator;
    this.getAnomalyExclusionRuleHandler = getAnomalyExclusionRuleHandler;
    this.getAnomalyExclusionRequestValidator = getAnomalyExclusionRequestValidator;
  }

  @Override
  public void createAnomalyExclusionRule(
      CreateAnomalyExclusionRuleRequest request,
      StreamObserver<CreateAnomalyExclusionRuleResponse> responseObserver) {
    Status status = createAnomalyExclusionRequestValidator.validate(request);
    if (!status.isOk()) {
      log.error("Unable to create exclusion rule, invalid request {}", status.getDescription());
      responseObserver.onError(status.asException());
      return;
    }
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      CreateAnomalyExclusionRuleResponse response =
          createAnomalyExclusionRuleHandler.createRule(request, requestContext);
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Error creating exclusion rule for {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateAnomalyExclusionRule(
      UpdateAnomalyExclusionRuleRequest request,
      StreamObserver<UpdateAnomalyExclusionRuleResponse> responseObserver) {

    Status status = updateAnomalyExclusionRequestValidator.validate(request);
    if (!status.isOk()) {
      log.error("Unable to update exclusion rule, invalid request {}", status.getDescription());
      responseObserver.onError(status.asException());
      return;
    }
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      UpdateAnomalyExclusionRuleResponse response =
          updateAnomalyExclusionRuleHandler.updateRule(request, requestContext);
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Error updating exclusion rule for {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void deleteAnomalyExclusionRule(
      DeleteAnomalyExclusionRuleRequest request,
      StreamObserver<DeleteAnomalyExclusionRuleResponse> responseObserver) {

    Status status = deleteExclusionRuleRequestValidator.validate(request);
    if (!status.isOk()) {
      log.error("Unable to delete exclusion rule, invalid request {}", status.getDescription());
      responseObserver.onError(status.asException());
      return;
    }
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      DeleteAnomalyExclusionRuleResponse response =
          deleteAnomalyExclusionRuleHandler.deleteRule(request, requestContext);
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Error deleting exclusion rule config", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getAnomalyExclusionRules(
      GetAnomalyExclusionRulesRequest request,
      StreamObserver<GetAnomalyExclusionRulesResponse> responseObserver) {
    Status status = getAnomalyExclusionRequestValidator.validate(request);
    if (!status.isOk()) {
      log.error("Unable to get exclusion rules, invalid request {}", status.getDescription());
      responseObserver.onError(status.asException());
      return;
    }
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      GetAnomalyExclusionRulesResponse response =
          getAnomalyExclusionRuleHandler.getRules(request, requestContext);
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Error getting exclusion rules", e);
      responseObserver.onError(e);
    }
  }
}
