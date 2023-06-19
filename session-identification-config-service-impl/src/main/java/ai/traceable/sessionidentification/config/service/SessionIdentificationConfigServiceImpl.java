package ai.traceable.sessionidentification.config.service;

import ai.traceable.sessionidentification.config.service.store.SessionIdentificationRuleGenerator;
import ai.traceable.sessionidentification.config.service.store.SessionIdentificationRuleStore;
import ai.traceable.sessionidentification.config.service.v1.CreateSessionIdentificationRuleRequest;
import ai.traceable.sessionidentification.config.service.v1.CreateSessionIdentificationRuleResponse;
import ai.traceable.sessionidentification.config.service.v1.DeleteSessionIdentificationRuleRequest;
import ai.traceable.sessionidentification.config.service.v1.DeleteSessionIdentificationRuleResponse;
import ai.traceable.sessionidentification.config.service.v1.GetSessionIdentificationRulesRequest;
import ai.traceable.sessionidentification.config.service.v1.GetSessionIdentificationRulesResponse;
import ai.traceable.sessionidentification.config.service.v1.SessionIdentificationConfigServiceGrpc;
import ai.traceable.sessionidentification.config.service.v1.SessionIdentificationRule;
import ai.traceable.sessionidentification.config.service.v1.UpdateSessionIdentificationRuleRequest;
import ai.traceable.sessionidentification.config.service.v1.UpdateSessionIdentificationRuleResponse;
import ai.traceable.sessionidentification.config.service.validation.SessionIdentificationConfigRequestValidator;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class SessionIdentificationConfigServiceImpl
    extends SessionIdentificationConfigServiceGrpc.SessionIdentificationConfigServiceImplBase {
  private final SessionIdentificationConfigRequestValidator validator;
  private final SessionIdentificationRuleStore ruleStore;
  private final SessionIdentificationRuleGenerator ruleGenerator;

  @Inject
  SessionIdentificationConfigServiceImpl(
      SessionIdentificationConfigRequestValidator validator,
      SessionIdentificationRuleStore ruleStore,
      SessionIdentificationRuleGenerator ruleGenerator) {
    this.validator = validator;
    this.ruleStore = ruleStore;
    this.ruleGenerator = ruleGenerator;
  }

  @Override
  public void getSessionIdentificationRules(
      GetSessionIdentificationRulesRequest request,
      StreamObserver<GetSessionIdentificationRulesResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      this.validator.validateGetRequest(requestContext);
      responseObserver.onNext(
          GetSessionIdentificationRulesResponse.newBuilder()
              .addAllRules(this.ruleStore.getAllConfigData(requestContext, request.getFilter()))
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      Exception decoratedException = decorateException(requestContext, exception);

      log.warn(
          "Error retrieving session Identification rules for customer with request context {}",
          requestContext,
          decoratedException);
      responseObserver.onError(decoratedException);
    }
  }

  @Override
  public void createSessionIdentificationRule(
      CreateSessionIdentificationRuleRequest request,
      StreamObserver<CreateSessionIdentificationRuleResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      this.validator.validateCreateRequest(requestContext, request);

      SessionIdentificationRule newRule =
          this.ruleGenerator.generateNewRuleFromCreateRequest(request);

      this.ruleStore.upsertObject(requestContext, newRule);
      responseObserver.onNext(
          CreateSessionIdentificationRuleResponse.newBuilder().setRule(newRule).build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      Exception decoratedException = decorateException(requestContext, exception);

      log.warn(
          "Error creating session Identification rule {} with request context {}",
          request,
          requestContext,
          decoratedException);
      responseObserver.onError(decoratedException);
    }
  }

  @Override
  public void updateSessionIdentificationRule(
      UpdateSessionIdentificationRuleRequest request,
      StreamObserver<UpdateSessionIdentificationRuleResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      this.validator.validateUpdateRequest(requestContext, request);

      SessionIdentificationRule rule = this.ruleGenerator.generateRuleFromUpdateRequest(request);

      responseObserver.onNext(
          UpdateSessionIdentificationRuleResponse.newBuilder()
              .setRule(this.ruleStore.upsertObject(requestContext, rule).getData())
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      Exception decoratedException = decorateException(requestContext, exception);

      log.warn(
          "Error updating session Identification rule: {} with request context {}",
          request,
          requestContext,
          decoratedException);
      responseObserver.onError(decoratedException);
    }
  }

  @Override
  public void deleteSessionIdentificationRule(
      DeleteSessionIdentificationRuleRequest request,
      StreamObserver<DeleteSessionIdentificationRuleResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      this.validator.validateDeleteRequest(requestContext, request);
      this.ruleStore
          .deleteObject(requestContext, request.getId())
          .orElseThrow(Status.NOT_FOUND::asRuntimeException);

      responseObserver.onNext(DeleteSessionIdentificationRuleResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      Exception decoratedException = decorateException(requestContext, exception);

      log.warn(
          "Error deleting session Identification rule: {} with request context {}",
          request,
          requestContext,
          decoratedException);
      responseObserver.onError(decoratedException);
    }
  }

  private Exception decorateException(RequestContext requestContext, Exception exception) {
    return Status.fromThrowable(exception)
        .withCause(exception)
        .asException(requestContext.buildTrailers());
  }
}
