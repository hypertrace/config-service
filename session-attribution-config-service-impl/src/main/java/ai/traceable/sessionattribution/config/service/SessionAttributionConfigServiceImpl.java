package ai.traceable.sessionattribution.config.service;

import ai.traceable.sessionattribution.config.service.store.SessionAttributionRuleGenerator;
import ai.traceable.sessionattribution.config.service.store.SessionAttributionRuleStore;
import ai.traceable.sessionattribution.config.service.v1.CreateSessionAttributionRuleRequest;
import ai.traceable.sessionattribution.config.service.v1.CreateSessionAttributionRuleResponse;
import ai.traceable.sessionattribution.config.service.v1.DeleteSessionAttributionRuleRequest;
import ai.traceable.sessionattribution.config.service.v1.DeleteSessionAttributionRuleResponse;
import ai.traceable.sessionattribution.config.service.v1.GetSessionAttributionRulesRequest;
import ai.traceable.sessionattribution.config.service.v1.GetSessionAttributionRulesResponse;
import ai.traceable.sessionattribution.config.service.v1.SessionAttributionConfigServiceGrpc;
import ai.traceable.sessionattribution.config.service.v1.SessionAttributionRule;
import ai.traceable.sessionattribution.config.service.v1.UpdateSessionAttributionRuleRequest;
import ai.traceable.sessionattribution.config.service.v1.UpdateSessionAttributionRuleResponse;
import ai.traceable.sessionattribution.config.service.validation.SessionAttributionConfigRequestValidator;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class SessionAttributionConfigServiceImpl
    extends SessionAttributionConfigServiceGrpc.SessionAttributionConfigServiceImplBase {
  private final SessionAttributionConfigRequestValidator validator;
  private final SessionAttributionRuleStore ruleStore;
  private final SessionAttributionRuleGenerator ruleGenerator;

  @Inject
  SessionAttributionConfigServiceImpl(
      SessionAttributionConfigRequestValidator validator,
      SessionAttributionRuleStore ruleStore,
      SessionAttributionRuleGenerator ruleGenerator) {
    this.validator = validator;
    this.ruleStore = ruleStore;
    this.ruleGenerator = ruleGenerator;
  }

  @Override
  public void getSessionAttributionRules(
      GetSessionAttributionRulesRequest request,
      StreamObserver<GetSessionAttributionRulesResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      this.validator.validateGetRequest(requestContext);
      responseObserver.onNext(
          GetSessionAttributionRulesResponse.newBuilder()
              .addAllRules(this.ruleStore.getAllConfigData(requestContext, request.getFilter()))
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      Exception decoratedException = decorateException(requestContext, exception);

      log.warn(
          "Error retrieving session attribution rules for customer with request context {}",
          requestContext,
          decoratedException);
      responseObserver.onError(decoratedException);
    }
  }

  @Override
  public void createSessionAttributionRule(
      CreateSessionAttributionRuleRequest request,
      StreamObserver<CreateSessionAttributionRuleResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      this.validator.validateCreateRequest(requestContext, request);

      SessionAttributionRule newRule = this.ruleGenerator.generateNewRuleFromCreateRequest(request);

      this.ruleStore.upsertObject(requestContext, newRule);
      responseObserver.onNext(
          CreateSessionAttributionRuleResponse.newBuilder().setRule(newRule).build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      Exception decoratedException = decorateException(requestContext, exception);

      log.warn(
          "Error creating session attribution rule {} with request context {}",
          request,
          requestContext,
          decoratedException);
      responseObserver.onError(decoratedException);
    }
  }

  @Override
  public void updateSessionAttributionRule(
      UpdateSessionAttributionRuleRequest request,
      StreamObserver<UpdateSessionAttributionRuleResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      this.validator.validateUpdateRequest(requestContext, request);

      SessionAttributionRule rule = this.ruleGenerator.generateRuleFromUpdateRequest(request);

      responseObserver.onNext(
          UpdateSessionAttributionRuleResponse.newBuilder()
              .setRule(this.ruleStore.upsertObject(requestContext, rule).getData())
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      Exception decoratedException = decorateException(requestContext, exception);

      log.warn(
          "Error updating session attribution rule: {} with request context {}",
          request,
          requestContext,
          decoratedException);
      responseObserver.onError(decoratedException);
    }
  }

  @Override
  public void deleteSessionAttributionRule(
      DeleteSessionAttributionRuleRequest request,
      StreamObserver<DeleteSessionAttributionRuleResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      this.validator.validateDeleteRequest(requestContext, request);
      this.ruleStore
          .deleteObject(requestContext, request.getId())
          .orElseThrow(Status.NOT_FOUND::asRuntimeException);

      responseObserver.onNext(DeleteSessionAttributionRuleResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      Exception decoratedException = decorateException(requestContext, exception);

      log.warn(
          "Error deleting session attribution rule: {} with request context {}",
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
