package ai.traceable.authorization.detection.config.service;

import ai.traceable.authorization.detection.config.service.v1.AuthorizationDetectionConfigServiceGrpc.AuthorizationDetectionConfigServiceImplBase;
import ai.traceable.authorization.detection.config.service.v1.CreateAuthorizationDetectionRuleRequest;
import ai.traceable.authorization.detection.config.service.v1.CreateAuthorizationDetectionRuleResponse;
import ai.traceable.authorization.detection.config.service.v1.DeleteAuthorizationDetectionRuleRequest;
import ai.traceable.authorization.detection.config.service.v1.DeleteAuthorizationDetectionRuleResponse;
import ai.traceable.authorization.detection.config.service.v1.GetAuthorizationDetectionRulesRequest;
import ai.traceable.authorization.detection.config.service.v1.GetAuthorizationDetectionRulesResponse;
import ai.traceable.authorization.detection.config.service.v1.UpdateAuthorizationDetectionRuleRequest;
import ai.traceable.authorization.detection.config.service.v1.UpdateAuthorizationDetectionRuleResponse;
import com.google.inject.Inject;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = {@Inject})
class AuthorizationDetectionConfigServiceImpl extends AuthorizationDetectionConfigServiceImplBase {

  private final AuthorizationDetectionConfigRequestValidator validator;
  private final AuthorizationDetectionRuleStore ruleStore;
  private final AuthorizationDetectionConfigRuleBuilder ruleBuilder;

  @Override
  public void getAuthorizationDetectionRules(
      GetAuthorizationDetectionRulesRequest request,
      StreamObserver<GetAuthorizationDetectionRulesResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      validator.validateOrThrow(requestContext, request);
      responseObserver.onNext(
          GetAuthorizationDetectionRulesResponse.newBuilder()
              .addAllRules(this.ruleStore.getAllConfigData(requestContext, request.getFilter()))
              .build());
      responseObserver.onCompleted();
    } catch (Throwable throwable) {
      log.warn(
          String.format(
              "Unable to fetch authorization detection rules in context %s with request %s",
              requestContext, request),
          throwable);
      responseObserver.onError(throwable);
    }
  }

  @Override
  public void updateAuthorizationDetectionRule(
      UpdateAuthorizationDetectionRuleRequest request,
      StreamObserver<UpdateAuthorizationDetectionRuleResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      validator.validateOrThrow(requestContext, request);
      responseObserver.onNext(
          UpdateAuthorizationDetectionRuleResponse.newBuilder()
              .setRule(
                  this.ruleStore
                      .upsertObject(requestContext, this.ruleBuilder.build(request))
                      .getData())
              .build());
      responseObserver.onCompleted();
    } catch (Throwable throwable) {
      log.warn(
          String.format(
              "Unable to update authorization detection rule in context %s with request %s",
              requestContext, request),
          throwable);
      responseObserver.onError(throwable);
    }
  }

  @Override
  public void createAuthorizationDetectionRule(
      CreateAuthorizationDetectionRuleRequest request,
      StreamObserver<CreateAuthorizationDetectionRuleResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      validator.validateOrThrow(requestContext, request);
      responseObserver.onNext(
          CreateAuthorizationDetectionRuleResponse.newBuilder()
              .setRule(
                  this.ruleStore
                      .upsertObject(requestContext, this.ruleBuilder.build(request))
                      .getData())
              .build());
      responseObserver.onCompleted();
    } catch (Throwable throwable) {
      log.warn(
          String.format(
              "Unable to create authorization detection rule in context %s with request %s",
              requestContext, request),
          throwable);
      responseObserver.onError(throwable);
    }
  }

  @Override
  public void deleteAuthorizationDetectionRule(
      DeleteAuthorizationDetectionRuleRequest request,
      StreamObserver<DeleteAuthorizationDetectionRuleResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      validator.validateOrThrow(requestContext, request);
      this.ruleStore
          .deleteObject(requestContext, request.getId())
          .orElseThrow(Status.NOT_FOUND::asRuntimeException);

      responseObserver.onNext(DeleteAuthorizationDetectionRuleResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Throwable throwable) {
      log.warn(
          String.format(
              "Unable to delete authorization detection rule in context %s with request %s",
              requestContext, request),
          throwable);
      responseObserver.onError(throwable);
    }
  }
}
