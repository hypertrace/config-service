package ai.traceable.auth.detection.config.service;

import ai.traceable.auth.detection.config.service.v1.AuthDetectionConfigServiceGrpc.AuthDetectionConfigServiceImplBase;
import ai.traceable.auth.detection.config.service.v1.CreateAuthDetectionRuleRequest;
import ai.traceable.auth.detection.config.service.v1.CreateAuthDetectionRuleResponse;
import ai.traceable.auth.detection.config.service.v1.DeleteAuthDetectionRuleRequest;
import ai.traceable.auth.detection.config.service.v1.DeleteAuthDetectionRuleResponse;
import ai.traceable.auth.detection.config.service.v1.GetAuthDetectionRulesRequest;
import ai.traceable.auth.detection.config.service.v1.GetAuthDetectionRulesResponse;
import ai.traceable.auth.detection.config.service.v1.UpdateAuthDetectionRuleRequest;
import ai.traceable.auth.detection.config.service.v1.UpdateAuthDetectionRuleResponse;
import com.google.inject.Inject;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = {@Inject})
class AuthDetectionConfigServiceImpl extends AuthDetectionConfigServiceImplBase {

  private final AuthDetectionConfigRequestValidator validator;
  private final AuthDetectionRuleStore ruleStore;
  private final AuthDetectionConfigRuleBuilder ruleBuilder;

  @Override
  public void getAuthDetectionRules(
      GetAuthDetectionRulesRequest request,
      StreamObserver<GetAuthDetectionRulesResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      validator.validateOrThrow(requestContext, request);
      responseObserver.onNext(
          GetAuthDetectionRulesResponse.newBuilder()
              .addAllRules(this.ruleStore.getAllConfigData(requestContext, request.getFilter()))
              .build());
      responseObserver.onCompleted();
    } catch (Throwable throwable) {
      log.warn(
          String.format(
              "Unable to fetch auth detection rules in context %s with request %s",
              requestContext, request),
          throwable);
      responseObserver.onError(throwable);
    }
  }

  @Override
  public void updateAuthDetectionRule(
      UpdateAuthDetectionRuleRequest request,
      StreamObserver<UpdateAuthDetectionRuleResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      validator.validateOrThrow(requestContext, request);
      responseObserver.onNext(
          UpdateAuthDetectionRuleResponse.newBuilder()
              .setRule(
                  this.ruleStore
                      .upsertObject(requestContext, this.ruleBuilder.build(request))
                      .getData())
              .build());
      responseObserver.onCompleted();
    } catch (Throwable throwable) {
      log.warn(
          String.format(
              "Unable to update auth detection rule in context %s with request %s",
              requestContext, request),
          throwable);
      responseObserver.onError(throwable);
    }
  }

  @Override
  public void createAuthDetectionRule(
      CreateAuthDetectionRuleRequest request,
      StreamObserver<CreateAuthDetectionRuleResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      validator.validateOrThrow(requestContext, request);
      responseObserver.onNext(
          CreateAuthDetectionRuleResponse.newBuilder()
              .setRule(
                  this.ruleStore
                      .upsertObject(requestContext, this.ruleBuilder.build(request))
                      .getData())
              .build());
      responseObserver.onCompleted();
    } catch (Throwable throwable) {
      log.warn(
          String.format(
              "Unable to create auth detection rule in context %s with request %s",
              requestContext, request),
          throwable);
      responseObserver.onError(throwable);
    }
  }

  @Override
  public void deleteAuthDetectionRule(
      DeleteAuthDetectionRuleRequest request,
      StreamObserver<DeleteAuthDetectionRuleResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      validator.validateOrThrow(requestContext, request);
      this.ruleStore
          .deleteObject(requestContext, request.getId())
          .orElseThrow(Status.NOT_FOUND::asRuntimeException);

      responseObserver.onNext(DeleteAuthDetectionRuleResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Throwable throwable) {
      log.warn(
          String.format(
              "Unable to delete auth detection rule in context %s with request %s",
              requestContext, request),
          throwable);
      responseObserver.onError(throwable);
    }
  }
}
