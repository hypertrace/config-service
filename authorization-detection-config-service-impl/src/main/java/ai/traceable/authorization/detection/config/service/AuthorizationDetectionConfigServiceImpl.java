package ai.traceable.authorization.detection.config.service;

import ai.traceable.authorization.detection.config.service.v1.AuthorizationDetectionConfigServiceGrpc.AuthorizationDetectionConfigServiceImplBase;
import ai.traceable.authorization.detection.config.service.v1.GetAuthorizationDetectionRulesRequest;
import ai.traceable.authorization.detection.config.service.v1.GetAuthorizationDetectionRulesResponse;
import com.google.inject.Inject;
import io.grpc.stub.StreamObserver;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor = @__(@Inject))
class AuthorizationDetectionConfigServiceImpl extends AuthorizationDetectionConfigServiceImplBase {

  private final AuthorizationDetectionConfigRequestValidator validator;
  private final AuthorizationDetectionRuleStore ruleStore;

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
}
