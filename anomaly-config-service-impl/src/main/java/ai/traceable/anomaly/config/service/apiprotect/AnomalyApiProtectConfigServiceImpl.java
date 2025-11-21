package ai.traceable.anomaly.config.service.apiprotect;

import ai.traceable.anomaly.config.service.apiprotect.protection.ApiProtectEvaluationConfigContextManager;
import ai.traceable.anomaly.config.service.apiprotect.validator.ApiProtectValidator;
import ai.traceable.anomaly.config.service.v1.apiprotect.AnomalyApiProtectConfigServiceGrpc.AnomalyApiProtectConfigServiceImplBase;
import ai.traceable.anomaly.config.service.v1.apiprotect.GetApiProtectEvaluationConfigContextRequest;
import ai.traceable.anomaly.config.service.v1.apiprotect.GetApiProtectEvaluationConfigContextResponse;
import ai.traceable.protection.engine.config.apiprotect.v1.ApiProtectionConfigContext;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class AnomalyApiProtectConfigServiceImpl extends AnomalyApiProtectConfigServiceImplBase {
  private final ApiProtectValidator validator;
  private final ApiProtectEvaluationConfigContextManager apiProtectEvaluationConfigContextManager;

  @Inject
  public AnomalyApiProtectConfigServiceImpl(
      ApiProtectValidator validator,
      ApiProtectEvaluationConfigContextManager apiProtectEvaluationConfigContextManager) {
    this.validator = validator;
    this.apiProtectEvaluationConfigContextManager = apiProtectEvaluationConfigContextManager;
  }

  @Override
  public void getApiProtectEvaluationConfigContext(
      GetApiProtectEvaluationConfigContextRequest request,
      StreamObserver<GetApiProtectEvaluationConfigContextResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      validator.validate(request);
      ApiProtectionConfigContext configContext =
          apiProtectEvaluationConfigContextManager.getApiProtectEvaluationConfigContext(
              requestContext, request);
      GetApiProtectEvaluationConfigContextResponse response =
          GetApiProtectEvaluationConfigContextResponse.newBuilder()
              .setApiProtectEvaluationConfigContext(configContext.toByteString())
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Failed during fetching API Protect Evaluation Config Context for request: {}",
          requestContext,
          e);
      responseObserver.onError(e);
    }
  }
}
