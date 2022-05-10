package ai.traceable.mock.config.service;

import ai.traceable.external.userattribution.config.service.v1.ExternalUserAttributionConfigServiceGrpc.ExternalUserAttributionConfigServiceImplBase;
import ai.traceable.external.userattribution.config.service.v1.GetExternalUserAttributionRulesRequest;
import ai.traceable.external.userattribution.config.service.v1.GetExternalUserAttributionRulesResponse;
import com.google.protobuf.util.JsonFormat;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class ExternalUserAttributionConfigServiceImpl
    extends ExternalUserAttributionConfigServiceImplBase {

  private String resourceName = "config-svc-data/external-user-attribution-config-rule.json";

  @Override
  public void getExternalUserAttributionRules(
      GetExternalUserAttributionRulesRequest request,
      StreamObserver<GetExternalUserAttributionRulesResponse> responseObserver) {
    try {
      GetExternalUserAttributionRulesResponse.Builder builder =
          GetExternalUserAttributionRulesResponse.newBuilder();
      Utils util = new Utils();
      String json = util.readJson(resourceName);
      JsonFormat.parser().merge(json, builder);
      responseObserver.onNext(builder.build());
      responseObserver.onCompleted();
    } catch (RuntimeException | IOException ex) {
      log.error("Unable to get external user attribution rules", ex);
      responseObserver.onError(ex);
    }
  }
}
