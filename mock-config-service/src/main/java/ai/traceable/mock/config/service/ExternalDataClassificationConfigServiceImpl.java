package ai.traceable.mock.config.service;

import ai.traceable.external.data.classification.config.service.v1.ExternalDataClassificationServiceGrpc;
import ai.traceable.external.data.classification.config.service.v1.GetDataClassificationConfigRequest;
import ai.traceable.external.data.classification.config.service.v1.GetDataClassificationConfigResponse;
import com.google.protobuf.util.JsonFormat;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ExternalDataClassificationConfigServiceImpl
    extends ExternalDataClassificationServiceGrpc.ExternalDataClassificationServiceImplBase {

  private String resourceName = "config-svc-data/external-data-classification-config-rule.json";

  @Override
  public void getDataClassificationConfig(
      GetDataClassificationConfigRequest request,
      StreamObserver<GetDataClassificationConfigResponse> responseObserver) {
    try {
      GetDataClassificationConfigResponse.Builder builder =
          GetDataClassificationConfigResponse.newBuilder();
      Utils util = new Utils();
      String json = util.readJson(resourceName);
      JsonFormat.parser().merge(json, builder);
      responseObserver.onNext(builder.build());
      responseObserver.onCompleted();
    } catch (RuntimeException | IOException ex) {
      log.error("Unable to get external data classification rules", ex);
      responseObserver.onError(ex);
    }
  }
}
