package ai.traceable.mock.config.service;

import ai.traceable.sensitivedata.config.service.v1.GetPiiFilterConfigRequest;
import ai.traceable.sensitivedata.config.service.v1.GetPiiFilterConfigResponse;
import ai.traceable.sensitivedata.config.service.v1.PiiFilterConfigServiceGrpc;
import com.google.protobuf.util.JsonFormat;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class PiiFilterConfigServiceImpl extends PiiFilterConfigServiceGrpc.PiiFilterConfigServiceImplBase {

  private String resourceName = "config-svc-data/pii-filter-config-rule.json";

  @Override
  public void getPiiFilterConfig(
      GetPiiFilterConfigRequest request,
      StreamObserver<GetPiiFilterConfigResponse> responseObserver) {
    try {
      GetPiiFilterConfigResponse.Builder builder = GetPiiFilterConfigResponse.newBuilder();
      Utils util = new Utils();
      String json = util.readJson(resourceName);
      JsonFormat.parser().merge(json, builder);
      responseObserver.onNext(builder.build());
      responseObserver.onCompleted();
    } catch (RuntimeException | IOException e) {
      log.error("Unable to get piiFilter rules", e);
      responseObserver.onError(e);
    }
  }
}
