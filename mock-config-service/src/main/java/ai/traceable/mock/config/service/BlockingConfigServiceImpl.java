package ai.traceable.mock.config.service;

import ai.traceable.blocking.config.service.v1.BlockingConfigServiceGrpc.BlockingConfigServiceImplBase;
import ai.traceable.blocking.config.service.v1.GetBlockingRulesRequest;
import ai.traceable.blocking.config.service.v1.GetBlockingRulesResponse;
import com.google.protobuf.util.JsonFormat;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class BlockingConfigServiceImpl extends BlockingConfigServiceImplBase {

  private String resourceName = "config-svc-data/blocking-config-rule.json";

  @Override
  public void getBlockingRules(
      GetBlockingRulesRequest request, StreamObserver<GetBlockingRulesResponse> responseObserver) {
    try {
      GetBlockingRulesResponse.Builder builder = GetBlockingRulesResponse.newBuilder();
      Utils util = new Utils();
      String json = util.readJson(resourceName);
      JsonFormat.parser().merge(json, builder);
      responseObserver.onNext(builder.build());
      responseObserver.onCompleted();
    } catch (RuntimeException | IOException e) {
      log.error("Unable to fetch  Blocking rules", e);
      responseObserver.onError(e);
    }
  }
}
