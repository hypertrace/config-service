package ai.traceable.mock.config.service;

import ai.traceable.blocking.config.service.v2.BlockingConfigServiceGrpc.BlockingConfigServiceImplBase;
import ai.traceable.blocking.config.service.v2.GetBlockingRulesRequest;
import ai.traceable.blocking.config.service.v2.GetBlockingRulesResponse;
import com.google.protobuf.util.JsonFormat;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class BlockingConfigServiceV2Impl extends BlockingConfigServiceImplBase {

  @Override
  public void getBlockingRules(
      GetBlockingRulesRequest request, StreamObserver<GetBlockingRulesResponse> responseObserver) {
    try {
      String environmentName = request.getEnvironment();
      // Set to "default" if environmentName is not "envA" or "envB" -> required for MATS
      List<String> allowedEnvironments = Arrays.asList("envA", "envB");
      if (!allowedEnvironments.contains(environmentName)) {
        environmentName = "default";
      }
      log.debug("Returning Blockingv2 Configs from environment: {}", environmentName);
      String resourceName = environmentName + "/blocking-config-v2-rule.json";
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
