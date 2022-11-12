package ai.traceable.mock.config.service;

import ai.traceable.external.agent.attribute.config.service.v1.ExternalAgentAttributeConfigServiceGrpc;
import ai.traceable.external.agent.attribute.config.service.v1.GetAgentAttributeRulesRequest;
import ai.traceable.external.agent.attribute.config.service.v1.GetAgentAttributeRulesResponse;
import com.google.protobuf.util.JsonFormat;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class ExternalAgentAttributeConfigServiceImpl
    extends ExternalAgentAttributeConfigServiceGrpc.ExternalAgentAttributeConfigServiceImplBase {

  private String resourceName = "config-svc-data/external-agent-attribute-config-rule.json";

  @Override
  public void getAgentAttributeRules(
      GetAgentAttributeRulesRequest request,
      StreamObserver<GetAgentAttributeRulesResponse> responseObserver) {
    try {
      GetAgentAttributeRulesResponse.Builder builder = GetAgentAttributeRulesResponse.newBuilder();
      Utils util = new Utils();
      String json = util.readJson(resourceName);
      JsonFormat.parser().merge(json, builder);
      responseObserver.onNext(builder.build());
      responseObserver.onCompleted();
    } catch (RuntimeException | IOException ex) {
      log.error("Unable to get external agent attribute rules", ex);
      responseObserver.onError(ex);
    }
  }
}
