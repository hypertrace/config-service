package ai.traceable.mock.config.service;

import ai.traceable.external.agent.attribute.config.service.v1.ExternalAgentAttributeConfigServiceGrpc;
import ai.traceable.external.agent.attribute.config.service.v1.GetAgentAttributeRulesRequest;
import ai.traceable.external.agent.attribute.config.service.v1.GetAgentAttributeRulesResponse;
import com.google.protobuf.util.JsonFormat;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class ExternalAgentAttributeConfigServiceImpl
    extends ExternalAgentAttributeConfigServiceGrpc.ExternalAgentAttributeConfigServiceImplBase {

  @Override
  public void getAgentAttributeRules(
      GetAgentAttributeRulesRequest request,
      StreamObserver<GetAgentAttributeRulesResponse> responseObserver) {
    try {
      String environmentName =
          request.getScope().hasEnvironmentName()
              ? request.getScope().getEnvironmentName()
              : "default";
      // Set to "default" if environmentName is not "envA" or "envB" -> required for MATS
      List<String> allowedEnvironments = Arrays.asList("envA", "envB");
      if (!allowedEnvironments.contains(environmentName)) {
        environmentName = "default";
      }
      log.debug("Returning External Agent Attribute configs from environment: {}", environmentName);
      String resourceName = environmentName + "/external-agent-attribute-config-rule.json";
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
