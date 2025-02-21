package ai.traceable.mock.config.service;

import ai.traceable.external.data.classification.config.service.v1.ExternalDataClassificationServiceGrpc;
import ai.traceable.external.data.classification.config.service.v1.GetDataClassificationConfigRequest;
import ai.traceable.external.data.classification.config.service.v1.GetDataClassificationConfigResponse;
import com.google.protobuf.util.JsonFormat;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ExternalDataClassificationConfigServiceImpl
    extends ExternalDataClassificationServiceGrpc.ExternalDataClassificationServiceImplBase {

  @Override
  public void getDataClassificationConfig(
      GetDataClassificationConfigRequest request,
      StreamObserver<GetDataClassificationConfigResponse> responseObserver) {
    try {
      GetDataClassificationConfigResponse.Builder builder =
          GetDataClassificationConfigResponse.newBuilder();
      Optional<String> requestedEnvironment =
          Optional.of(request.getEnvironmentFilter().getEnvironmentName())
              .filter(envName -> !envName.isBlank());
      String environmentName = requestedEnvironment.orElse("default");
      // Set to "default" if environmentName is not "envA" or "envB" -> required for MATS
      List<String> allowedEnvironments = Arrays.asList("envA", "envB");
      if (!allowedEnvironments.contains(environmentName)) {
        environmentName = "default";
      }
      log.debug(
          "Returning External Data Classification Configs from environment: {}", environmentName);
      String resourceName = environmentName + "/external-data-classification-config-rule.json";
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
