package ai.traceable.mock.config.service;

import ai.traceable.localprocessing.config.service.v1.*;
import ai.traceable.localprocessing.config.service.v1.LocalProcessingConfigServiceGrpc.LocalProcessingConfigServiceImplBase;
import com.google.protobuf.util.JsonFormat;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LocalProcessingConfigServiceImpl extends LocalProcessingConfigServiceImplBase {

  private static final String DEFAULT_ENV = "default";

  @Override
  public void getLocalProcessingConfig(
      GetLocalProcessingConfigRequest request,
      StreamObserver<GetLocalProcessingConfigResponse> responseObserver) {
    try {
      GetLocalProcessingConfigResponse.Builder builder =
          GetLocalProcessingConfigResponse.newBuilder();
      String environmentName = request.hasEnvironment() ? request.getEnvironment() : DEFAULT_ENV;
      // Set to "default" if environmentName is not "envA" or "envB" -> required for MATS
      List<String> allowedEnvironments = Arrays.asList("envA", "envB");
      if (!allowedEnvironments.contains(environmentName)) {
        environmentName = DEFAULT_ENV;
      }
      log.debug("Returning Local Processing Configs from environment: {}", environmentName);
      String resourceName = environmentName + "/get-local-processing-config-rule.json";
      Utils util = new Utils();
      String json = util.readJson(resourceName);
      JsonFormat.parser().merge(json, builder);
      responseObserver.onNext(builder.build());
      responseObserver.onCompleted();
    } catch (RuntimeException | IOException e) {
      log.error("Get Local Processing Config RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getApiNamingModel(
      GetApiNamingModelRequest request,
      StreamObserver<GetApiNamingModelResponse> responseObserver) {
    try {
      GetApiNamingModelResponse.Builder builder = GetApiNamingModelResponse.newBuilder();
      String environmentName = request.hasEnvironment() ? request.getEnvironment() : DEFAULT_ENV;
      // Set to "default" if environmentName is not "envA" or "envB" -> required for MATS
      List<String> allowedEnvironments = Arrays.asList("envA", "envB");
      if (!allowedEnvironments.contains(environmentName)) {
        environmentName = DEFAULT_ENV;
      }
      log.debug("Returning Api Naming Model from environment: {}", environmentName);
      String resourceName = environmentName + "/get-api-naming-rule.json";
      Utils util = new Utils();
      String json = util.readJson(resourceName);
      JsonFormat.parser().merge(json, builder);
      responseObserver.onNext(builder.build());
      responseObserver.onCompleted();
    } catch (RuntimeException | IOException e) {
      log.error("Get Api Naming Model RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getSpanProcessingRules(
      GetSpanProcessingRulesRequest request,
      StreamObserver<GetSpanProcessingRulesResponse> responseObserver) {
    try {
      GetSpanProcessingRulesResponse.Builder builder = GetSpanProcessingRulesResponse.newBuilder();
      String environmentName = request.hasEnvironment() ? request.getEnvironment() : DEFAULT_ENV;
      // Set to "default" if environmentName is not "envA" or "envB" -> required for MATS
      List<String> allowedEnvironments = Arrays.asList("envA", "envB");
      if (!allowedEnvironments.contains(environmentName)) {
        environmentName = DEFAULT_ENV;
      }
      log.debug("Returning Span Processing Configs from environment: {}", environmentName);
      String resourceName = environmentName + "/get-span-processing-rule.json";
      Utils util = new Utils();
      String json = util.readJson(resourceName);
      JsonFormat.parser().merge(json, builder);
      responseObserver.onNext(builder.build());
      responseObserver.onCompleted();
    } catch (RuntimeException | IOException e) {
      log.error("Get Span processing rules RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }
}
