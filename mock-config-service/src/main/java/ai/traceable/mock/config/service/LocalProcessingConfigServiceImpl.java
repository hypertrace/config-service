package ai.traceable.mock.config.service;

import ai.traceable.localprocessing.config.service.v1.*;
import ai.traceable.localprocessing.config.service.v1.LocalProcessingConfigServiceGrpc.LocalProcessingConfigServiceImplBase;
import com.google.protobuf.util.JsonFormat;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LocalProcessingConfigServiceImpl extends LocalProcessingConfigServiceImplBase {

  @Override
  public void getLocalProcessingConfig(
      GetLocalProcessingConfigRequest request,
      StreamObserver<GetLocalProcessingConfigResponse> responseObserver) {
    try {
      GetLocalProcessingConfigResponse.Builder builder =
          GetLocalProcessingConfigResponse.newBuilder();
      String resourceName = "config-svc-data/get-local-processing-config-rule.json";
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
      String resourceName = "config-svc-data/get-api-naming-rule.json";
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
      String resourceName = "config-svc-data/get-span-processing-rule.json";
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
