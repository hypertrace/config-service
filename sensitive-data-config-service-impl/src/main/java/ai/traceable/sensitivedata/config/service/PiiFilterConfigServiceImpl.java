package ai.traceable.sensitivedata.config.service;

import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.PII_FILTER_CONFIG_RESOURCE;
import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.SENSITIVE_PARAMETERS_NAMESPACE;
import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.getConfig;
import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.toPiiFilterConfig;
import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.toValue;
import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.upsertConfig;

import ai.traceable.sensitivedata.config.service.v1.GetPiiFilterConfigRequest;
import ai.traceable.sensitivedata.config.service.v1.GetPiiFilterConfigResponse;
import ai.traceable.sensitivedata.config.service.v1.PiiFilterConfig;
import ai.traceable.sensitivedata.config.service.v1.PiiFilterConfigServiceGrpc;
import ai.traceable.sensitivedata.config.service.v1.UpsertPiiFilterConfigRequest;
import ai.traceable.sensitivedata.config.service.v1.UpsertPiiFilterConfigResponse;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.config.service.v1.GetConfigRequest;
import org.hypertrace.config.service.v1.UpsertConfigRequest;

@Slf4j
public class PiiFilterConfigServiceImpl
    extends PiiFilterConfigServiceGrpc.PiiFilterConfigServiceImplBase {

  private final ConfigServiceBlockingStub configServiceBlockingStub;

  public PiiFilterConfigServiceImpl(ConfigServiceBlockingStub configServiceBlockingStub) {
    this.configServiceBlockingStub = configServiceBlockingStub;
  }

  @Override
  public void getPiiFilterConfig(
      GetPiiFilterConfigRequest request,
      StreamObserver<GetPiiFilterConfigResponse> responseObserver) {
    try {
      GetConfigRequest getConfigRequest =
          GetConfigRequest.newBuilder()
              .setResourceName(PII_FILTER_CONFIG_RESOURCE)
              .setResourceNamespace(SENSITIVE_PARAMETERS_NAMESPACE)
              .build();
      PiiFilterConfig piiFilterConfig =
          toPiiFilterConfig(getConfig(configServiceBlockingStub, getConfigRequest).getConfig());
      GetPiiFilterConfigResponse response =
          GetPiiFilterConfigResponse.newBuilder().setPiiFilterConfig(piiFilterConfig).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Get PII Filter Config RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void upsertPiiFilterConfig(
      UpsertPiiFilterConfigRequest request,
      StreamObserver<UpsertPiiFilterConfigResponse> responseObserver) {
    try {
      UpsertConfigRequest upsertConfigRequest =
          UpsertConfigRequest.newBuilder()
              .setResourceName(PII_FILTER_CONFIG_RESOURCE)
              .setResourceNamespace(SENSITIVE_PARAMETERS_NAMESPACE)
              .setConfig(toValue(request.getPiiFilterConfig()))
              .build();
      upsertConfig(configServiceBlockingStub, upsertConfigRequest);
      UpsertPiiFilterConfigResponse response =
          UpsertPiiFilterConfigResponse.newBuilder().setSuccess(true).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Upsert PII Filter Config RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }
}
