package ai.traceable.sensitivedata.config.service;

import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.PII_FILTER_CONFIG_RESOURCE;
import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.SENSITIVE_DATA_CONFIGURATION;
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
import com.typesafe.config.Config;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.config.service.v1.GetConfigRequest;
import org.hypertrace.config.service.v1.UpsertConfigRequest;

@Slf4j
public class PiiFilterConfigServiceImpl
    extends PiiFilterConfigServiceGrpc.PiiFilterConfigServiceImplBase {

  static final String DEFAULT_PII_FILTER_CONFIG = "default.pii.filter.config";

  private final ConfigServiceBlockingStub configServiceBlockingStub;
  private final PiiFilterConfig defaultPiiFilterConfig;

  public PiiFilterConfigServiceImpl(
      ConfigServiceBlockingStub configServiceBlockingStub, Config config) {
    this.configServiceBlockingStub = configServiceBlockingStub;
    this.defaultPiiFilterConfig =
        SensitiveDataConfigUtils.toPiiFilterConfig(config.getConfig(DEFAULT_PII_FILTER_CONFIG));
  }

  @Override
  public void getPiiFilterConfig(
      GetPiiFilterConfigRequest request,
      StreamObserver<GetPiiFilterConfigResponse> responseObserver) {
    try {
      GetConfigRequest getConfigRequest =
          GetConfigRequest.newBuilder()
              .setResourceName(PII_FILTER_CONFIG_RESOURCE)
              .setResourceNamespace(SENSITIVE_DATA_CONFIGURATION)
              .build();
      PiiFilterConfig piiFilterConfig =
          toPiiFilterConfig(getConfig(configServiceBlockingStub, getConfigRequest));
      PiiFilterConfig resultingPiiFilterConfig =
          PiiFilterConfig.newBuilder(defaultPiiFilterConfig)
              .clearKeyRegexs()
              .addAllKeyRegexs(piiFilterConfig.getKeyRegexsList())
              .addAllKeyRegexs(defaultPiiFilterConfig.getKeyRegexsList())
              .build();
      GetPiiFilterConfigResponse response =
          GetPiiFilterConfigResponse.newBuilder()
              .setPiiFilterConfig(resultingPiiFilterConfig)
              .build();
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
              .setResourceNamespace(SENSITIVE_DATA_CONFIGURATION)
              .setConfig(toValue(request.getPiiFilterConfig()))
              .build();
      upsertConfig(configServiceBlockingStub, upsertConfigRequest);
      UpsertPiiFilterConfigResponse response = UpsertPiiFilterConfigResponse.newBuilder().build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Upsert PII Filter Config RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }
}
