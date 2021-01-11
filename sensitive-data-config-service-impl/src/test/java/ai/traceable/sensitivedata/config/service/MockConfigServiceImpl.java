package ai.traceable.sensitivedata.config.service;

import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.PARAMETERS_WITH_SENSITIVITY;
import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.PII_FILTER_CONFIG_RESOURCE;
import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.SENSITIVE_DATA_CONFIGURATION;
import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.getContext;
import static ai.traceable.sensitivedata.config.service.TestUtils.ENDPOINT1;
import static ai.traceable.sensitivedata.config.service.TestUtils.getParametersWithSensitivityValue;
import static ai.traceable.sensitivedata.config.service.TestUtils.getPiiFilterConfigValue;
import static ai.traceable.sensitivedata.config.service.v1.ParamType.PARAM_TYPE_BODY;
import static ai.traceable.sensitivedata.config.service.v1.ParamType.PARAM_TYPE_QUERY;

import com.google.protobuf.Value;
import io.grpc.stub.StreamObserver;
import java.util.HashSet;
import java.util.Set;
import lombok.Getter;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.GetConfigRequest;
import org.hypertrace.config.service.v1.GetConfigResponse;
import org.hypertrace.config.service.v1.UpsertConfigRequest;
import org.hypertrace.config.service.v1.UpsertConfigResponse;

@Getter
class MockConfigServiceImpl extends ConfigServiceGrpc.ConfigServiceImplBase {

  private Value upsertedPiiFilterConfig;
  private Set<Value> upsertedBodyParameters = new HashSet<>();
  private Set<Value> upsertedQueryParameters = new HashSet<>();

  @Override
  public void upsertConfig(
      UpsertConfigRequest request, StreamObserver<UpsertConfigResponse> responseObserver) {
    if (request.getResourceNamespace().equals(SENSITIVE_DATA_CONFIGURATION)) {
      if (request.getResourceName().equals(PII_FILTER_CONFIG_RESOURCE)) {
        upsertedPiiFilterConfig = request.getConfig();
      } else if (request.getResourceName().equals(PARAMETERS_WITH_SENSITIVITY)) {
        if (request.getContext().startsWith(PARAM_TYPE_BODY.name())) {
          upsertedBodyParameters.addAll(request.getConfig().getListValue().getValuesList());
        } else if (request.getContext().startsWith(PARAM_TYPE_QUERY.name())) {
          upsertedQueryParameters.addAll(request.getConfig().getListValue().getValuesList());
        }
      }
    }
    responseObserver.onNext(UpsertConfigResponse.newBuilder().build());
    responseObserver.onCompleted();
  }

  @Override
  public void getConfig(
      GetConfigRequest request, StreamObserver<GetConfigResponse> responseObserver) {
    GetConfigResponse getConfigResponse = null;
    if (request.getResourceNamespace().equals(SENSITIVE_DATA_CONFIGURATION)) {
      if (request.getResourceName().equals(PII_FILTER_CONFIG_RESOURCE)) {
        getConfigResponse =
            GetConfigResponse.newBuilder().setConfig(getPiiFilterConfigValue()).build();
      } else if (request.getResourceName().equals(PARAMETERS_WITH_SENSITIVITY)
          && request.getContexts(0).equals(getContext(PARAM_TYPE_BODY, ENDPOINT1))) {
        getConfigResponse =
            GetConfigResponse.newBuilder().setConfig(getParametersWithSensitivityValue()).build();
      }
    }
    responseObserver.onNext(getConfigResponse);
    responseObserver.onCompleted();
  }
}
