package ai.traceable.sensitivedata.config.service;

import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.INSENSITIVE_PARAMETERS_RESOURCE;
import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.PII_FILTER_CONFIG_RESOURCE;
import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.SENSITIVE_PARAMETERS_NAMESPACE;
import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.SENSITIVE_PARAMETERS_RESOURCE;
import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.getConfig;
import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.toParameter;
import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.toPiiFilterConfig;
import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.toValue;
import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.upsertConfig;

import ai.traceable.sensitivedata.config.service.v1.GetInsensitiveParamsRequest;
import ai.traceable.sensitivedata.config.service.v1.GetInsensitiveParamsResponse;
import ai.traceable.sensitivedata.config.service.v1.GetSensitiveParamsRequest;
import ai.traceable.sensitivedata.config.service.v1.GetSensitiveParamsResponse;
import ai.traceable.sensitivedata.config.service.v1.MarkSensitiveDataRequest;
import ai.traceable.sensitivedata.config.service.v1.MarkSensitiveDataResponse;
import ai.traceable.sensitivedata.config.service.v1.ParamType;
import ai.traceable.sensitivedata.config.service.v1.Parameter;
import ai.traceable.sensitivedata.config.service.v1.PiiElement;
import ai.traceable.sensitivedata.config.service.v1.PiiFilterConfig;
import ai.traceable.sensitivedata.config.service.v1.RedactionStrategy;
import ai.traceable.sensitivedata.config.service.v1.SensitiveDataConfigServiceGrpc;
import ai.traceable.sensitivedata.config.service.v1.UnmarkSensitiveDataRequest;
import ai.traceable.sensitivedata.config.service.v1.UnmarkSensitiveDataResponse;
import ai.traceable.sensitivedata.config.service.v1.UpdateRedactionStrategyRequest;
import ai.traceable.sensitivedata.config.service.v1.UpdateRedactionStrategyResponse;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import io.grpc.stub.StreamObserver;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.config.service.v1.GetConfigRequest;
import org.hypertrace.config.service.v1.UpsertConfigRequest;

@Slf4j
public class SensitiveDataConfigServiceImpl
    extends SensitiveDataConfigServiceGrpc.SensitiveDataConfigServiceImplBase {

  private final ConfigServiceBlockingStub configServiceBlockingStub;

  public SensitiveDataConfigServiceImpl(ConfigServiceBlockingStub configServiceBlockingStub) {
    this.configServiceBlockingStub = configServiceBlockingStub;
  }

  @Override
  public void markSensitiveData(
      MarkSensitiveDataRequest request,
      StreamObserver<MarkSensitiveDataResponse> responseObserver) {
    try {
      markData(request.getParametersList(), request.getEndpoint(), true);
      responseObserver.onNext(MarkSensitiveDataResponse.newBuilder().setSuccess(true).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Mark Sensitive Data RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void unmarkSensitiveData(
      UnmarkSensitiveDataRequest request,
      StreamObserver<UnmarkSensitiveDataResponse> responseObserver) {
    try {
      markData(request.getParametersList(), request.getEndpoint(), false);
      responseObserver.onNext(UnmarkSensitiveDataResponse.newBuilder().setSuccess(true).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Unmark Sensitive Data RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getSensitiveParams(
      GetSensitiveParamsRequest request,
      StreamObserver<GetSensitiveParamsResponse> responseObserver) {
    try {
      String context = getContext(request.getParamType(), request.getEndpoint());
      GetSensitiveParamsResponse.Builder responseBuilder = GetSensitiveParamsResponse.newBuilder();
      for (Value value : getSensitiveParameters(context)) {
        responseBuilder.addParameters(toParameter(value));
      }
      responseObserver.onNext(responseBuilder.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Get Sensitive Params RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getInsensitiveParams(
      GetInsensitiveParamsRequest request,
      StreamObserver<GetInsensitiveParamsResponse> responseObserver) {
    try {
      String context = getContext(request.getParamType(), request.getEndpoint());
      GetInsensitiveParamsResponse.Builder responseBuilder =
          GetInsensitiveParamsResponse.newBuilder();
      for (Value value : getInsensitiveParameters(context)) {
        responseBuilder.addParameters(toParameter(value));
      }
      responseObserver.onNext(responseBuilder.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Get Insensitive Params RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateRedactionStrategy(
      UpdateRedactionStrategyRequest request,
      StreamObserver<UpdateRedactionStrategyResponse> responseObserver) {
    try {
      GetConfigRequest getConfigRequest =
        GetConfigRequest.newBuilder()
            .setResourceName(PII_FILTER_CONFIG_RESOURCE)
            .setResourceNamespace(SENSITIVE_PARAMETERS_RESOURCE)
            .build();
      PiiFilterConfig oldPiiFilterConfig =
          toPiiFilterConfig(getConfig(configServiceBlockingStub, getConfigRequest).getConfig());
      String parameterName = request.getParameter().getName();
      RedactionStrategy redactionStrategy = request.getRedactionStrategy();

      PiiFilterConfig.Builder builder = PiiFilterConfig.newBuilder(oldPiiFilterConfig);
      builder.clearKeyRegexs();
      boolean updatedExistingRule = false;
      for (PiiElement piiElement : oldPiiFilterConfig.getKeyRegexsList()) {
        if (piiElement.getRegex().equals(parameterName)) {  // check if rule already exists for the given parameter
          // update existing rule's redaction strategy
          PiiElement updatedPiiElement = PiiElement.newBuilder(piiElement)
              .setRedactionStrategy(redactionStrategy)
              .build();
          builder.addKeyRegexs(updatedPiiElement);
          updatedExistingRule = true;
        } else {
          builder.addKeyRegexs(piiElement);
        }
      }
      
      if (!updatedExistingRule) {
        // add a new rule in the beginning
        builder.addKeyRegexs(0, PiiElement.newBuilder()
//          .setCategory()  // category represents the PII type. Do we need this?
            .setRegex(parameterName)
            .setRedactionStrategy(redactionStrategy)
            .build());
      }

      UpsertConfigRequest upsertConfigRequest =
          UpsertConfigRequest.newBuilder()
              .setResourceName(PII_FILTER_CONFIG_RESOURCE)
              .setResourceNamespace(SENSITIVE_PARAMETERS_NAMESPACE)
              .setConfig(toValue(builder.build()))
              .build();
      upsertConfig(configServiceBlockingStub, upsertConfigRequest);
    } catch (Exception e) {
      log.error("Update Redaction Strategy RPC failed for request:{}", request, e);
      responseObserver.onError(e);    }
  }

  private void markData(List<Parameter> allParameters, String endpoint, boolean isDataSensitive)
      throws InvalidProtocolBufferException {
    Map<ParamType, List<Parameter>> paramTypeToListMap =
        allParameters.stream().collect(Collectors.groupingBy(Parameter::getParamType));
    for (Map.Entry<ParamType, List<Parameter>> entry : paramTypeToListMap.entrySet()) {
      ParamType paramType = entry.getKey();
      List<Parameter> parameters = entry.getValue();
      if (paramType == ParamType.PARAM_TYPE_UNSPECIFIED || parameters.isEmpty()) {
        continue;
      }
      String context = getContext(paramType, endpoint);
      Set<Value> sensitiveParameters = new HashSet<>(getSensitiveParameters(context));
      Set<Value> insensitiveParameters = new HashSet<>(getInsensitiveParameters(context));
      boolean sensitiveParametersUpdated = false;
      boolean insensitiveParametersUpdated = false;
      for (Parameter parameter : parameters) {
        Value parameterValue = toValue(parameter);
        if (isDataSensitive) {
          if (sensitiveParameters.add(parameterValue)) {
            sensitiveParametersUpdated = true;
          }
          if (insensitiveParameters.remove(parameterValue)) {
            insensitiveParametersUpdated = true;
          }
        } else {
          if (sensitiveParameters.remove(parameterValue)) {
            sensitiveParametersUpdated = true;
          }
          if (insensitiveParameters.add(parameterValue)) {
            insensitiveParametersUpdated = true;
          }
        }
      }

      if (sensitiveParametersUpdated) {
        upsertParameters(sensitiveParameters, context, true);
      }
      if (insensitiveParametersUpdated) {
        upsertParameters(insensitiveParameters, context, false);
      }
    }
  }

  private String getContext(ParamType paramType, String endpoint) {
    if (paramType == ParamType.PARAM_TYPE_HEADER) {
      return paramType.name();
    }
    return paramType.name() + endpoint;
  }

  private List<Value> getSensitiveParameters(String context) {
    GetConfigRequest getConfigRequest =
        GetConfigRequest.newBuilder()
            .setResourceName(SENSITIVE_PARAMETERS_RESOURCE)
            .setResourceNamespace(SENSITIVE_PARAMETERS_NAMESPACE)
            .addContexts(context)
            .build();
    Value config = getConfig(configServiceBlockingStub, getConfigRequest).getConfig();
    return config.getListValue().getValuesList();
  }

  private List<Value> getInsensitiveParameters(String context) {
    GetConfigRequest getConfigRequest =
        GetConfigRequest.newBuilder()
            .setResourceName(INSENSITIVE_PARAMETERS_RESOURCE)
            .setResourceNamespace(SENSITIVE_PARAMETERS_RESOURCE)
            .addContexts(context)
            .build();
    Value config = getConfig(configServiceBlockingStub, getConfigRequest).getConfig();
    return config.getListValue().getValuesList();
  }

  private void upsertParameters(
      Iterable<Value> parametersIterable, String context, boolean isSensitive) {
    Value parameters =
        Value.newBuilder()
            .setListValue(ListValue.newBuilder().addAllValues(parametersIterable).build())
            .build();
    String resourceName =
        isSensitive ? SENSITIVE_PARAMETERS_RESOURCE : INSENSITIVE_PARAMETERS_RESOURCE;
    UpsertConfigRequest upsertConfigRequest =
        UpsertConfigRequest.newBuilder()
            .setResourceName(resourceName)
            .setResourceNamespace(SENSITIVE_PARAMETERS_NAMESPACE)
            .setConfig(parameters)
            .setContext(context)
            .build();
    upsertConfig(configServiceBlockingStub, upsertConfigRequest);
  }
}
