package ai.traceable.sensitivedata.config.service;

import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.PARAMETERS_WITH_SENSITIVITY;
import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.PII_FILTER_CONFIG_RESOURCE;
import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.SENSITIVE_DATA_CONFIGURATION;
import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.getConfig;
import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.getContext;
import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.toParameterWithSensitivity;
import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.toPiiFilterConfig;
import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.toValue;
import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.upsertConfig;

import ai.traceable.sensitivedata.config.service.v1.GetParametersRequest;
import ai.traceable.sensitivedata.config.service.v1.GetParametersResponse;
import ai.traceable.sensitivedata.config.service.v1.MarkParametersRequest;
import ai.traceable.sensitivedata.config.service.v1.MarkParametersResponse;
import ai.traceable.sensitivedata.config.service.v1.ParamType;
import ai.traceable.sensitivedata.config.service.v1.Parameter;
import ai.traceable.sensitivedata.config.service.v1.ParameterWithSensitivity;
import ai.traceable.sensitivedata.config.service.v1.PiiElement;
import ai.traceable.sensitivedata.config.service.v1.PiiFilterConfig;
import ai.traceable.sensitivedata.config.service.v1.RedactionStrategy;
import ai.traceable.sensitivedata.config.service.v1.SensitiveDataConfigServiceGrpc;
import ai.traceable.sensitivedata.config.service.v1.UpdateRedactionStrategyRequest;
import ai.traceable.sensitivedata.config.service.v1.UpdateRedactionStrategyResponse;
import com.google.common.base.Preconditions;
import com.google.protobuf.BoolValue;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.protobuf.Value.KindCase;
import io.grpc.stub.StreamObserver;
import java.util.Collections;
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
  public void markParameters(
      MarkParametersRequest request, StreamObserver<MarkParametersResponse> responseObserver) {
    try {
      Map<ParamType, List<Parameter>> paramTypeToListMap =
          request.getParametersList().stream()
              .collect(Collectors.groupingBy(Parameter::getParamType));
      Preconditions.checkNotNull(
          request.getSensitive(),
          "Must specify whether to mark parameters as sensitive or insensitive");
      boolean isSensitive = request.getSensitive().getValue();
      for (Map.Entry<ParamType, List<Parameter>> entry : paramTypeToListMap.entrySet()) {
        ParamType paramType = entry.getKey();
        List<Parameter> parameters = entry.getValue();
        if (paramType == ParamType.PARAM_TYPE_UNSPECIFIED || parameters.isEmpty()) {
          continue;
        }
        String context = getContext(paramType, request.getEndpoint());
        Set<Value> parametersWithSensitivity = new HashSet<>(getParametersWithSensitivity(context));
        for (Parameter parameter : parameters) {
          Value parameterWithSensitivity =
              toValue(
                  ParameterWithSensitivity.newBuilder()
                      .setParameter(parameter)
                      .setSensitive(isSensitive)
                      .build());
          Value parameterWithOppositeSensitivity =
              toValue(
                  ParameterWithSensitivity.newBuilder()
                      .setParameter(parameter)
                      .setSensitive(!isSensitive)
                      .build());
          parametersWithSensitivity.add(parameterWithSensitivity);
          parametersWithSensitivity.remove(parameterWithOppositeSensitivity);
        }
        upsertParametersWithSensitivity(parametersWithSensitivity, context);
      }

      responseObserver.onNext(MarkParametersResponse.newBuilder().setSuccess(true).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Mark Parameters RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getParameters(
      GetParametersRequest request, StreamObserver<GetParametersResponse> responseObserver) {
    try {
      String context = getContext(request.getParamType(), request.getEndpoint());
      GetParametersResponse.Builder responseBuilder = GetParametersResponse.newBuilder();
      for (Value value : getParametersWithSensitivity(context)) {
        ParameterWithSensitivity parameterWithSensitivity = toParameterWithSensitivity(value);
        if (shouldInclude(request.getSensitive(), parameterWithSensitivity)) {
          responseBuilder.addParametersWithSensitivity(parameterWithSensitivity);
        }
      }
      responseObserver.onNext(responseBuilder.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Get Parameters RPC failed for request:{}", request, e);
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
              .setResourceNamespace(SENSITIVE_DATA_CONFIGURATION)
              .build();
      PiiFilterConfig oldPiiFilterConfig =
          toPiiFilterConfig(getConfig(configServiceBlockingStub, getConfigRequest).getConfig());
      String parameterName = request.getParameter().getName();
      RedactionStrategy redactionStrategy = request.getRedactionStrategy();

      PiiFilterConfig.Builder builder = PiiFilterConfig.newBuilder(oldPiiFilterConfig);
      builder.clearKeyRegexs();
      boolean updatedExistingRule = false;
      for (PiiElement piiElement : oldPiiFilterConfig.getKeyRegexsList()) {
        if (piiElement
            .getRegex()
            .equals(parameterName)) { // check if rule already exists for the given parameter
          // update existing rule's redaction strategy
          PiiElement updatedPiiElement =
              PiiElement.newBuilder(piiElement).setRedactionStrategy(redactionStrategy).build();
          builder.addKeyRegexs(updatedPiiElement);
          updatedExistingRule = true;
        } else {
          builder.addKeyRegexs(piiElement);
        }
      }

      if (!updatedExistingRule) {
        // add a new rule in the beginning
        builder.addKeyRegexs(
            0,
            PiiElement.newBuilder()
                .setRegex(parameterName)
                .setRedactionStrategy(redactionStrategy)
                .build());
      }

      UpsertConfigRequest upsertConfigRequest =
          UpsertConfigRequest.newBuilder()
              .setResourceName(PII_FILTER_CONFIG_RESOURCE)
              .setResourceNamespace(SENSITIVE_DATA_CONFIGURATION)
              .setConfig(toValue(builder.build()))
              .build();
      upsertConfig(configServiceBlockingStub, upsertConfigRequest);
      responseObserver.onNext(
          UpdateRedactionStrategyResponse.newBuilder().setSuccess(true).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Update Redaction Strategy RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  private List<Value> getParametersWithSensitivity(String context) {
    GetConfigRequest getConfigRequest =
        GetConfigRequest.newBuilder()
            .setResourceName(PARAMETERS_WITH_SENSITIVITY)
            .setResourceNamespace(SENSITIVE_DATA_CONFIGURATION)
            .addContexts(context)
            .build();
    Value config = getConfig(configServiceBlockingStub, getConfigRequest).getConfig();
    if (config == null || config.getKindCase() != KindCase.LIST_VALUE) {
      return Collections.emptyList();
    }
    return config.getListValue().getValuesList();
  }

  private void upsertParametersWithSensitivity(
      Iterable<Value> parametersWithSensitivityIterable, String context) {
    Value parametersWithSensitivity =
        Value.newBuilder()
            .setListValue(
                ListValue.newBuilder().addAllValues(parametersWithSensitivityIterable).build())
            .build();
    UpsertConfigRequest upsertConfigRequest =
        UpsertConfigRequest.newBuilder()
            .setResourceName(PARAMETERS_WITH_SENSITIVITY)
            .setResourceNamespace(SENSITIVE_DATA_CONFIGURATION)
            .setConfig(parametersWithSensitivity)
            .setContext(context)
            .build();
    upsertConfig(configServiceBlockingStub, upsertConfigRequest);
  }

  private boolean shouldInclude(
      BoolValue sensitive, ParameterWithSensitivity parameterWithSensitivity) {
    return sensitive == null || sensitive.getValue() == parameterWithSensitivity.getSensitive();
  }
}
