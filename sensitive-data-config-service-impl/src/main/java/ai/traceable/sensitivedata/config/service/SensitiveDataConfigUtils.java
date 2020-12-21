package ai.traceable.sensitivedata.config.service;

import ai.traceable.sensitivedata.config.service.v1.ParamType;
import ai.traceable.sensitivedata.config.service.v1.ParameterWithSensitivity;
import ai.traceable.sensitivedata.config.service.v1.PiiFilterConfig;
import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.StringValue;
import com.google.protobuf.Value;
import com.google.protobuf.Value.KindCase;
import com.google.protobuf.util.JsonFormat;
import java.util.Optional;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.config.service.v1.GetConfigRequest;
import org.hypertrace.config.service.v1.GetConfigResponse;
import org.hypertrace.config.service.v1.UpsertConfigRequest;
import org.hypertrace.config.service.v1.UpsertConfigResponse;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class SensitiveDataConfigUtils {

  public static final String PARAMETERS_WITH_SENSITIVITY = "parameters-with-sensitivity";
  public static final String PII_FILTER_CONFIG_RESOURCE = "pii-filter-config";
  public static final String SENSITIVE_DATA_CONFIGURATION = "sensitive-data-configuration";

  private SensitiveDataConfigUtils() {
    // to prevent instantiation
  }

  public static Value toValue(Message message) throws InvalidProtocolBufferException {
    String jsonString = JsonFormat.printer().print(message);
    Value.Builder valueBuilder = Value.newBuilder();
    JsonFormat.parser().merge(jsonString, valueBuilder);
    return valueBuilder.build();
  }

  public static PiiFilterConfig toPiiFilterConfig(Value value)
      throws InvalidProtocolBufferException {
    PiiFilterConfig.Builder builder = PiiFilterConfig.newBuilder();
    if (value != null && value.getKindCase() != KindCase.NULL_VALUE) {
      String jsonString = JsonFormat.printer().print(value);
      JsonFormat.parser().merge(jsonString, builder);
    }
    return builder.build();
  }

  public static ParameterWithSensitivity toParameterWithSensitivity(Value value)
      throws InvalidProtocolBufferException {
    ParameterWithSensitivity.Builder builder = ParameterWithSensitivity.newBuilder();
    if (value != null && value.getKindCase() != KindCase.NULL_VALUE) {
      String jsonString = JsonFormat.printer().print(value);
      JsonFormat.parser().merge(jsonString, builder);
    }
    return builder.build();
  }

  public static String getContext(ParamType paramType, StringValue endpoint) {
    if (paramType == ParamType.PARAM_TYPE_HEADER) {
      return paramType.name();
    }
    Preconditions.checkNotNull(endpoint, "Endpoint can't be null for non-header parameter");
    return paramType.name() + "_" + endpoint.getValue();
  }

  public static GetConfigResponse getConfig(
      ConfigServiceBlockingStub configServiceBlockingStub, GetConfigRequest request) {
    return GrpcClientRequestContextUtil.executeInTenantContext(
        getTenantId(), () -> configServiceBlockingStub.getConfig(request));
  }

  public static UpsertConfigResponse upsertConfig(
      ConfigServiceBlockingStub configServiceBlockingStub, UpsertConfigRequest request) {
    return GrpcClientRequestContextUtil.executeInTenantContext(
        getTenantId(), () -> configServiceBlockingStub.upsertConfig(request));
  }

  public static String getTenantId() {
    Optional<String> tenantId = RequestContext.CURRENT.get().getTenantId();
    if (tenantId.isEmpty()) {
      throw new IllegalArgumentException("Tenant Id is missing in the request.");
    }
    return tenantId.get();
  }
}
