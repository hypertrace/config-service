package ai.traceable.sensitivedata.config.service;

import ai.traceable.sensitivedata.config.service.v1.Parameter;
import ai.traceable.sensitivedata.config.service.v1.PiiFilterConfig;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Value;
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

  public static final String SENSITIVE_PARAMETERS_RESOURCE = "sensitive-parameters";
  public static final String INSENSITIVE_PARAMETERS_RESOURCE = "insensitive-parameters";
  public static final String PII_FILTER_CONFIG_RESOURCE = "pii-filter-config";
  public static final String SENSITIVE_PARAMETERS_NAMESPACE = "sensitive-data-configuration";

  private SensitiveDataConfigUtils() {
    // to prevent instantiation
  }

  public static Value toValue(Message message) throws InvalidProtocolBufferException {
    String jsonString = JsonFormat.printer().print(message);
    Value.Builder valueBuilder = Value.newBuilder();
    JsonFormat.parser().merge(jsonString, valueBuilder);
    return valueBuilder.build();
  }

  public static Parameter toParameter(Value value) throws InvalidProtocolBufferException {
    String jsonString = JsonFormat.printer().print(value);
    Parameter.Builder builder = Parameter.newBuilder();
    JsonFormat.parser().merge(jsonString, builder);
    return builder.build();
  }

  public static PiiFilterConfig toPiiFilterConfig(Value value)
      throws InvalidProtocolBufferException {
    String jsonString = JsonFormat.printer().print(value);
    PiiFilterConfig.Builder builder = PiiFilterConfig.newBuilder();
    JsonFormat.parser().merge(jsonString, builder);
    return builder.build();
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
