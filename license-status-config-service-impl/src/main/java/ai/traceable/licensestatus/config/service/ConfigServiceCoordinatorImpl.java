package ai.traceable.licensestatus.config.service;

import static ai.traceable.licensestatus.config.service.LicenseStatusConstants.LICENSE_STATUS_CONFIG;
import static ai.traceable.licensestatus.config.service.LicenseStatusConstants.LICENSE_STATUS_RESOURCE_NAMESPACE;

import ai.traceable.licensestatus.config.service.v1.LicenseLimit;
import ai.traceable.licensestatus.config.service.v1.LicenseStatus;
import com.google.protobuf.Value;
import com.typesafe.config.Config;
import io.grpc.Channel;
import io.grpc.Status;
import lombok.SneakyThrows;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.config.service.v1.GetConfigRequest;
import org.hypertrace.config.service.v1.GetConfigResponse;
import org.hypertrace.config.service.v1.UpsertConfigRequest;
import org.hypertrace.config.service.v1.UpsertConfigResponse;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class ConfigServiceCoordinatorImpl implements ConfigServiceCoordinator {

  static final String LICENSE_STATUS_CONFIG_SERVICE = "license.status.config.service";
  static final String DEFAULT_LICENSE_LIMIT = "default.license.limit";

  private final ConfigServiceBlockingStub configServiceBlockingStub;
  private final LicenseStatus defaultLicenseStatus;

  public ConfigServiceCoordinatorImpl(Channel configChannel, Config config) {
    this.configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(configChannel)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
    this.defaultLicenseStatus =
        LicenseStatus.newBuilder()
            .setTracesLicenseLimit(
                LicenseLimit.valueOf(
                    config
                        .getConfig(LICENSE_STATUS_CONFIG_SERVICE)
                        .getString(DEFAULT_LICENSE_LIMIT)))
            .build();
  }

  @Override
  public LicenseStatus upsertLicenseStatusConfig(
      RequestContext requestContext, LicenseStatus licenseStatus) {
    UpsertConfigRequest upsertConfigRequest =
        UpsertConfigRequest.newBuilder()
            .setResourceName(LICENSE_STATUS_CONFIG)
            .setResourceNamespace(LICENSE_STATUS_RESOURCE_NAMESPACE)
            .setConfig(convertToGenericFromLicenseStatus(licenseStatus))
            .build();
    UpsertConfigResponse upsertConfigResponse = upsertConfig(requestContext, upsertConfigRequest);
    return convertToLicenseStatusFromGeneric(upsertConfigResponse.getConfig());
  }

  @Override
  public LicenseStatus getLicenseStatusConfig(RequestContext requestContext) {
    GetConfigRequest getConfigRequest =
        GetConfigRequest.newBuilder()
            .setResourceName(LICENSE_STATUS_CONFIG)
            .setResourceNamespace(LICENSE_STATUS_RESOURCE_NAMESPACE)
            .build();
    return getLicenseStatus(requestContext, getConfigRequest);
  }

  @SneakyThrows
  private LicenseStatus getLicenseStatus(
      RequestContext requestContext, GetConfigRequest getConfigRequest) {
    try {
      GetConfigResponse getConfigResponse = getConfig(requestContext, getConfigRequest);
      return convertToLicenseStatusFromGeneric(getConfigResponse.getConfig());
    } catch (Exception e) {
      if (Status.fromThrowable(e).equals(Status.NOT_FOUND)) {
        return defaultLicenseStatus;
      }
      throw e;
    }
  }

  private UpsertConfigResponse upsertConfig(RequestContext context, UpsertConfigRequest request) {
    return context.call(() -> configServiceBlockingStub.upsertConfig(request));
  }

  private GetConfigResponse getConfig(RequestContext context, GetConfigRequest request) {
    return context.call(() -> configServiceBlockingStub.getConfig(request));
  }

  @SneakyThrows
  private Value convertToGenericFromLicenseStatus(LicenseStatus licenseStatus) {
    return ConfigProtoConverter.convertToValue(licenseStatus);
  }

  @SneakyThrows
  private LicenseStatus convertToLicenseStatusFromGeneric(Value value) {
    LicenseStatus.Builder builder = LicenseStatus.newBuilder();
    ConfigProtoConverter.mergeFromValue(value, builder);
    return builder.build();
  }
}
