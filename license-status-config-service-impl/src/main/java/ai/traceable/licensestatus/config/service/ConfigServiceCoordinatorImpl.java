package ai.traceable.licensestatus.config.service;

import static ai.traceable.licensestatus.config.service.LicenseStatusConstants.LICENSE_STATUS_CONFIG;
import static ai.traceable.licensestatus.config.service.LicenseStatusConstants.LICENSE_STATUS_RESOURCE_NAMESPACE;

import ai.traceable.licensestatus.config.service.v1.LicenseStatus;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import java.util.Optional;
import javax.inject.Inject;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.DefaultObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class ConfigServiceCoordinatorImpl extends DefaultObjectStore<LicenseStatus>
    implements ConfigServiceCoordinator {

  private final LicenseProvider licenseProvider;

  @Inject
  public ConfigServiceCoordinatorImpl(
      ConfigServiceBlockingStub configServiceBlockingStub, LicenseProvider licenseProvider) {
    super(configServiceBlockingStub, LICENSE_STATUS_RESOURCE_NAMESPACE, LICENSE_STATUS_CONFIG);
    this.licenseProvider = licenseProvider;
  }

  @Override
  public LicenseStatus upsertLicenseStatusConfig(
      RequestContext requestContext, LicenseStatus licenseStatus) {
    return this.upsertObject(requestContext, licenseStatus).getData();
  }

  @Override
  public LicenseStatus getLicenseStatusConfig(RequestContext requestContext) {
    return this.getData(requestContext)
        .orElseGet(() -> this.licenseProvider.getLicenseStatus(requestContext));
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(LicenseStatus data) {
    return ConfigProtoConverter.convertToValue(data);
  }

  @Override
  protected Optional<LicenseStatus> buildDataFromValue(Value value) {
    LicenseStatus.Builder builder = LicenseStatus.newBuilder();
    try {
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (InvalidProtocolBufferException e) {
      return Optional.empty();
    }
  }
}
