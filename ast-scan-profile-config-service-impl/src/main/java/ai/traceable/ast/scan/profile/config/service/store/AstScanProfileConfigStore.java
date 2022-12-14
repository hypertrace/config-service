package ai.traceable.ast.scan.profile.config.service.store;

import ai.traceable.ast.scan.profile.config.service.v1.ScanProfile;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import java.util.Optional;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;

public class AstScanProfileConfigStore extends IdentifiedObjectStore<ScanProfile> {
  private static final String AST_SCAN_PROFILE_CONFIG_RESOURCE_NAME = "ast-scan-profile";
  private static final String AST_SCAN_PROFILE_CONFIG_RESOURCE_NAMESPACE =
      "ast-scan-profile-config";

  @Inject
  public AstScanProfileConfigStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub) {
    super(
        configServiceBlockingStub,
        AST_SCAN_PROFILE_CONFIG_RESOURCE_NAMESPACE,
        AST_SCAN_PROFILE_CONFIG_RESOURCE_NAME);
  }

  @SneakyThrows
  @Override
  protected Optional<ScanProfile> buildDataFromValue(Value value) {
    ScanProfile.Builder configBuilder = ScanProfile.newBuilder();
    ConfigProtoConverter.mergeFromValue(value, configBuilder);
    return Optional.of(configBuilder.build());
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(ScanProfile scanProfile) {
    return ConfigProtoConverter.convertToValue(scanProfile);
  }

  @Override
  protected String getContextFromData(ScanProfile scanProfile) {
    return scanProfile.getName();
  }
}
