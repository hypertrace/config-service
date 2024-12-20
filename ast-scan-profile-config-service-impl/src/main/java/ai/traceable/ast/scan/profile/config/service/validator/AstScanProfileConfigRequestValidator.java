package ai.traceable.ast.scan.profile.config.service.validator;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.ast.scan.profile.config.service.AstScanProfileServiceConfig;
import ai.traceable.ast.scan.profile.config.service.store.AstScanProfileConfigStore;
import ai.traceable.ast.scan.profile.config.service.v1.CreateScanProfileRequest;
import ai.traceable.ast.scan.profile.config.service.v1.DeleteScanProfilesRequest;
import ai.traceable.ast.scan.profile.config.service.v1.GetScanProfilesRequest;
import ai.traceable.ast.scan.profile.config.service.v1.ScanProfile;
import ai.traceable.ast.scan.profile.config.service.v1.ScanProfileConfiguration;
import ai.traceable.ast.scan.profile.config.service.v1.UpdateScanProfileRequest;
import ai.traceable.config.utils.RegexValidator;
import com.google.common.base.Preconditions;
import io.grpc.Status;
import jakarta.inject.Inject;
import java.util.Optional;
import java.util.regex.Pattern;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class AstScanProfileConfigRequestValidator {
  // Regex to check the validation of sub-category(plugin) name.
  private static final Pattern SUB_CATEGORY_NAME_PATTERN = Pattern.compile("^[a-zA-Z0-9_]*$");
  // Regex to check the validation of scan profile name.
  private static final Pattern SCAN_PROFILE_NAME_PATTERN = Pattern.compile("^[a-zA-Z0-9_-]*$");
  private final AstScanProfileServiceConfig astScanProfileServiceConfig;
  private final AstScanProfileConfigStore configStore;

  @Inject
  AstScanProfileConfigRequestValidator(
      AstScanProfileServiceConfig astScanProfileServiceConfig,
      AstScanProfileConfigStore configStore) {
    this.astScanProfileServiceConfig = astScanProfileServiceConfig;
    this.configStore = configStore;
  }

  public void validateOrThrow(RequestContext requestContext, CreateScanProfileRequest request) {
    String profileName = request.getName();
    validateRequestContextOrThrow(requestContext);
    Preconditions.checkArgument(request.hasCreateScanProfile(), "Create scan profile not found");
    validateScanProfileName(profileName);
    Preconditions.checkArgument(
        request.getCreateScanProfile().hasProfileConfiguration(),
        "Scan profile configuration not found");
    validateScanProfileConfigurationFields(
        request.getCreateScanProfile().getProfileConfiguration());
  }

  public void validateOrThrow(RequestContext requestContext, UpdateScanProfileRequest request) {
    String profileName = request.getName();
    validateRequestContextOrThrow(requestContext);
    Preconditions.checkArgument(request.hasUpdateScanProfile(), "Update scan profile not found");
    validateScanProfileName(profileName);
    validateScanProfileExistence(requestContext, profileName);
    Preconditions.checkArgument(
        request.getUpdateScanProfile().hasProfileConfiguration(),
        "Scan profile configuration not found");
    validateScanProfileConfigurationFields(
        request.getUpdateScanProfile().getProfileConfiguration());
  }

  public void validateOrThrow(RequestContext requestContext, GetScanProfilesRequest request) {
    validateRequestContextOrThrow(requestContext);
    Preconditions.checkArgument(request.hasFilter(), "Scan profile filter not found");
  }

  public void validateOrThrow(RequestContext requestContext, DeleteScanProfilesRequest request) {
    validateRequestContextOrThrow(requestContext);
    Preconditions.checkArgument(request.hasFilter(), "Scan profile filter not found");
  }

  private void validateScanProfileConfigurationFields(
      ScanProfileConfiguration scanProfileConfiguration) {
    Status regexValidationStatus;
    regexValidationStatus =
        RegexValidator.validateRegex(scanProfileConfiguration.getIncludeUrlRegex());
    if (!regexValidationStatus.isOk()) {
      throw regexValidationStatus.asRuntimeException();
    }
    regexValidationStatus =
        RegexValidator.validateRegex(scanProfileConfiguration.getExcludeUrlRegex());
    if (!regexValidationStatus.isOk()) {
      throw regexValidationStatus.asRuntimeException();
    }
    regexValidationStatus =
        RegexValidator.validateRegex(scanProfileConfiguration.getIncludeFqnRegex());
    if (!regexValidationStatus.isOk()) {
      throw regexValidationStatus.asRuntimeException();
    }
    regexValidationStatus =
        RegexValidator.validateRegex(scanProfileConfiguration.getExcludeUrlRegex());
    if (!regexValidationStatus.isOk()) {
      throw regexValidationStatus.asRuntimeException();
    }
    scanProfileConfiguration.getPluginNamesList().forEach(this::validatePluginSubcategoryName);
  }

  private void validatePluginSubcategoryName(String subcategoryName) {
    if (!isValidSubcategoryName(subcategoryName)) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Provided subcategoryName is not valid: " + subcategoryName)
          .asRuntimeException();
    }
  }

  private boolean isValidSubcategoryName(String s) {
    return SUB_CATEGORY_NAME_PATTERN.matcher(s).find();
  }

  private void validateScanProfileName(String s) {
    Preconditions.checkArgument(!s.isEmpty(), "Scan profile name not found ");
    int maxUserInputLength = astScanProfileServiceConfig.getMaxUserInputLength();
    if (!SCAN_PROFILE_NAME_PATTERN.matcher(s).find()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Provided scan profile Name is not valid: " + s)
          .asRuntimeException();
    }
    if (s.length() > maxUserInputLength) {
      throw Status.INVALID_ARGUMENT
          .withDescription(s + " is greater than max allowed length: " + maxUserInputLength)
          .asRuntimeException();
    }
  }

  private void validateScanProfileExistence(RequestContext requestContext, String name) {
    Optional<ScanProfile> maybeScanProfile = configStore.getData(requestContext, name);
    if (maybeScanProfile.isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("No such profile with given name exists: " + name)
          .asRuntimeException();
    }
  }
}
