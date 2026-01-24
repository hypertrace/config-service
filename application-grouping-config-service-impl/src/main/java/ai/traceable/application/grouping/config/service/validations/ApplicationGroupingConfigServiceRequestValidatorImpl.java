package ai.traceable.application.grouping.config.service.validations;

import static java.lang.Character.isLetterOrDigit;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;

import ai.traceable.application.grouping.config.service.manager.ApplicationGroupingRuleConfigService;
import ai.traceable.application.grouping.config.service.v1.ApplicationGroupingRuleConfigInfo;
import ai.traceable.application.grouping.config.service.v1.AssetSelector;
import ai.traceable.application.grouping.config.service.v1.CreateApplicationGroupingRuleConfigRequest;
import ai.traceable.application.grouping.config.service.v1.DeleteApplicationGroupingRuleConfigsRequest;
import ai.traceable.application.grouping.config.service.v1.GetAllApplicationGroupingRuleConfigsRequest;
import ai.traceable.application.grouping.config.service.v1.UpdateApplicationGroupingRuleConfigRequest;
import com.google.inject.Inject;
import com.google.re2j.Pattern;
import io.grpc.Status;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.validation.GrpcValidatorUtils;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class ApplicationGroupingConfigServiceRequestValidatorImpl
    implements ApplicationGroupingConfigServiceRequestValidator {

  private static final int MAX_ID_LENGTH = 255;
  private static final int MAX_RULE_NAME_LENGTH = 255;
  private static final int MAX_GROUP_NAME_LENGTH = 255;
  private static final Pattern VALID_STRING_PATTERN = Pattern.compile("^[a-zA-Z0-9 _-]+$");

  private final ApplicationGroupingRuleConfigService applicationGroupingRuleConfigService;

  @Inject
  public ApplicationGroupingConfigServiceRequestValidatorImpl(
      ApplicationGroupingRuleConfigService applicationGroupingRuleConfigService) {
    this.applicationGroupingRuleConfigService = applicationGroupingRuleConfigService;
  }

  @Override
  public void validateOrThrow(
      RequestContext requestContext, GetAllApplicationGroupingRuleConfigsRequest request) {
    validateOrThrow(requestContext);
  }

  @Override
  public void validateOrThrow(
      RequestContext requestContext, DeleteApplicationGroupingRuleConfigsRequest request) {
    validateOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(
        request, DeleteApplicationGroupingRuleConfigsRequest.IDS_FIELD_NUMBER);
    request
        .getIdsList()
        .forEach(id -> validateStringField(requestContext, id, "ID", MAX_ID_LENGTH));
  }

  @Override
  public void validateOrThrow(
      RequestContext requestContext, UpdateApplicationGroupingRuleConfigRequest request) {
    validateOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(
        request, UpdateApplicationGroupingRuleConfigRequest.ID_FIELD_NUMBER);
    validateUpdateApplicationGroupingRuleConfigRequest(requestContext, request);

    if (!applicationGroupingRuleConfigService.doesApplicationGroupingRuleConfigExist(
        requestContext, request.getId())) {
      throw Status.NOT_FOUND
          .withDescription(
              String.format(
                  "Unable to find application grouping rule config in context %s for request %s",
                  requestContext, request))
          .asRuntimeException(requestContext.buildTrailers());
    }
  }

  @Override
  public void validateOrThrow(
      RequestContext requestContext, CreateApplicationGroupingRuleConfigRequest request) {
    validateOrThrow(requestContext);
    validateApplicationGroupingRuleConfigInfo(
        requestContext, request.getApplicationGroupingRuleConfigInfo());
  }

  private void validateUpdateApplicationGroupingRuleConfigRequest(
      RequestContext requestContext, UpdateApplicationGroupingRuleConfigRequest request) {
    validateStringField(requestContext, request.getId(), "ID", MAX_ID_LENGTH);
    validateApplicationGroupingRuleConfigInfo(
        requestContext, request.getApplicationGroupingRuleConfigInfo());
  }

  private void validateApplicationGroupingRuleConfigInfo(
      RequestContext context, ApplicationGroupingRuleConfigInfo configInfo) {
    validateNonDefaultPresenceOrThrow(
        configInfo, ApplicationGroupingRuleConfigInfo.RULE_NAME_FIELD_NUMBER);
    validateStringField(context, configInfo.getRuleName(), "Rule name", MAX_RULE_NAME_LENGTH);
    validateRuleNameStartsAndEndsWithAlphanumeric(context, configInfo.getRuleName());

    validateStringField(
        context, configInfo.getGroupName().getStatic(), "Group name", MAX_GROUP_NAME_LENGTH);

    validateNonDefaultPresenceOrThrow(
        configInfo, ApplicationGroupingRuleConfigInfo.SELECTOR_FIELD_NUMBER);
    if (configInfo.getSelectorList().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("At least one asset selector is required")
          .asRuntimeException(context.buildTrailers());
    }

    for (AssetSelector selector : configInfo.getSelectorList()) {
      validateAssetSelector(context, selector);
    }
  }

  private void validateAssetSelector(RequestContext context, AssetSelector selector) {
    if (!selector.getAssetType().hasWellKnownAssetType()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Asset type must have a well-known asset type")
          .asRuntimeException(context.buildTrailers());
    }

    if (!selector.getFilter().hasSavedFilter()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Filter must have a saved filter reference")
          .asRuntimeException(context.buildTrailers());
    }
    validateStringField(
        context, selector.getFilter().getSavedFilter().getId(), "Filter ID", MAX_ID_LENGTH);
  }

  private void validateStringField(
      RequestContext requestContext, String stringField, String fieldName, int maxLength) {
    if (stringField == null || stringField.isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(String.format("%s must not be blank", fieldName))
          .asRuntimeException(requestContext.buildTrailers());
    }

    if (stringField.length() > maxLength) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "%s must not exceed %d characters (provided: %d)",
                  fieldName, maxLength, stringField.length()))
          .asRuntimeException(requestContext.buildTrailers());
    }

    if (!VALID_STRING_PATTERN.matcher(stringField).matches()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "%s contains invalid characters. Only alphanumeric characters, spaces, hyphens, and underscores are allowed",
                  fieldName))
          .asRuntimeException(requestContext.buildTrailers());
    }
  }

  private void validateRuleNameStartsAndEndsWithAlphanumeric(
      RequestContext requestContext, String ruleName) {
    if (!isLetterOrDigit(ruleName.charAt(0))
        || !isLetterOrDigit(ruleName.charAt(ruleName.length() - 1))) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Rule name must start and end with an alphanumeric character")
          .asRuntimeException(requestContext.buildTrailers());
    }
  }

  private void validateOrThrow(RequestContext requestContext) {
    GrpcValidatorUtils.validateRequestContextOrThrow(requestContext);
  }
}
