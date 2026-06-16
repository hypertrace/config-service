package ai.traceable.application.grouping.config.service.validations;

import static java.lang.Character.isLetterOrDigit;
import static java.util.Collections.emptyList;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;

import ai.traceable.application.grouping.config.service.manager.ApplicationGroupingRuleConfigService;
import ai.traceable.application.grouping.config.service.store.ApplicationGroupingRuleConfigStore;
import ai.traceable.application.grouping.config.service.v1.ApplicationGroupingRuleConfigInfo;
import ai.traceable.application.grouping.config.service.v1.AssetSelector;
import ai.traceable.application.grouping.config.service.v1.CreateApplicationGroupingRuleConfigRequest;
import ai.traceable.application.grouping.config.service.v1.DeleteApplicationGroupingRuleConfigsRequest;
import ai.traceable.application.grouping.config.service.v1.GetAllApplicationGroupingRuleConfigsRequest;
import ai.traceable.application.grouping.config.service.v1.UpdateApplicationGroupingRuleConfigRequest;
import com.google.inject.Inject;
import com.google.re2j.Pattern;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.grpc.Status;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.validation.GrpcValidatorUtils;
import org.hypertrace.core.grpcutils.context.ContextualStatusExceptionBuilder;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class ApplicationGroupingConfigServiceRequestValidatorImpl
    implements ApplicationGroupingConfigServiceRequestValidator {

  private static final int MAX_ID_LENGTH = 255;
  private static final int MAX_RULE_NAME_LENGTH = 255;
  private static final Pattern VALID_STRING_PATTERN = Pattern.compile("^[a-zA-Z0-9 _-]+$");
  private static final String APPLICATION_GROUPING_CONFIG_SERVICE_PATH =
      "application.grouping.config.service";
  private static final String DYNAMIC_API_REGEX_DENY_LIST_PATH = "dynamicApiRegexDenyList";
  private static final String RULE_NAME_FIELD = "Rule name";

  private final ApplicationGroupingRuleConfigService applicationGroupingRuleConfigService;
  private final ApplicationGroupingRuleConfigStore applicationGroupingRuleConfigStore;
  private final Config config;

  @Inject
  public ApplicationGroupingConfigServiceRequestValidatorImpl(
      final ApplicationGroupingRuleConfigService applicationGroupingRuleConfigService,
      final ApplicationGroupingRuleConfigStore applicationGroupingRuleConfigStore,
      final Config config) {
    this.applicationGroupingRuleConfigService = applicationGroupingRuleConfigService;
    this.applicationGroupingRuleConfigStore = applicationGroupingRuleConfigStore;
    this.config =
        config.hasPath(APPLICATION_GROUPING_CONFIG_SERVICE_PATH)
            ? config.getConfig(APPLICATION_GROUPING_CONFIG_SERVICE_PATH)
            : ConfigFactory.empty();
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
      throw ContextualStatusExceptionBuilder.from(
              Status.NOT_FOUND
                  .withDescription(
                      String.format(
                          "Unable to find application grouping rule config in context %s for request %s",
                          requestContext, request))
                  .asRuntimeException(requestContext.buildTrailers()))
          .withExternalMessage(
              "The application grouping rule could not be found. It may have been deleted or is no longer available.")
          .buildRuntimeException();
    }
  }

  @Override
  public void validateOrThrow(
      RequestContext requestContext, CreateApplicationGroupingRuleConfigRequest request) {
    validateOrThrow(requestContext);
    validateApplicationGroupingRuleConfigInfo(
        requestContext, request.getApplicationGroupingRuleConfigInfo());
    validateUniqueRuleName(
        requestContext, request.getApplicationGroupingRuleConfigInfo().getRuleName(), null);
  }

  private void validateUpdateApplicationGroupingRuleConfigRequest(
      RequestContext requestContext, UpdateApplicationGroupingRuleConfigRequest request) {
    validateStringField(requestContext, request.getId(), "ID", MAX_ID_LENGTH);
    validateApplicationGroupingRuleConfigInfo(
        requestContext, request.getApplicationGroupingRuleConfigInfo());
    validateIsNotDynamic(requestContext, request.getApplicationGroupingRuleConfigInfo());
    validateUniqueRuleName(
        requestContext,
        request.getApplicationGroupingRuleConfigInfo().getRuleName(),
        request.getId());
  }

  private void validateIsNotDynamic(
      RequestContext requestContext, ApplicationGroupingRuleConfigInfo configInfo) {
    if (configInfo.getGroupName().hasDynamic()) {
      throw ContextualStatusExceptionBuilder.from(
              Status.INVALID_ARGUMENT
                  .withDescription("Dynamic rules cannot be updated")
                  .asRuntimeException(requestContext.buildTrailers()))
          .useStatusDescriptionAsExternalMessage()
          .buildRuntimeException();
    }
  }

  private void validateApplicationGroupingRuleConfigInfo(
      RequestContext context, ApplicationGroupingRuleConfigInfo configInfo) {
    validateNonDefaultPresenceOrThrow(
        configInfo, ApplicationGroupingRuleConfigInfo.RULE_NAME_FIELD_NUMBER);
    validateStringFieldWithExternalMessage(
        context, configInfo.getRuleName(), RULE_NAME_FIELD, MAX_RULE_NAME_LENGTH);
    validateRuleNameStartsAndEndsWithAlphanumeric(context, configInfo.getRuleName());

    if (!configInfo.getGroupName().hasDynamic()) {
      validateNonDefaultPresenceOrThrow(
          configInfo, ApplicationGroupingRuleConfigInfo.SELECTOR_FIELD_NUMBER);
      if (configInfo.getSelectorList().isEmpty()) {
        throw ContextualStatusExceptionBuilder.from(
                Status.INVALID_ARGUMENT
                    .withDescription("At least one asset selector is required")
                    .asRuntimeException(context.buildTrailers()))
            .useStatusDescriptionAsExternalMessage()
            .buildRuntimeException();
      }
      validateDynamicApiRegex(configInfo.getGroupName().getDynamic().getApiRegex(), context);
    }

    for (AssetSelector selector : configInfo.getSelectorList()) {
      validateAssetSelector(context, selector);
    }
  }

  private void validateAssetSelector(RequestContext context, AssetSelector selector) {
    if (!selector.getAssetType().hasWellKnownAssetType()) {
      throw ContextualStatusExceptionBuilder.from(
              Status.INVALID_ARGUMENT
                  .withDescription("Asset type must have a well-known asset type")
                  .asRuntimeException(context.buildTrailers()))
          .useStatusDescriptionAsExternalMessage()
          .buildRuntimeException();
    }

    if (!selector.getFilter().hasSavedFilter()) {
      throw ContextualStatusExceptionBuilder.from(
              Status.INVALID_ARGUMENT
                  .withDescription("Filter must have a saved filter reference")
                  .asRuntimeException(context.buildTrailers()))
          .withExternalMessage("Filter in the asset selector does not contain a valid filter")
          .buildRuntimeException();
    }
    validateStringField(
        context, selector.getFilter().getSavedFilter().getId(), "Filter ID", MAX_ID_LENGTH);
  }

  private void validateStringField(
      RequestContext requestContext, String stringField, String fieldName, int maxLength) {
    validateStringField(requestContext, stringField, fieldName, maxLength, false);
  }

  private void validateStringFieldWithExternalMessage(
      RequestContext requestContext, String stringField, String fieldName, int maxLength) {
    validateStringField(requestContext, stringField, fieldName, maxLength, true);
  }

  private void validateStringField(
      RequestContext requestContext,
      String stringField,
      String fieldName,
      int maxLength,
      boolean exposeAsExternalMessage) {
    if (stringField == null || stringField.isBlank()) {
      throw buildValidationException(
          requestContext,
          String.format("%s must not be blank.", fieldName),
          exposeAsExternalMessage);
    }

    if (stringField.length() > maxLength) {
      throw buildValidationException(
          requestContext,
          String.format(
              "%s must not exceed %d characters (provided: %d)",
              fieldName, maxLength, stringField.length()),
          String.format("%s cannot exceed %d characters.", fieldName, maxLength),
          exposeAsExternalMessage);
    }

    if (!VALID_STRING_PATTERN.matcher(stringField).matches()) {
      throw buildValidationException(
          requestContext,
          String.format(
              "%s contains invalid characters. Use only letters, numbers, spaces, hyphens (-), and underscores (_).",
              fieldName),
          exposeAsExternalMessage);
    }
  }

  private RuntimeException buildValidationException(
      RequestContext requestContext, String message, boolean exposeAsExternalMessage) {
    return buildValidationException(requestContext, message, message, exposeAsExternalMessage);
  }

  private RuntimeException buildValidationException(
      RequestContext requestContext,
      String internalMessage,
      String externalMessage,
      boolean exposeAsExternalMessage) {
    final ContextualStatusExceptionBuilder exceptionBuilder =
        ContextualStatusExceptionBuilder.from(
            Status.INVALID_ARGUMENT
                .withDescription(internalMessage)
                .asRuntimeException(requestContext.buildTrailers()));
    return exposeAsExternalMessage
        ? exceptionBuilder.withExternalMessage(externalMessage).buildRuntimeException()
        : exceptionBuilder.buildRuntimeException();
  }

  private void validateRuleNameStartsAndEndsWithAlphanumeric(
      RequestContext requestContext, String ruleName) {
    if (!isLetterOrDigit(ruleName.charAt(0))
        || !isLetterOrDigit(ruleName.charAt(ruleName.length() - 1))) {
      throw ContextualStatusExceptionBuilder.from(
              Status.INVALID_ARGUMENT
                  .withDescription("Rule name must start and end with a letter or number.")
                  .asRuntimeException(requestContext.buildTrailers()))
          .useStatusDescriptionAsExternalMessage()
          .buildRuntimeException();
    }
  }

  private void validateOrThrow(RequestContext requestContext) {
    GrpcValidatorUtils.validateRequestContextOrThrow(requestContext);
  }

  private void validateUniqueRuleName(
      RequestContext requestContext, String ruleName, String excludeId) {
    applicationGroupingRuleConfigStore
        .findByRuleName(requestContext, ruleName)
        .ifPresent(
            existingConfig -> {
              if (excludeId == null || !existingConfig.getId().equals(excludeId)) {
                throw ContextualStatusExceptionBuilder.from(
                        Status.ALREADY_EXISTS
                            .withDescription(
                                String.format(
                                    "An application rule with the name \"%s\" already exists. Please choose a different name.",
                                    ruleName))
                            .asRuntimeException(requestContext.buildTrailers()))
                    .useStatusDescriptionAsExternalMessage()
                    .buildRuntimeException();
              }
            });
  }

  private void validateDynamicApiRegex(final String apiRegex, final RequestContext requestContext) {
    List<String> denyList =
        config.hasPath(DYNAMIC_API_REGEX_DENY_LIST_PATH)
            ? config.getStringList(DYNAMIC_API_REGEX_DENY_LIST_PATH)
            : emptyList();

    if (denyList.contains(apiRegex)) {
      throw ContextualStatusExceptionBuilder.from(
              Status.INVALID_ARGUMENT
                  .withDescription(
                      String.format(
                          "The API pattern \"%s\" is not supported. Please use a different pattern.",
                          apiRegex))
                  .asRuntimeException(requestContext.buildTrailers()))
          .useStatusDescriptionAsExternalMessage()
          .buildRuntimeException();
    }
  }
}
