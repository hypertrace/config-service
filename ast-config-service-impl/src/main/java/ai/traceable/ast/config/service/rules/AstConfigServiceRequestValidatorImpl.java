package ai.traceable.ast.config.service.rules;

import ai.traceable.ast.config.service.v1.AstFeatureConfigFilter;
import ai.traceable.ast.config.service.v1.CodeSnippetDetails;
import ai.traceable.ast.config.service.v1.CodeSnippetType;
import ai.traceable.ast.config.service.v1.CreateCustomTestPlugin;
import ai.traceable.ast.config.service.v1.CreateCustomTestPluginRequest;
import ai.traceable.ast.config.service.v1.CustomerDefinedTagsMap;
import ai.traceable.ast.config.service.v1.DeleteCustomTestPluginRequest;
import ai.traceable.ast.config.service.v1.DeleteVulnerabilityMetadataOverridesConfigRequest;
import ai.traceable.ast.config.service.v1.EditVulnerabilityMetadataOverridesRequest;
import ai.traceable.ast.config.service.v1.GetAllCustomTestPluginsRequest;
import ai.traceable.ast.config.service.v1.GetAllVulnerabilityMetadataOverridesRequest;
import ai.traceable.ast.config.service.v1.GetAstFeatureConfigsRequest;
import ai.traceable.ast.config.service.v1.GetScanPurgeConfigRequest;
import ai.traceable.ast.config.service.v1.GetVulnerabilityMetadataOverridesRequest;
import ai.traceable.ast.config.service.v1.HttpMethod;
import ai.traceable.ast.config.service.v1.HttpRestApiDetails;
import ai.traceable.ast.config.service.v1.IdentifyingAttributes;
import ai.traceable.ast.config.service.v1.Location;
import ai.traceable.ast.config.service.v1.RelationalOperator;
import ai.traceable.ast.config.service.v1.SampleData;
import ai.traceable.ast.config.service.v1.SpanFilters;
import ai.traceable.ast.config.service.v1.StringPredicate;
import ai.traceable.ast.config.service.v1.TagValue;
import ai.traceable.ast.config.service.v1.TestPluginSafetyType;
import ai.traceable.ast.config.service.v1.TestPluginType;
import ai.traceable.ast.config.service.v1.UpdateAstFeatureConfigRequest;
import ai.traceable.ast.config.service.v1.UpdateCustomTestPlugin;
import ai.traceable.ast.config.service.v1.UpdateCustomTestPluginRequest;
import ai.traceable.ast.config.service.v1.UpdateScanPurgeConfigRequest;
import ai.traceable.ast.config.service.v1.VulnerabilityMetadataOverrides;
import ai.traceable.ast.config.service.v1.VulnerabilitySeverity;
import com.google.common.base.Preconditions;
import com.google.protobuf.Duration;
import io.grpc.Status;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.validation.GrpcValidatorUtils;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class AstConfigServiceRequestValidatorImpl implements AstConfigServiceRequestValidator {

  @Override
  public void validateOrThrow(RequestContext requestContext, UpdateScanPurgeConfigRequest request) {
    validateOrThrow(requestContext);
    if (!request.hasPurgeConfig() || !request.getPurgeConfig().hasPurgeDuration()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Request should have a valid purge config with a valid duration")
          .asRuntimeException(requestContext.buildTrailers());
    }
  }

  @Override
  public void validateOrThrow(RequestContext requestContext, GetScanPurgeConfigRequest request) {
    validateOrThrow(requestContext);
  }

  @Override
  public void validateOrThrow(
      RequestContext requestContext, GetAllCustomTestPluginsRequest request) {
    validateOrThrow(requestContext);
  }

  @Override
  public void validateOrThrow(
      RequestContext requestContext, EditVulnerabilityMetadataOverridesRequest request) {
    validateOrThrow(requestContext);
    final VulnerabilityMetadataOverrides vulnerabilityMetadataOverrides =
        request.getVulnerabilityMetadataOverrides();
    IdentifyingAttributes identifyingAttributes =
        vulnerabilityMetadataOverrides.getIdentifyingAttributes();
    if (!vulnerabilityMetadataOverrides.hasIdentifyingAttributes()
        || identifyingAttributes.getMetadataId().isEmpty()
        || identifyingAttributes.getCategory().isEmpty()
        || identifyingAttributes.getSubcategory().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Request should have identifying attributes to update the Plugin")
          .asRuntimeException(requestContext.buildTrailers());
    }

    if (vulnerabilityMetadataOverrides.hasSeverity()) {
      validateSeverity(requestContext, vulnerabilityMetadataOverrides.getSeverity());
    }
    if (vulnerabilityMetadataOverrides.hasCvssVectorString()) {
      validateCvssVectorString(
          requestContext, vulnerabilityMetadataOverrides.getCvssVectorString());
    }
    if (vulnerabilityMetadataOverrides.hasEstimatedFixTime()) {
      validateEstimatedFixTime(
          requestContext, vulnerabilityMetadataOverrides.getEstimatedFixTime());
    }
    if (vulnerabilityMetadataOverrides.hasCustomerDefinedTags()) {
      validateCustomerDefinedTags(
          requestContext, vulnerabilityMetadataOverrides.getCustomerDefinedTags());
    }
  }

  private void validateCustomerDefinedTags(
      final RequestContext requestContext, final CustomerDefinedTagsMap customerDefinedTags) {
    if (CustomerDefinedTagsMap.getDefaultInstance().equals(customerDefinedTags)) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Customer defined tags provided: %s cannot be empty", customerDefinedTags))
          .asRuntimeException(requestContext.buildTrailers());
    }
    customerDefinedTags
        .getCustomerDefinedTagsMap()
        .entrySet()
        .forEach(entry -> validateTagValue(requestContext, entry.getKey(), entry.getValue()));
  }

  private void validateTagValue(
      final RequestContext requestContext, final String key, final TagValue value) {
    if (value.getValueCount() == 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Tag values set cannot be empty. Empty values were set for the key: %s", key))
          .asRuntimeException(requestContext.buildTrailers());
    }
  }

  private void validateEstimatedFixTime(
      final RequestContext requestContext, final Duration estimatedFixTime) {
    if (Duration.getDefaultInstance().equals(estimatedFixTime)) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format("Invalid Estimated fix time provided: %s", estimatedFixTime))
          .asRuntimeException(requestContext.buildTrailers());
    }
  }

  private void validateCvssVectorString(
      final RequestContext requestContext, final String cvssVectorString) {
    if (cvssVectorString.isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Cvss vector string provided cannot be blank")
          .asRuntimeException(requestContext.buildTrailers());
    }
  }

  private void validateSeverity(
      final RequestContext requestContext, final VulnerabilitySeverity severity) {
    switch (severity) {
      case VULNERABILITY_SEVERITY_LOW:
      case VULNERABILITY_SEVERITY_MEDIUM:
      case VULNERABILITY_SEVERITY_HIGH:
      case VULNERABILITY_SEVERITY_CRITICAL:
        break;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(
                String.format("Encountered unknown vulnerability severity: %s", severity))
            .asRuntimeException(requestContext.buildTrailers());
    }
  }

  @Override
  public void validateOrThrow(
      RequestContext requestContext, GetVulnerabilityMetadataOverridesRequest request) {
    validateOrThrow(requestContext);
    if (request.getMetadataId().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Request should have metadata_id to update the Plugin")
          .asRuntimeException(requestContext.buildTrailers());
    }
  }

  @Override
  public void validateOrThrow(
      RequestContext requestContext, DeleteVulnerabilityMetadataOverridesConfigRequest request) {
    validateOrThrow(requestContext);
    if (request.getMetadataId().isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Request should have metadata_id to update the Plugin")
          .asRuntimeException(requestContext.buildTrailers());
    }
  }

  @Override
  public void validateOrThrow(
      RequestContext requestContext, UpdateCustomTestPluginRequest request) {
    UpdateCustomTestPlugin updateCustomTestPlugin = request.getUpdateCustomTestPlugin();
    validateOrThrow(requestContext);
    if (updateCustomTestPlugin.getId().isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Request should have custom plugin id to update the Custom Plugin")
          .asRuntimeException(requestContext.buildTrailers());
    }

    if (updateCustomTestPlugin.getName().isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Request should have custom plugin name to update the Custom Plugin")
          .asRuntimeException(requestContext.buildTrailers());
    }
    validateCodeSnippetDetails(requestContext, updateCustomTestPlugin.getCodeSnippetDetails());
    validateStringBlankness(requestContext, updateCustomTestPlugin.getDescription());
    validateStringBlankness(requestContext, updateCustomTestPlugin.getPluginDetails());
    validateStringList(
        requestContext, updateCustomTestPlugin.getPotentialGeneratedVulnerabilityTypesList());
    validateTestPluginType(requestContext, updateCustomTestPlugin.getPluginType());
    validateTestPluginSafetyType(requestContext, updateCustomTestPlugin.getPluginSafetyType());
    validateSampleData(requestContext, updateCustomTestPlugin.getSampleData());
  }

  @Override
  public void validateOrThrow(
      RequestContext requestContext, DeleteCustomTestPluginRequest request) {
    validateOrThrow(requestContext);
    if (request.getId().isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Request should have custom plugin id to delete the Custom Plugin")
          .asRuntimeException(requestContext.buildTrailers());
    }
  }

  @Override
  public void validateOrThrow(
      RequestContext requestContext, CreateCustomTestPluginRequest request) {
    CreateCustomTestPlugin createCustomTestPlugin = request.getCreateCustomTestPlugin();
    validateOrThrow(requestContext);
    validateCodeSnippetDetails(requestContext, createCustomTestPlugin.getCodeSnippetDetails());
    if (createCustomTestPlugin.getName().isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Request should have custom plugin name to create the Custom Plugin")
          .asRuntimeException(requestContext.buildTrailers());
    }
    validateCodeSnippetDetails(requestContext, createCustomTestPlugin.getCodeSnippetDetails());
    validateStringBlankness(requestContext, createCustomTestPlugin.getDescription());
    validateStringBlankness(requestContext, createCustomTestPlugin.getPluginDetails());
    validateStringList(
        requestContext, createCustomTestPlugin.getPotentialGeneratedVulnerabilityTypesList());
    validateTestPluginType(requestContext, createCustomTestPlugin.getPluginType());
    validateTestPluginSafetyType(requestContext, createCustomTestPlugin.getPluginSafetyType());
    validateSampleData(requestContext, createCustomTestPlugin.getSampleData());
  }

  @Override
  public void validateOrThrow(
      RequestContext requestContext, GetAllVulnerabilityMetadataOverridesRequest request) {
    validateOrThrow(requestContext);
  }

  @Override
  public void validateOrThrow(RequestContext requestContext, GetAstFeatureConfigsRequest request) {
    validateOrThrow(requestContext);
    if (request.hasFilter()
        && request
            .getFilter()
            .getFilterCase()
            .equals(AstFeatureConfigFilter.FilterCase.ENVIRONMENT_ID_FILTER)
        && request.getFilter().getEnvironmentIdFilter().getEnvironmentIdsList().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              "The environment filter in  AstFeatureConfigFilter has no environment ids : "
                  + request.getFilter())
          .asRuntimeException(requestContext.buildTrailers());
    }
  }

  @Override
  public void validateOrThrow(
      RequestContext requestContext, UpdateAstFeatureConfigRequest request) {
    validateOrThrow(requestContext);
    validateEnvironmentId(requestContext, request.getEnvironmentId());
    if (request
        .getUpdateStatusCase()
        .equals(UpdateAstFeatureConfigRequest.UpdateStatusCase.UPDATESTATUS_NOT_SET)) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Update Ast feature config status not set.")
          .asRuntimeException(requestContext.buildTrailers());
    }
    if (request.getEnabledConfig().getReplayConfig().hasSpanFilters()) {
      validateSpanFilters(request.getEnabledConfig().getReplayConfig().getSpanFilters());
    }
  }

  private void validateSpanFilters(SpanFilters spanFilters) {
    spanFilters
        .getConditionsList()
        .forEach(
            condition -> {
              Preconditions.checkArgument(
                  condition.hasKeyValuePredicate(),
                  "Key Value Predicate in Span Filters Not Present");
              Preconditions.checkArgument(
                  condition.getKeyValuePredicate().hasKeyPredicate(),
                  "Key Predicate in Span Filters Not Present");
              Preconditions.checkArgument(
                  !condition.getLocation().equals(Location.LOCATION_UNSPECIFIED),
                  "Location for Span Filters is Not Specified");
              StringPredicate keyPredicate = condition.getKeyValuePredicate().getKeyPredicate();
              validateStringPredicate(keyPredicate);
              if (condition.getKeyValuePredicate().hasValuePredicate()) {
                StringPredicate valuePredicate =
                    condition.getKeyValuePredicate().getValuePredicate();
                validateStringPredicate(valuePredicate);
              }
            });
  }

  private void validateStringPredicate(StringPredicate stringPredicate) {
    Preconditions.checkArgument(
        !stringPredicate.getOperator().equals(RelationalOperator.RELATIONAL_OPERATOR_UNSPECIFIED),
        "Relational Operator in Predicate for Span Filters is Not Specified");
    if (stringPredicate
        .getOperator()
        .equals(RelationalOperator.RELATIONAL_OPERATOR_MATCHES_REGEX)) {
      Preconditions.checkArgument(
          isValidRegex(stringPredicate.getValue()),
          "Regex provided in value for Span Filters is invalid");
    }
  }

  private boolean isValidRegex(final String regex) {
    try {
      Pattern.compile(regex);
      return true;
    } catch (PatternSyntaxException e) {
      return false;
    }
  }

  private void validateOrThrow(RequestContext requestContext) {
    GrpcValidatorUtils.validateRequestContextOrThrow(requestContext);
  }

  private void validateEnvironmentId(RequestContext requestContext, String environmentId) {
    if (environmentId.isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Request should have an environment_id to update the Ast Feature Config")
          .asRuntimeException(requestContext.buildTrailers());
    }
  }

  private void validateCodeSnippetDetails(
      RequestContext requestContext, CodeSnippetDetails snippetDetails) {
    String codeSnippet = snippetDetails.getCodeSnippet();
    if (codeSnippet.isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Request should have code snippet")
          .asRuntimeException(requestContext.buildTrailers());
    }

    if (CodeSnippetType.CODE_SNIPPET_TYPE_UNSPECIFIED.equals(snippetDetails.getCodeSnippetType())) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Request should have specified code snippet type")
          .asRuntimeException(requestContext.buildTrailers());
    }
  }

  private void validateSampleData(RequestContext requestContext, SampleData sampleData) {
    if (hasHttpRestApiDetails(sampleData)) {
      validateHttpRestApiDetails(requestContext, sampleData.getHttpRestApiDetails());
    }
  }

  private boolean hasHttpRestApiDetails(SampleData sampleData) {
    return !SampleData.getDefaultInstance().equals(sampleData)
        && sampleData.hasHttpRestApiDetails();
  }

  private void validateHttpRestApiDetails(
      RequestContext requestContext, HttpRestApiDetails httpRestApiDetails) {
    validateHttpMethod(requestContext, httpRestApiDetails.getMethod());
    validateTargetUrl(requestContext, httpRestApiDetails.getTargetUrl());
  }

  private void validateTargetUrl(RequestContext context, String targetUrl) {
    validateStringBlankness(context, targetUrl);
  }

  private void validateHttpMethod(RequestContext context, HttpMethod httpMethod) {
    if (HttpMethod.HTTP_METHOD_UNSPECIFIED.equals(httpMethod)) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Request should have a specified http method")
          .asRuntimeException(context.buildTrailers());
    }
  }

  private void validateStringBlankness(RequestContext requestContext, String stringsField) {
    if (stringsField.isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Request should have a non-blank string")
          .asRuntimeException(requestContext.buildTrailers());
    }
  }

  private void validateStringList(RequestContext requestContext, List<String> stringsFieldList) {
    if (stringsFieldList.isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Request should have a non-empty list of strings")
          .asRuntimeException(requestContext.buildTrailers());
    }
    stringsFieldList.forEach(string -> validateStringBlankness(requestContext, string));
  }

  private void validateTestPluginType(RequestContext requestContext, TestPluginType pluginType) {
    if (TestPluginType.TEST_PLUGIN_TYPE_UNSPECIFIED.equals(pluginType)) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Request should have a specified plugin type")
          .asRuntimeException(requestContext.buildTrailers());
    }
  }

  private void validateTestPluginSafetyType(
      RequestContext requestContext, TestPluginSafetyType pluginSafetyType) {
    if (TestPluginSafetyType.TEST_PLUGIN_SAFETY_TYPE_UNSPECIFIED.equals(pluginSafetyType)) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Request should have a specified plugin safety type")
          .asRuntimeException(requestContext.buildTrailers());
    }
  }
}
