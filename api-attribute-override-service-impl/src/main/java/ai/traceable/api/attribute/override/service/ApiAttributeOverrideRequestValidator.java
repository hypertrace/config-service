package ai.traceable.api.attribute.override.service;

import ai.traceable.api.attribute.override.service.v1.AttributeOverride;
import ai.traceable.api.attribute.override.service.v1.AttributeOverrideIdentifier;
import ai.traceable.api.attribute.override.service.v1.GetAllApiAttributeOverridesRequest;
import ai.traceable.api.attribute.override.service.v1.ParamTypeAction;
import ai.traceable.api.attribute.override.service.v1.ParamTypeOverride;
import ai.traceable.api.attribute.override.service.v1.RemoveApiAttributeOverridesRequest;
import ai.traceable.api.attribute.override.service.v1.UpsertApiAttributeOverridesRequest;
import com.google.common.base.Strings;
import io.grpc.Status;

public class ApiAttributeOverrideRequestValidator {

  public Status validate(GetAllApiAttributeOverridesRequest request) {
    if (request.hasFilter()
        && request.getFilter().getApiIdsList().stream().anyMatch(Strings::isNullOrEmpty)) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Invalid filter to get api attribute overrides, invalid request");
    }
    return Status.OK;
  }

  public Status validate(UpsertApiAttributeOverridesRequest request) {
    if (Strings.isNullOrEmpty(request.getApiId())) {
      return Status.INVALID_ARGUMENT.withDescription("Api Id not found while creating the rule");
    }
    if (request.getAttributeOverridesList().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "No override provided while creating the rule [%s]");
    }
    if (request.getAttributeOverridesList().stream()
        .anyMatch(attributeOverride -> !validateAttributeOverride(attributeOverride).isOk())) {
      return Status.INVALID_ARGUMENT.withDescription("Invalid input for attribute override");
    }
    return Status.OK;
  }

  public Status validate(RemoveApiAttributeOverridesRequest request) {
    if (Strings.isNullOrEmpty(request.getApiId())) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Api Id not found while removing the override");
    }
    if (request.getAttributeOverrideIdentifiersList().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "No override provided while removing the override");
    }
    if (request.getAttributeOverrideIdentifiersList().stream()
        .anyMatch(overrideIdentifier -> !validateAttributeIdentifier(overrideIdentifier).isOk())) {
      return Status.INVALID_ARGUMENT.withDescription("Invalid input for attribute override");
    }
    return Status.OK;
  }

  private Status validateAttributeIdentifier(AttributeOverrideIdentifier overrideIdentifier) {
    switch (overrideIdentifier.getAttributeOverrideIdentifierCase()) {
      case PARAM_TYPE_OVERRIDE_IDENTIFIER:
        if (Strings.isNullOrEmpty(
            overrideIdentifier.getParamTypeOverrideIdentifier().getParamName())) {
          return Status.INVALID_ARGUMENT;
        }
        return Status.OK;
      case API_EXTERNAL_OVERRIDE_IDENTIFIER:
        return Status.OK;
      default:
        return Status.INVALID_ARGUMENT;
    }
  }

  private Status validateAttributeOverride(AttributeOverride attributeOverride) {
    switch (attributeOverride.getAttributeOverrideCase()) {
      case PARAM_TYPE_OVERRIDE:
        return validateParamTypeOverride(attributeOverride.getParamTypeOverride());
      case API_EXTERNAL_OVERRIDE:
        return Status.OK;
      default:
        return Status.INVALID_ARGUMENT;
    }
  }

  private Status validateParamTypeOverride(ParamTypeOverride paramTypeOverride) {
    if (Strings.isNullOrEmpty(paramTypeOverride.getParamName())) {
      return Status.INVALID_ARGUMENT.withDescription("Param name not found in param Type override");
    }
    if (ParamTypeAction.PARAM_TYPE_ACTION_UNSPECIFIED.equals(
        paramTypeOverride.getParamTypeAction())) {
      return Status.INVALID_ARGUMENT.withDescription("Unspecified param type action");
    }
    return Status.OK;
  }
}
