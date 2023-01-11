package ai.traceable.span.processing.config.service.servicenaming;

import ai.traceable.span.processing.config.service.v1.CreateServiceNamingRuleRequest;
import ai.traceable.span.processing.config.service.v1.ServiceNamingRuleAction;
import ai.traceable.span.processing.config.service.v1.ServiceNamingRuleAction.StaticNameAssignment;
import ai.traceable.span.processing.config.service.v1.ServiceNamingRuleCondition;
import ai.traceable.span.processing.config.service.v1.ServiceNamingRuleCondition.AttributeCondition;
import ai.traceable.span.processing.config.service.v1.UpdateServiceNamingRuleRequest;
import io.grpc.Status;
import org.hypertrace.config.validation.GrpcValidatorUtils;
import org.hypertrace.core.grpcutils.context.RequestContext;

class ServiceNamingRequestValidator {
  void validateOrThrow(RequestContext requestContext, CreateServiceNamingRuleRequest request) {
    this.validateOrThrow(requestContext);
    GrpcValidatorUtils.validateNonDefaultPresenceOrThrow(
        request, CreateServiceNamingRuleRequest.NAME_FIELD_NUMBER);
    GrpcValidatorUtils.validateNonDefaultPresenceOrThrow(
        request, CreateServiceNamingRuleRequest.CONDITIONS_FIELD_NUMBER);
    request.getConditionsList().forEach(this::validateConditionOrThrow);
    this.validateActionOrThrow(request.getAction());
  }

  void validateOrThrow(RequestContext requestContext, UpdateServiceNamingRuleRequest request) {
    this.validateOrThrow(requestContext);
    GrpcValidatorUtils.validateNonDefaultPresenceOrThrow(
        request, UpdateServiceNamingRuleRequest.ID_FIELD_NUMBER);
    GrpcValidatorUtils.validateNonDefaultPresenceOrThrow(
        request, UpdateServiceNamingRuleRequest.NAME_FIELD_NUMBER);
    GrpcValidatorUtils.validateNonDefaultPresenceOrThrow(
        request, UpdateServiceNamingRuleRequest.CONDITIONS_FIELD_NUMBER);
    request.getConditionsList().forEach(this::validateConditionOrThrow);
    this.validateActionOrThrow(request.getAction());
  }

  void validateOrThrow(RequestContext requestContext) {
    GrpcValidatorUtils.validateRequestContextOrThrow(requestContext);
  }

  private void validateConditionOrThrow(ServiceNamingRuleCondition condition) {
    switch (condition.getConditionCase()) {
      case ATTRIBUTE_CONDITION:
        this.validateAttributeConditionOrThrow(condition.getAttributeCondition());
        return;
      case CONDITION_NOT_SET:
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(
                String.format("Invalid condition received with unrecognized case: %s", condition))
            .asRuntimeException();
    }
  }

  private void validateAttributeConditionOrThrow(AttributeCondition condition) {
    GrpcValidatorUtils.validateNonDefaultPresenceOrThrow(
        condition, AttributeCondition.ATTRIBUTE_KEY_FIELD_NUMBER);
    GrpcValidatorUtils.validateNonDefaultPresenceOrThrow(
        condition, AttributeCondition.OPERATOR_FIELD_NUMBER);
    GrpcValidatorUtils.validateNonDefaultPresenceOrThrow(
        condition, AttributeCondition.VALUE_FIELD_NUMBER);
  }

  private void validateActionOrThrow(ServiceNamingRuleAction action) {
    switch (action.getActionCase()) {
      case STATIC_NAME_ASSIGNMENT:
        this.validateStaticNameActionOrThrow(action.getStaticNameAssignment());
        return;
      case ACTION_NOT_SET:
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(
                String.format("Invalid action received with unrecognized case: %s", action))
            .asRuntimeException();
    }
  }

  private void validateStaticNameActionOrThrow(StaticNameAssignment action) {
    GrpcValidatorUtils.validateNonDefaultPresenceOrThrow(
        action, StaticNameAssignment.SERVICE_NAME_FIELD_NUMBER);
  }
}
