package org.hypertrace.alerting.config.service;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import com.google.common.base.Preconditions;
import org.hypertrace.alerting.config.service.v1.CreateEventConditionRequest;
import org.hypertrace.alerting.config.service.v1.DeleteEventConditionRequest;
import org.hypertrace.alerting.config.service.v1.EventConditionData;
import org.hypertrace.alerting.config.service.v1.GetAllEventConditionsRequest;
import org.hypertrace.alerting.config.service.v1.UpdateEventConditionRequest;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class EventConditionConfigServiceRequestValidator {

  public void validateCreateEventConditionRequest(
      RequestContext requestContext, CreateEventConditionRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(
        request, CreateEventConditionRequest.NEW_EVENT_CONDITION_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        request.getNewEventCondition().getEventConditionData(),
        EventConditionData.METRIC_ANOMALY_EVENT_CONDITION_FIELD_NUMBER);
  }

  public void validateUpdateEventConditionRequest(
      RequestContext requestContext, UpdateEventConditionRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(
        request, UpdateEventConditionRequest.EVENT_CONDITION_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        request.getEventCondition().getEventConditionData(),
        EventConditionData.METRIC_ANOMALY_EVENT_CONDITION_FIELD_NUMBER);
  }

  public void validateGetAllEventConditionsRequest(
      RequestContext requestContext, GetAllEventConditionsRequest request) {
    validateRequestContextOrThrow(requestContext);
  }

  public void validateDeleteEventConditionRequest(
      RequestContext requestContext, DeleteEventConditionRequest request) {
    validateRequestContextOrThrow(requestContext);
    Preconditions.checkArgument(
        !request.getEventConditionId().isBlank(), "EventCondition Id cannot be empty");
  }
}
