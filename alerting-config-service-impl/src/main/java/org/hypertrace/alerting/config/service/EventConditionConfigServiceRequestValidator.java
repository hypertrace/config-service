package org.hypertrace.alerting.config.service;

import com.google.common.base.Preconditions;
import org.hypertrace.alerting.config.service.v1.CreateEventConditionRequest;
import org.hypertrace.alerting.config.service.v1.DeleteEventConditionRequest;
import org.hypertrace.alerting.config.service.v1.GetAllEventConditionsRequest;
import org.hypertrace.alerting.config.service.v1.UpdateEventConditionRequest;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class EventConditionConfigServiceRequestValidator {

  public void validateCreateEventConditionRequest(
      RequestContext requestContext, CreateEventConditionRequest request) {
    validateTenantID(requestContext);
    Preconditions.checkArgument(request.hasNewEventCondition(), "EventCondition should be present");
    Preconditions.checkArgument(
        request.getNewEventCondition().getEventConditionData().getMetricAnomalyEventCondition()
            != null,
        "MetricAnomalyEventCondition should be present");
  }

  public void validateUpdateEventConditionRequest(
      RequestContext requestContext, UpdateEventConditionRequest request) {
    validateTenantID(requestContext);
    Preconditions.checkArgument(request.hasEventCondition(), "EventCondition should be present");
    Preconditions.checkArgument(
        request.getEventCondition().getEventConditionData().getMetricAnomalyEventCondition()
            != null,
        "MetricAnomalyEventCondition should be present");
  }

  public void validateGetAllEventConditionsRequest(
      RequestContext requestContext, GetAllEventConditionsRequest request) {
    validateTenantID(requestContext);
  }

  public void validateDeleteEventConditionRequest(
      RequestContext requestContext, DeleteEventConditionRequest request) {
    validateTenantID(requestContext);
    Preconditions.checkArgument(
        !request.getEventConditionId().isBlank(), "EventCondition Id cannot be empty");
  }

  private void validateTenantID(RequestContext requestContext) {
    Preconditions.checkArgument(
        requestContext.getTenantId().isPresent(), "Tenant ID is not present in request context");
  }
}
