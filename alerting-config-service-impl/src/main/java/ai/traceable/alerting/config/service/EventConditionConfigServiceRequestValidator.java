package ai.traceable.alerting.config.service;

import static org.hypertrace.config.validation.GrpcValidatorUtils.printMessage;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.alerting.config.service.v2.CreateEventConditionRequest;
import ai.traceable.alerting.config.service.v2.DeleteEventConditionRequest;
import ai.traceable.alerting.config.service.v2.EventCondition;
import ai.traceable.alerting.config.service.v2.EventConditionMutableData;
import ai.traceable.alerting.config.service.v2.GetAllEventConditionsRequest;
import ai.traceable.alerting.config.service.v2.UpdateEventConditionRequest;
import io.grpc.Status;
import org.hypertrace.alerting.config.service.v1.MetricAnomalyEventCondition;
import org.hypertrace.alerting.config.service.v1.MetricSelection;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class EventConditionConfigServiceRequestValidator {

  public void validateCreateEventConditionRequest(
      RequestContext requestContext, CreateEventConditionRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateEventConditionMutableData(request.getEventConditionMutableData());
  }

  public void validateUpdateEventConditionRequest(
      RequestContext requestContext, UpdateEventConditionRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, UpdateEventConditionRequest.ID_FIELD_NUMBER);
    validateEventConditionMutableData(request.getEventConditionMutableData());
  }

  private void validateEventConditionMutableData(EventConditionMutableData data) {
    switch (data.getConditionCase()) {
      case METRIC_ANOMALY_EVENT_CONDITION:
        // todo add detailed check
        validateNonDefaultPresenceOrThrow(
            data.getMetricAnomalyEventCondition(),
            MetricAnomalyEventCondition.EVALUATION_WINDOW_DURATION_FIELD_NUMBER);
        validateNonDefaultPresenceOrThrow(
            data.getMetricAnomalyEventCondition().getMetricSelection(),
            MetricSelection.METRIC_AGGREGATION_INTERVAL_FIELD_NUMBER);
        break;
      case CONDITION_NOT_SET:
        throw Status.INVALID_ARGUMENT
            .withDescription("Event condition must be set: " + printMessage(data))
            .asRuntimeException();
    }
  }

  public void validateGetAllEventConditionsRequest(
      RequestContext requestContext, GetAllEventConditionsRequest request) {
    validateRequestContextOrThrow(requestContext);
  }

  public void validateDeleteEventConditionRequest(
      RequestContext requestContext, DeleteEventConditionRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, EventCondition.ID_FIELD_NUMBER);
  }
}
