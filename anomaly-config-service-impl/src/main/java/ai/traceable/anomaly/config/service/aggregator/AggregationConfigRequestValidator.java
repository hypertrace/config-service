package ai.traceable.anomaly.config.service.aggregator;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.anomaly.config.service.v1.aggregator.DeleteScopedAnomalyEventAggregationConfigRequest;
import ai.traceable.anomaly.config.service.v1.aggregator.EventAggregationConfig;
import ai.traceable.anomaly.config.service.v1.aggregator.EventAggregationFamilyConfig;
import ai.traceable.anomaly.config.service.v1.aggregator.GetAllScopedAnomalyEventAggregationConfigsRequest;
import ai.traceable.anomaly.config.service.v1.aggregator.GetAllUnresolvedScopedAnomalyEventAggregationConfigsRequest;
import ai.traceable.anomaly.config.service.v1.aggregator.GetScopedAnomalyEventAggregationConfigRequest;
import ai.traceable.anomaly.config.service.v1.aggregator.GetUnresolvedScopedAnomalyEventAggregationConfigRequest;
import ai.traceable.anomaly.config.service.v1.aggregator.ScopedAnomalyEventAggregationConfig;
import ai.traceable.anomaly.config.service.v1.aggregator.UpdateScopedAnomalyEventAggregationConfigRequest;
import io.grpc.Status;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class AggregationConfigRequestValidator {

  public void validateUpdateAggregatorConfigurationRequest(
      RequestContext requestContext, UpdateScopedAnomalyEventAggregationConfigRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateScopeAnomalyEventAggregationConfig(request.getScopedAnomalyEventAggregationConfig());
  }

  public void validateGetAllScopedAnomalyEventAggregationConfigsRequest(
      RequestContext requestContext, GetAllScopedAnomalyEventAggregationConfigsRequest request) {
    validateRequestContextOrThrow(requestContext);
  }

  public void validateDeleteScopedAnomalyEventAggregationConfigRequest(
      RequestContext requestContext, DeleteScopedAnomalyEventAggregationConfigRequest request) {
    validateRequestContextOrThrow(requestContext);
    if (!request.hasConfigScope()) {
      throw Status.INVALID_ARGUMENT.withDescription("Anomaly Scope missing ").asRuntimeException();
    }
  }

  public void validateGetAllUnresolvedScopedAnomalyEventAggregationConfigs(
      RequestContext requestContext,
      GetAllUnresolvedScopedAnomalyEventAggregationConfigsRequest request) {
    validateRequestContextOrThrow(requestContext);
  }

  public void validateGetScopedAnomalyEventAggregationConfig(
      RequestContext requestContext, GetScopedAnomalyEventAggregationConfigRequest request) {
    validateRequestContextOrThrow(requestContext);
    if (!request.hasConfigScope()) {
      throw Status.INVALID_ARGUMENT.withDescription("Anomaly Scope missing ").asRuntimeException();
    }
  }

  public void validateGetUnresolvedScopedAnomalyEventAggregationConfig(
      RequestContext requestContext,
      GetUnresolvedScopedAnomalyEventAggregationConfigRequest request) {
    validateRequestContextOrThrow(requestContext);
    if (!request.hasConfigScope()) {
      throw Status.INVALID_ARGUMENT.withDescription("Anomaly Scope missing ").asRuntimeException();
    }
  }

  private void validateScopeAnomalyEventAggregationConfig(
      ScopedAnomalyEventAggregationConfig scopedAnomalyEventAggregationConfig) {
    if (!scopedAnomalyEventAggregationConfig.hasConfigScope()) {
      throw Status.INVALID_ARGUMENT.withDescription("Anomaly Scope missing ").asRuntimeException();
    }
    validateEventAggregationConfig(scopedAnomalyEventAggregationConfig.getEventAggregationConfig());
  }

  private void validateEventAggregationConfig(EventAggregationConfig eventAggregationConfig) {
    if (!eventAggregationConfig.getFamilyConfigsList().isEmpty()) {
      for (EventAggregationFamilyConfig eventAggregationFamilyConfig :
          eventAggregationConfig.getFamilyConfigsList()) {
        validateNonDefaultPresenceOrThrow(
            eventAggregationFamilyConfig,
            EventAggregationFamilyConfig.ANOMALY_EVENT_FAMILY_FIELD_NUMBER);
      }
    }
  }
}
