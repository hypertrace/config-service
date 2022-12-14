package ai.traceable.reporting.config.service.v1;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import io.grpc.Status;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class ReportingConfigRequestValidator {

  public void validateCreateReportConfigurationRequest(
      RequestContext requestContext, CreateReportConfigurationRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateReportConfigurationDetailsData(request.getReportConfigurationDetails());
  }

  public void validateUpdateReportConfigurationRequest(
      RequestContext requestContext, UpdateReportConfigurationRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, UpdateReportConfigurationRequest.ID_FIELD_NUMBER);
    validateReportConfigurationDetailsData(request.getReportConfigurationDetails());
  }

  public void validateUpdateReportExecutionTimeRequest(
      RequestContext requestContext, UpdateReportExecutionTimeRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, UpdateReportExecutionTimeRequest.ID_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        request, UpdateReportExecutionTimeRequest.LAST_EXECUTED_TIMESTAMP_MILLIS_FIELD_NUMBER);
  }

  private void validateReportConfigurationDetailsData(ReportConfigurationDetails data) {
    if (!data.hasReportFrequency()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Missing expected Tenant ID")
          .asRuntimeException();
    }
  }

  public void validateGetAllReportConfigurationsRequest(
      RequestContext requestContext, GetAllReportConfigurationsRequest request) {
    validateRequestContextOrThrow(requestContext);
  }

  public void validateDeleteReportConfigurationRequest(
      RequestContext requestContext, DeleteReportConfigurationRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, DeleteReportConfigurationRequest.ID_FIELD_NUMBER);
  }
}
