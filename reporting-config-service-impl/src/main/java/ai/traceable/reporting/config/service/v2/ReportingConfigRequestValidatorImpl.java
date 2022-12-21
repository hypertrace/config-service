package ai.traceable.reporting.config.service.v2;

import static ai.traceable.reporting.config.service.v2.Format.FORMAT_UNSPECIFIED;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import io.grpc.Status;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class ReportingConfigRequestValidatorImpl implements ReportingConfigRequestValidator {

  public void validateCreateReportConfigurationRequest(
      RequestContext requestContext, CreateReportConfigurationRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateCommonConfigurationDetails(request.getCommonConfigurationDetails());
  }

  public void validateUpdateReportConfigurationRequest(
      RequestContext requestContext, UpdateReportConfigurationRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, UpdateReportConfigurationRequest.ID_FIELD_NUMBER);
    validateCommonConfigurationDetails(request.getCommonConfigurationDetails());
  }

  public void validateGetReportConfigurationsRequest(
      RequestContext requestContext, GetReportConfigurationsRequest request) {
    validateRequestContextOrThrow(requestContext);
  }

  public void validateDeleteReportConfigurationRequest(
      RequestContext requestContext, DeleteReportConfigurationRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, DeleteReportConfigurationRequest.ID_FIELD_NUMBER);
  }

  private void validateCommonConfigurationDetails(CommonConfigurationDetails data) {
    // If schedule is enabled, then notification details must be present
    if (data.hasSchedulingDetails() && !data.hasNotificationDetails()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Scheduling is enabled, but missing notification details")
          .asRuntimeException();
    }

    if (FORMAT_UNSPECIFIED.equals(data.getFormat())) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Unspecified report format")
          .asRuntimeException();
    }
  }
}
