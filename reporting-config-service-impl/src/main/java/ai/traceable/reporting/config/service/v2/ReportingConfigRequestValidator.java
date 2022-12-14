package ai.traceable.reporting.config.service.v2;

import org.hypertrace.core.grpcutils.context.RequestContext;

public interface ReportingConfigRequestValidator {

  void validateCreateReportConfigurationRequest(
      RequestContext requestContext, CreateReportConfigurationRequest request);

  void validateUpdateReportConfigurationRequest(
      RequestContext requestContext, UpdateReportConfigurationRequest request);

  void validateGetReportConfigurationsRequest(
      RequestContext requestContext, GetReportConfigurationsRequest request);

  void validateDeleteReportConfigurationRequest(
      RequestContext requestContext, DeleteReportConfigurationRequest request);
}
