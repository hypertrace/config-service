package ai.traceable.reporting.config.service.v2;

import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface ReportingConfigManager {

  ReportConfiguration createReportConfiguration(
      RequestContext requestContext, CommonConfigurationDetails commonConfigurationDetails);

  List<ReportConfiguration> getReportConfigurations(
      RequestContext requestContext, GetReportsFilter filter);

  ReportConfiguration updateReportConfiguration(
      RequestContext requestContext,
      String reportConfigId,
      CommonConfigurationDetails commonConfigurationDetails);

  void deleteReportConfiguration(RequestContext requestContext, String reportConfigId);
}
