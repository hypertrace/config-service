package ai.traceable.reporting.config.service.v2;

import ai.traceable.config.utils.UuidGenerator;
import io.grpc.Status;
import java.util.List;
import javax.inject.Inject;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class ReportingConfigManagerImpl implements ReportingConfigManager {
  private final ReportingConfigStore reportingConfigStore;
  private final UuidGenerator uuidGenerator;

  @Inject
  public ReportingConfigManagerImpl(
      ReportingConfigStore reportingConfigStore, UuidGenerator uuidGenerator) {
    this.reportingConfigStore = reportingConfigStore;
    this.uuidGenerator = uuidGenerator;
  }

  @Override
  public ReportConfiguration createReportConfiguration(
      RequestContext requestContext, CommonConfigurationDetails commonConfigurationDetails) {
    ReportConfiguration reportConfiguration =
        ReportConfiguration.newBuilder()
            .setId(uuidGenerator.generateRandomId())
            .setCreator(requestContext.getUserId().orElseThrow())
            .setCommonConfigurationDetails(commonConfigurationDetails)
            .build();

    return reportingConfigStore.upsertObject(requestContext, reportConfiguration).getData();
  }

  @Override
  public ReportConfiguration updateReportConfiguration(
      RequestContext requestContext,
      String reportConfigId,
      CommonConfigurationDetails commonConfigurationDetails) {
    ReportConfiguration existingConfiguration =
        reportingConfigStore
            .getData(requestContext, reportConfigId)
            .orElseThrow(Status.NOT_FOUND::asRuntimeException);

    ReportConfiguration updatedConfiguration =
        existingConfiguration.toBuilder()
            .setCommonConfigurationDetails(commonConfigurationDetails)
            .build();
    return reportingConfigStore.upsertObject(requestContext, updatedConfiguration).getData();
  }

  @Override
  public List<ReportConfiguration> getReportConfigurations(
      RequestContext requestContext, GetReportsFilter filter) {
    return reportingConfigStore.getAllConfigData(requestContext, filter);
  }

  @Override
  public ReportConfiguration deleteReportConfiguration(
      RequestContext requestContext, String reportConfigId) {
    return reportingConfigStore
        .deleteObject(requestContext, reportConfigId)
        .orElseThrow(Status.NOT_FOUND::asRuntimeException)
        .getData();
  }
}
