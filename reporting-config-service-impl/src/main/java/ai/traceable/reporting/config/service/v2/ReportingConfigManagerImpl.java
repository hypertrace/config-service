package ai.traceable.reporting.config.service.v2;

import ai.traceable.config.utils.UuidGenerator;
import io.grpc.Status;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
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
            .setCreator(getReportCreator(requestContext).orElseThrow())
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
            .orElseThrow(() -> Status.NOT_FOUND.asRuntimeException(requestContext.buildTrailers()));

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
  public void deleteReportConfiguration(RequestContext requestContext, String reportConfigId) {
    reportingConfigStore
        .deleteObject(requestContext, reportConfigId)
        .orElseThrow(() -> Status.NOT_FOUND.asRuntimeException(requestContext.buildTrailers()));
  }

  private Optional<String> getReportCreator(RequestContext requestContext) {
    if (requestContext.getName().isPresent()) {
      return requestContext.getName();
    } else if (requestContext.getEmail().isPresent()) {
      return requestContext.getEmail();
    } else {
      return requestContext.getUserId();
    }
  }
}
