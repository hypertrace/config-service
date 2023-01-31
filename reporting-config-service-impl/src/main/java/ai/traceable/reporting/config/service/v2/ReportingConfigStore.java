package ai.traceable.reporting.config.service.v2;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import java.util.Optional;
import javax.inject.Inject;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;

@Slf4j
public class ReportingConfigStore
    extends IdentifiedObjectStoreWithFilter<ReportConfiguration, GetReportsFilter> {

  private static final String REPORTING_CONFIG_NAMESPACE = "reporting";
  private static final String REPORTING_CONFIG_RESOURCE_NAME = "reporting-config-v2";

  @Inject
  public ReportingConfigStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        REPORTING_CONFIG_NAMESPACE,
        REPORTING_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<ReportConfiguration> buildDataFromValue(Value value) {
    ReportConfiguration.Builder builder = ReportConfiguration.newBuilder();
    try {
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (InvalidProtocolBufferException e) {
      log.error("Failed to create ReportConfiguration from value: {}", value, e);
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(ReportConfiguration object) {
    return ConfigProtoConverter.convertToValue(object);
  }

  @Override
  protected String getContextFromData(ReportConfiguration object) {
    return object.getId();
  }

  @Override
  protected Optional<ReportConfiguration> filterConfigData(
      ReportConfiguration reportConfiguration, GetReportsFilter reportsFilter) {
    return Optional.of(reportConfiguration)
        .filter(report -> filterById(report, reportsFilter))
        .filter(report -> filterByEnvironmentId(report, reportsFilter))
        .filter(report -> filterByName(report, reportsFilter))
        .filter(report -> filterByCreator(report, reportsFilter));
  }

  private boolean filterById(ReportConfiguration reportConfiguration, GetReportsFilter filter) {
    return !filter.hasId() || filter.getId().equals(reportConfiguration.getId());
  }

  private boolean filterByEnvironmentId(
      ReportConfiguration reportConfiguration, GetReportsFilter filter) {
    // If filter does not have any environmentId, then return true
    // Else if filter contains environment Id, then it must match with that present within report.
    return !filter.hasEnvironmentId()
        || filter
            .getEnvironmentId()
            .equals(reportConfiguration.getCommonConfigurationDetails().getEnvironmentId());
  }

  private boolean filterByName(ReportConfiguration reportConfiguration, GetReportsFilter filter) {
    return filter.getNamesList().isEmpty()
        || filter
            .getNamesList()
            .contains(reportConfiguration.getCommonConfigurationDetails().getName());
  }

  private boolean filterByCreator(
      ReportConfiguration reportConfiguration, GetReportsFilter filter) {
    return filter.getCreatorsList().isEmpty()
        || filter.getCreatorsList().contains(reportConfiguration.getCreator());
  }
}
