package ai.traceable.dashboard.config.service;

import ai.traceable.dashboard.config.service.v1.Dashboard;
import ai.traceable.dashboard.config.service.v1.GetDashboardsRequest;
import com.google.protobuf.Value;
import java.util.Optional;
import javax.inject.Inject;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;

@Slf4j
class DashboardStore
    extends IdentifiedObjectStoreWithFilter<Dashboard, GetDashboardsRequest.DashboardFilter> {

  private static final String DASHBOARD_RESOURCE_NAME = "dashboard";
  private static final String DASHBOARD_NAMESPACE = "dashboards";

  @Inject
  DashboardStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        DASHBOARD_NAMESPACE,
        DASHBOARD_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<Dashboard> buildDataFromValue(Value ruleValue) {
    try {
      Dashboard.Builder builder = Dashboard.newBuilder();
      ConfigProtoConverter.mergeFromValue(ruleValue, builder);
      return Optional.of(builder.build());
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(Dashboard rule) {
    return ConfigProtoConverter.convertToValue(rule);
  }

  @Override
  protected String getContextFromData(Dashboard data) {
    return data.getId();
  }

  @Override
  protected Optional<Dashboard> filterConfigData(
      Dashboard data, GetDashboardsRequest.DashboardFilter filter) {
    return Optional.of(data)
        .filter(
            dashboard ->
                !filter.hasUiReference()
                    || filter.getUiReference().equals(dashboard.getUiReference()))
        .filter(
            dashboard ->
                !filter.hasDashboardId() || filter.getDashboardId().equals(dashboard.getId()));
  }
}
