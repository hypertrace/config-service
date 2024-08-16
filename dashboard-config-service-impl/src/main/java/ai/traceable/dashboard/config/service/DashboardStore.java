package ai.traceable.dashboard.config.service;

import ai.traceable.dashboard.config.service.v1.Dashboard;
import ai.traceable.dashboard.config.service.v1.GetDashboardsRequest;
import com.google.protobuf.Value;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class DashboardStore
    extends IdentifiedObjectStoreWithFilter<Dashboard, GetDashboardsRequest.DashboardFilter> {

  private static final String DASHBOARD_RESOURCE_NAME = "dashboard";
  private static final String DASHBOARD_NAMESPACE = "dashboards";
  private static final String guidRegexPattern =
      "^[{]?[0-9a-fA-F]{8}" + "-([0-9a-fA-F]{4}-)" + "{3}[0-9a-fA-F]{12}[}]?$";

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
  public Optional<ContextualConfigObject<Dashboard>> getObject(
      RequestContext requestContext, String id) {
    return super.getObject(requestContext, id)
        .filter(dashboard -> isSystemOrUserDashboard(requestContext, dashboard.getData()));
  }

  @Override
  public List<ContextualConfigObject<Dashboard>> getAllObjects(
      RequestContext requestContext, GetDashboardsRequest.DashboardFilter filter) {
    return super.getAllObjects(requestContext, filter).stream()
        .filter(dashboard -> isSystemOrUserDashboard(requestContext, dashboard.getData()))
        .collect(Collectors.toUnmodifiableList());
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

  private boolean isSystemOrUserDashboard(RequestContext requestContext, Dashboard dashboard) {
    return dashboard.hasUiReference()
            && !Pattern.matches(guidRegexPattern, dashboard.getUiReference())
        || requestContext
            .getEmail()
            .map(email -> email.equals(dashboard.getAuthor()))
            .orElse(false);
  }
}
