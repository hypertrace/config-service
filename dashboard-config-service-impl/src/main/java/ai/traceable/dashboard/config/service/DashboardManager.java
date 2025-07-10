package ai.traceable.dashboard.config.service;

import ai.traceable.config.utils.TimestampConverter;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.dashboard.config.service.v1.CreateDashboardRequest;
import ai.traceable.dashboard.config.service.v1.Dashboard;
import ai.traceable.dashboard.config.service.v1.DashboardPrincipal;
import ai.traceable.dashboard.config.service.v1.DashboardRoleAssignments;
import ai.traceable.dashboard.config.service.v1.DeleteDashboardRequest;
import ai.traceable.dashboard.config.service.v1.DeleteDashboardResponse;
import ai.traceable.dashboard.config.service.v1.GetDashboardsRequest;
import ai.traceable.dashboard.config.service.v1.GetDashboardsResponse;
import ai.traceable.dashboard.config.service.v1.UpdateDashboardRequest;
import ai.traceable.dashboard.config.service.v1.UpdateDashboardRoleAssignmentsRequest;
import com.google.common.collect.ImmutableList;
import io.grpc.Status;
import jakarta.inject.Inject;
import java.time.Clock;
import java.util.List;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = @Inject)
public class DashboardManager {

  private final DashboardConfigServiceValidator validator;
  private final DashboardStore dashboardStore;
  private final DashboardAccessUtils dashboardAccessUtils;
  private final UuidGenerator uuidGenerator;
  private final TimestampConverter timestampConverter;
  private final Clock clock;

  public GetDashboardsResponse getDashboards(
      RequestContext requestContext, GetDashboardsRequest request) {

    List<Dashboard> dashboards =
        this.dashboardStore.getAllConfigData(requestContext, request.getFilter());

    List<Dashboard> dashboardsWithAccessibility = ensureRoleAssignments(dashboards);

    String userEmail = requestContext.getEmail().orElseThrow();
    List<Dashboard> accessibleDashboards =
        dashboardsWithAccessibility.stream()
            .filter(dashboard -> this.dashboardAccessUtils.hasReadAccess(userEmail, dashboard))
            .collect(Collectors.toList());

    return GetDashboardsResponse.newBuilder().addAllDashboards(accessibleDashboards).build();
  }

  public Dashboard createDashboard(RequestContext requestContext, CreateDashboardRequest request) {
    Dashboard.Builder dashboardBuilder =
        Dashboard.newBuilder()
            .setId(uuidGenerator.generateRandomId())
            .setName(request.getName())
            .setAuthor(
                requestContext
                    .getEmail()
                    .orElseThrow(
                        Status.INVALID_ARGUMENT.withDescription(
                                "E-mail not found in the request context, this is unexpected")
                            ::asRuntimeException))
            .setJson(request.getJson())
            .setCreationTimestamp(timestampConverter.convert(clock.instant()))
            .setLastUpdatedTimestamp(timestampConverter.convert(clock.instant()));

    if (request.hasDescription()) {
      dashboardBuilder.setDescription(request.getDescription());
    }

    if (request.hasUiReference()) {
      dashboardBuilder.setUiReference(request.getUiReference());
    }
    Dashboard dashboard = dashboardBuilder.build();
    Dashboard dashboardWithPermissions = assignDefaultSharingPermissions(dashboard);

    return dashboardStore.upsertObject(requestContext, dashboardWithPermissions).getData();
  }

  public Dashboard updateDashboard(RequestContext requestContext, UpdateDashboardRequest request) {
    Dashboard existingDashboard = getDashboard(requestContext, request.getId());

    this.validator.validateUpdateAccess(requestContext, existingDashboard);

    Dashboard.Builder dashboardBuilder =
        existingDashboard.toBuilder()
            .setName(request.getName())
            .setJson(request.getJson())
            .setLastUpdatedTimestamp(timestampConverter.convert(clock.instant()));
    if (request.hasDescription()) {
      dashboardBuilder.setDescription(request.getDescription());
    }
    Dashboard updatedDashboard = dashboardBuilder.build();

    return dashboardStore.updateDashboard(requestContext, request, updatedDashboard);
  }

  public DeleteDashboardResponse deleteDashboard(
      RequestContext requestContext, DeleteDashboardRequest request) {

    Dashboard existingDashboard = getDashboard(requestContext, request.getId());
    this.validator.validateDeleteAccess(requestContext, existingDashboard);
    this.dashboardStore.deleteObject(requestContext, request.getId());
    return DeleteDashboardResponse.getDefaultInstance();
  }

  public Dashboard updateDashboardRoleAssignments(
      RequestContext requestContext, UpdateDashboardRoleAssignmentsRequest request) {
    Dashboard existingDashboard = getDashboard(requestContext, request.getId());

    this.validator.validateRoleUpdateAccess(requestContext, existingDashboard);

    Dashboard.Builder dashboardBuilder =
        existingDashboard.toBuilder()
            .setLastUpdatedTimestamp(timestampConverter.convert(clock.instant()));
    dashboardBuilder.setRoleAssignments(request.getRoleAssignments());
    Dashboard updatedDashboard = dashboardBuilder.build();
    return this.dashboardStore.updateDashboardRolesAssignments(requestContext, updatedDashboard);
  }

  private Dashboard getDashboard(RequestContext requestContext, String dashboardId) {
    Dashboard dashboard =
        this.dashboardStore
            .getData(requestContext, dashboardId)
            .orElseThrow(Status.NOT_FOUND::asRuntimeException);

    return assignDefaultSharingPermissions(dashboard);
  }

  private List<Dashboard> ensureRoleAssignments(List<Dashboard> dashboards) {
    return dashboards.stream()
        .map(this::assignDefaultSharingPermissions)
        .collect(ImmutableList.toImmutableList());
  }

  private Dashboard assignDefaultSharingPermissions(Dashboard dashboard) {
    if (!dashboard.getRoleAssignments().getOwnersList().isEmpty()) {
      return dashboard;
    }
    DashboardRoleAssignments.Builder roleAssignmentsBuilder =
        dashboard.getRoleAssignments().toBuilder();
    roleAssignmentsBuilder.addOwners(
        DashboardPrincipal.newBuilder().setUserEmail(dashboard.getAuthor()));
    return dashboard.toBuilder().setRoleAssignments(roleAssignmentsBuilder).build();
  }

  private void sendInvitation(
      RequestContext requestContext,
      Dashboard dashboard,
      UpdateDashboardRoleAssignmentsRequest updateRequest) {
    // TODO: in separate PR
  }
}
