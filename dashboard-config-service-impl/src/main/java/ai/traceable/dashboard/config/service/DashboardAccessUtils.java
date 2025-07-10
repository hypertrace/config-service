package ai.traceable.dashboard.config.service;

import ai.traceable.dashboard.config.service.v1.Dashboard;
import ai.traceable.dashboard.config.service.v1.DashboardPrincipal;
import ai.traceable.dashboard.config.service.v1.DashboardRoleAssignments;
import com.google.inject.Inject;
import io.grpc.Status;
import java.util.List;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.ContextualStatusExceptionBuilder;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class DashboardAccessUtils {

  public boolean hasRole(String userEmail, Dashboard dashboard, DashboardRole role) {

    validateUserEmail(userEmail);
    validateDashboardRoleAssignments(dashboard);

    if (isSystemDashboard(dashboard)) {
      return true;
    }
    DashboardRoleAssignments roleAssignments = dashboard.getRoleAssignments();
    return checkRoleAccess(userEmail, roleAssignments, role);
  }

  public boolean hasUpdateAccess(String userEmail, Dashboard dashboard) {
    return hasRole(userEmail, dashboard, DashboardRole.EDITOR);
  }

  public boolean hasRoleUpdateAccess(String userEmail, Dashboard dashboard) {
    return hasRole(userEmail, dashboard, DashboardRole.OWNER) && !isSystemDashboard(dashboard);
  }

  boolean hasDeleteAccess(String userEmail, Dashboard dashboard) {
    return hasRole(userEmail, dashboard, DashboardRole.OWNER);
  }

  boolean hasReadAccess(String userEmail, Dashboard dashboard) {
    return hasRole(userEmail, dashboard, DashboardRole.VIEWER);
  }

  private boolean checkRoleAccess(
      String userEmail, DashboardRoleAssignments roleAssignments, DashboardRole role) {
    switch (role) {
      case VIEWER:
        return isInPrincipalsList(userEmail, roleAssignments.getViewersList())
            || checkRoleAccess(userEmail, roleAssignments, DashboardRole.EDITOR);
      case EDITOR:
        return isInPrincipalsList(userEmail, roleAssignments.getEditorsList())
            || checkRoleAccess(userEmail, roleAssignments, DashboardRole.OWNER);
      case OWNER:
        return isInPrincipalsList(userEmail, roleAssignments.getOwnersList());
      default:
        return false;
    }
  }

  private boolean isInPrincipalsList(String userEmail, List<DashboardPrincipal> principals) {
    return principals.stream()
        .anyMatch(
            principal ->
                (principal.hasUserEmail() && userEmail.equals(principal.getUserEmail()))
                    || principal.hasAllAuthenticatedUsers());
  }

  private void validateUserEmail(String userEmail) {
    if (userEmail == null) {
      throw ContextualStatusExceptionBuilder.from(
              Status.INTERNAL.withDescription(
                  "User email cannot be null when checking dashboard access"))
          .buildRuntimeException();
    }
  }

  private boolean isSystemDashboard(Dashboard dashboard) {
    return !dashboard.getUiReference().isEmpty();
  }

  private void validateDashboardRoleAssignments(Dashboard dashboard) {
    if (!dashboard.hasRoleAssignments()) {
      throw ContextualStatusExceptionBuilder.from(
              Status.INTERNAL.withDescription(
                  "Dashboard is missing role assignments: " + dashboard.getId()))
          .buildRuntimeException();
    }
  }
}
