package ai.traceable.dashboard.config.service;

import ai.traceable.dashboard.config.service.v1.CreateDashboardRequest;
import ai.traceable.dashboard.config.service.v1.CreateDashboardResponse;
import ai.traceable.dashboard.config.service.v1.Dashboard;
import ai.traceable.dashboard.config.service.v1.DashboardConfigServiceGrpc;
import ai.traceable.dashboard.config.service.v1.DeleteDashboardRequest;
import ai.traceable.dashboard.config.service.v1.DeleteDashboardResponse;
import ai.traceable.dashboard.config.service.v1.GetDashboardsRequest;
import ai.traceable.dashboard.config.service.v1.GetDashboardsResponse;
import ai.traceable.dashboard.config.service.v1.UpdateDashboardRequest;
import ai.traceable.dashboard.config.service.v1.UpdateDashboardResponse;
import ai.traceable.dashboard.config.service.v1.UpdateDashboardRoleAssignmentsRequest;
import ai.traceable.dashboard.config.service.v1.UpdateDashboardRoleAssignmentsResponse;
import ai.traceable.dashboard.config.service.validation.DashboardConfigServiceValidator;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = @Inject)
class DashboardConfigServiceImpl extends DashboardConfigServiceGrpc.DashboardConfigServiceImplBase {
  private final DashboardConfigServiceValidator validator;
  private final DashboardManager dashboardManager;

  @Override
  public void getDashboards(
      GetDashboardsRequest request, StreamObserver<GetDashboardsResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      this.validator.validate(requestContext);
      GetDashboardsResponse response = this.dashboardManager.getDashboards(requestContext, request);
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception exception) {
      Exception decoratedException = decorateException(requestContext, exception);
      log.warn(
          "Failed to get dashboards for request: {} with context: {}",
          request,
          requestContext,
          decoratedException);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void createDashboard(
      CreateDashboardRequest request, StreamObserver<CreateDashboardResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      this.validator.validate(requestContext, request);
      Dashboard createdDashboard = dashboardManager.createDashboard(requestContext, request);
      responseObserver.onNext(
          CreateDashboardResponse.newBuilder().setDashboard(createdDashboard).build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      Exception decoratedException = decorateException(requestContext, exception);
      log.warn(
          "Failed to create dashboard for request: {} with context: {}",
          request,
          requestContext,
          decoratedException);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void updateDashboard(
      UpdateDashboardRequest request, StreamObserver<UpdateDashboardResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      this.validator.validate(requestContext, request);
      Dashboard updatedDashboard = dashboardManager.updateDashboard(requestContext, request);
      responseObserver.onNext(
          UpdateDashboardResponse.newBuilder().setDashboard(updatedDashboard).build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      Exception decoratedException = decorateException(requestContext, exception);
      log.warn(
          "Failed to update dashboard for request: {} with context: {}",
          request,
          requestContext,
          decoratedException);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void deleteDashboard(
      DeleteDashboardRequest request, StreamObserver<DeleteDashboardResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      this.validator.validate(requestContext, request);
      DeleteDashboardResponse response =
          this.dashboardManager.deleteDashboard(requestContext, request);
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception exception) {
      Exception decoratedException = decorateException(requestContext, exception);
      log.warn(
          "Failed to delete dashboard for request: {} with context: {}",
          request,
          requestContext,
          decoratedException);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void updateDashboardRoleAssignments(
      UpdateDashboardRoleAssignmentsRequest request,
      StreamObserver<UpdateDashboardRoleAssignmentsResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      this.validator.validate(requestContext, request);
      Dashboard updatedDashboard =
          dashboardManager.updateDashboardRoleAssignments(requestContext, request);
      responseObserver.onNext(
          UpdateDashboardRoleAssignmentsResponse.newBuilder()
              .setDashboard(updatedDashboard)
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.warn(
          "Failed to update dashboard for request: {} with context: {}",
          request,
          requestContext,
          exception);
      responseObserver.onError(exception);
    }
  }

  private Exception decorateException(RequestContext requestContext, Exception exception) {
    return Status.fromThrowable(exception)
        .withCause(exception)
        .asRuntimeException(requestContext.buildTrailers());
  }
}
