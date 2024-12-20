package ai.traceable.dashboard.config.service;

import ai.traceable.config.utils.TimestampConverter;
import ai.traceable.config.utils.UuidGenerator;
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
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import java.time.Clock;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = @Inject)
class DashboardConfigServiceImpl extends DashboardConfigServiceGrpc.DashboardConfigServiceImplBase {
  private final DashboardConfigServiceValidator validator;
  private final DashboardStore dashboardStore;
  private final UuidGenerator uuidGenerator;
  private final TimestampConverter timestampConverter;
  private final Clock clock;

  @Override
  public void getDashboards(
      GetDashboardsRequest request, StreamObserver<GetDashboardsResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      this.validator.validate(requestContext);
      responseObserver.onNext(
          GetDashboardsResponse.newBuilder()
              .addAllDashboards(
                  this.dashboardStore.getAllConfigData(requestContext, request.getFilter()))
              .build());
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

      responseObserver.onNext(
          CreateDashboardResponse.newBuilder()
              .setDashboard(
                  this.dashboardStore
                      .upsertObject(requestContext, dashboardBuilder.build())
                      .getData())
              .build());
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
      Dashboard existingDashboard =
          this.dashboardStore
              .getData(requestContext, request.getId())
              .orElseThrow(Status.NOT_FOUND::asRuntimeException);

      Dashboard.Builder dashboardBuilder =
          existingDashboard.toBuilder()
              .setName(request.getName())
              .setJson(request.getJson())
              .setLastUpdatedTimestamp(timestampConverter.convert(clock.instant()));

      if (request.hasDescription()) {
        dashboardBuilder.setDescription(request.getDescription());
      }

      responseObserver.onNext(
          UpdateDashboardResponse.newBuilder()
              .setDashboard(
                  this.dashboardStore.updateDashboard(
                      requestContext, request, dashboardBuilder.build()))
              .build());
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
      this.dashboardStore
          .getData(requestContext, request.getId())
          .orElseThrow(Status.NOT_FOUND::asRuntimeException);

      this.dashboardStore.deleteObject(requestContext, request.getId());
      responseObserver.onNext(DeleteDashboardResponse.getDefaultInstance());
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

  private Exception decorateException(RequestContext requestContext, Exception exception) {
    return Status.fromThrowable(exception)
        .withCause(exception)
        .asRuntimeException(requestContext.buildTrailers());
  }
}
