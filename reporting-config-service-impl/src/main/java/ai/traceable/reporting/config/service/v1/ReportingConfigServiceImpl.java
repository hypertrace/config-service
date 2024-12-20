package ai.traceable.reporting.config.service.v1;

import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ConfigObject;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class ReportingConfigServiceImpl
    extends ReportingConfigServiceGrpc.ReportingConfigServiceImplBase {

  private final ReportingConfigRequestValidator validator;
  private final ReportingConfigStore reportingConfigStore;

  @Inject
  public ReportingConfigServiceImpl(
      Channel channel, ConfigChangeEventGenerator configChangeEventGenerator) {
    this.validator = new ReportingConfigRequestValidator();
    this.reportingConfigStore = new ReportingConfigStore(channel, configChangeEventGenerator);
  }

  @Override
  public void createReportConfiguration(
      CreateReportConfigurationRequest request,
      StreamObserver<CreateReportConfigurationResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      validator.validateCreateReportConfigurationRequest(requestContext, request);
      ReportConfiguration.Builder builder =
          ReportConfiguration.newBuilder()
              .setId(UUID.randomUUID().toString())
              .setReportConfigurationDetails(request.getReportConfigurationDetails());

      responseObserver.onNext(
          CreateReportConfigurationResponse.newBuilder()
              .setReportConfiguration(
                  reportingConfigStore.upsertObject(requestContext, builder.build()).getData())
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Create Report Configuration RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateReportConfiguration(
      UpdateReportConfigurationRequest request,
      StreamObserver<UpdateReportConfigurationResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      validator.validateUpdateReportConfigurationRequest(requestContext, request);
      ReportConfiguration reportConfiguration =
          reportingConfigStore
              .getData(requestContext, request.getId())
              .orElseThrow(
                  () ->
                      new UnsupportedOperationException(
                          "Unable to Update Report config as config not present"));
      responseObserver.onNext(
          UpdateReportConfigurationResponse.newBuilder()
              .setReportConfiguration(
                  reportingConfigStore
                      .upsertObject(
                          requestContext,
                          ReportConfiguration.newBuilder()
                              .setId(request.getId())
                              .setReportConfigurationDetails(
                                  request.getReportConfigurationDetails())
                              .setLastExecutedTimestampMillis(
                                  reportConfiguration.getLastExecutedTimestampMillis())
                              .build())
                      .getData())
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Update Report Configuration RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateReportExecutionTime(
      UpdateReportExecutionTimeRequest request,
      StreamObserver<UpdateReportExecutionTimeResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      validator.validateUpdateReportExecutionTimeRequest(requestContext, request);
      ReportConfiguration reportConfiguration =
          reportingConfigStore
              .getData(requestContext, request.getId())
              .orElseThrow(
                  () ->
                      new UnsupportedOperationException(
                          "Unable to update ReportExecutionTime as config not present"));
      responseObserver.onNext(
          UpdateReportExecutionTimeResponse.newBuilder()
              .setReportConfiguration(
                  reportingConfigStore
                      .upsertObject(
                          requestContext,
                          ReportConfiguration.newBuilder()
                              .setId(request.getId())
                              .setReportConfigurationDetails(
                                  reportConfiguration.getReportConfigurationDetails())
                              .setLastExecutedTimestampMillis(
                                  request.getLastExecutedTimestampMillis())
                              .build())
                      .getData())
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Update Report Configuration RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getAllReportConfigurations(
      GetAllReportConfigurationsRequest request,
      StreamObserver<GetAllReportConfigurationsResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      validator.validateGetAllReportConfigurationsRequest(requestContext, request);
      responseObserver.onNext(
          GetAllReportConfigurationsResponse.newBuilder()
              .addAllReportConfigurations(
                  reportingConfigStore.getAllObjects(requestContext).stream()
                      .map(ConfigObject::getData)
                      .collect(Collectors.toUnmodifiableList()))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Get All Report Configurations RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void deleteReportConfiguration(
      DeleteReportConfigurationRequest request,
      StreamObserver<DeleteReportConfigurationResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      validator.validateDeleteReportConfigurationRequest(requestContext, request);
      reportingConfigStore
          .deleteObject(requestContext, request.getId())
          .orElseThrow(Status.NOT_FOUND::asRuntimeException);
      responseObserver.onNext(DeleteReportConfigurationResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Delete Report Configuration RPC failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }
}
