package ai.traceable.reporting.config.service.v2;

import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class ReportingConfigServiceImpl
    extends ReportingConfigServiceGrpc.ReportingConfigServiceImplBase {

  private final ReportingConfigRequestValidator reportingConfigValidator;
  private final ReportingConfigManager reportingConfigManager;

  @Inject
  public ReportingConfigServiceImpl(
      ReportingConfigRequestValidator validator, ReportingConfigManager reportingConfigManager) {
    this.reportingConfigValidator = validator;
    this.reportingConfigManager = reportingConfigManager;
  }

  @Override
  public void createReportConfiguration(
      CreateReportConfigurationRequest request,
      StreamObserver<CreateReportConfigurationResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      reportingConfigValidator.validateCreateReportConfigurationRequest(requestContext, request);
      CreateReportConfigurationResponse response =
          CreateReportConfigurationResponse.newBuilder()
              .setReportConfiguration(
                  reportingConfigManager.createReportConfiguration(
                      requestContext, request.getCommonConfigurationDetails()))
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.warn("Failed to create report configuration for request: {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateReportConfiguration(
      UpdateReportConfigurationRequest request,
      StreamObserver<UpdateReportConfigurationResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      reportingConfigValidator.validateUpdateReportConfigurationRequest(requestContext, request);

      UpdateReportConfigurationResponse response =
          UpdateReportConfigurationResponse.newBuilder()
              .setReportConfiguration(
                  reportingConfigManager.updateReportConfiguration(
                      requestContext, request.getId(), request.getCommonConfigurationDetails()))
              .build();

      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.warn("Failed to update report configuration for request: {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getReportConfigurations(
      GetReportConfigurationsRequest request,
      StreamObserver<GetReportConfigurationsResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      reportingConfigValidator.validateGetReportConfigurationsRequest(requestContext, request);

      GetReportConfigurationsResponse response =
          GetReportConfigurationsResponse.newBuilder()
              .addAllReportConfigurations(
                  reportingConfigManager.getReportConfigurations(
                      requestContext, request.getGetReportsFilter()))
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.warn("Failed to get report configurations for request: {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void deleteReportConfiguration(
      DeleteReportConfigurationRequest request,
      StreamObserver<DeleteReportConfigurationResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      reportingConfigValidator.validateDeleteReportConfigurationRequest(requestContext, request);

      reportingConfigManager.deleteReportConfiguration(requestContext, request.getId());
      responseObserver.onNext(DeleteReportConfigurationResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.warn("Failed to delete report configuration for request: {}", request, e);
      responseObserver.onError(e);
    }
  }
}
