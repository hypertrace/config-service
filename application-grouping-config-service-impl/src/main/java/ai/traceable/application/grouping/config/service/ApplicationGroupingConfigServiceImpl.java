package ai.traceable.application.grouping.config.service;

import ai.traceable.application.grouping.config.service.manager.ApplicationGroupingRuleConfigService;
import ai.traceable.application.grouping.config.service.v1.ApplicationGroupingConfigServiceGrpc.ApplicationGroupingConfigServiceImplBase;
import ai.traceable.application.grouping.config.service.v1.CreateApplicationGroupingRuleConfigRequest;
import ai.traceable.application.grouping.config.service.v1.CreateApplicationGroupingRuleConfigResponse;
import ai.traceable.application.grouping.config.service.v1.DeleteApplicationGroupingRuleConfigsRequest;
import ai.traceable.application.grouping.config.service.v1.DeleteApplicationGroupingRuleConfigsResponse;
import ai.traceable.application.grouping.config.service.v1.GetAllApplicationGroupingRuleConfigsRequest;
import ai.traceable.application.grouping.config.service.v1.GetAllApplicationGroupingRuleConfigsResponse;
import ai.traceable.application.grouping.config.service.v1.UpdateApplicationGroupingRuleConfigRequest;
import ai.traceable.application.grouping.config.service.v1.UpdateApplicationGroupingRuleConfigResponse;
import ai.traceable.application.grouping.config.service.validations.ApplicationGroupingConfigServiceRequestValidator;
import com.google.inject.Inject;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class ApplicationGroupingConfigServiceImpl extends ApplicationGroupingConfigServiceImplBase {

  private final ApplicationGroupingConfigServiceRequestValidator requestValidator;
  private final ApplicationGroupingRuleConfigService applicationGroupingRuleConfigService;

  @Inject
  public ApplicationGroupingConfigServiceImpl(
      ApplicationGroupingConfigServiceRequestValidator requestValidator,
      ApplicationGroupingRuleConfigService applicationGroupingRuleConfigService) {
    this.requestValidator = requestValidator;
    this.applicationGroupingRuleConfigService = applicationGroupingRuleConfigService;
  }

  @Override
  public void getAllApplicationGroupingRuleConfigs(
      GetAllApplicationGroupingRuleConfigsRequest request,
      StreamObserver<GetAllApplicationGroupingRuleConfigsResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(requestContext, request);
      responseObserver.onNext(
          GetAllApplicationGroupingRuleConfigsResponse.newBuilder()
              .addAllApplicationGroupingRuleConfigs(
                  applicationGroupingRuleConfigService.getAllApplicationGroupingRuleConfigs(
                      requestContext, request))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Error occurred while fetching application grouping rule configs for request {} with context {}",
          request,
          requestContext,
          e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void createApplicationGroupingRuleConfig(
      CreateApplicationGroupingRuleConfigRequest request,
      StreamObserver<CreateApplicationGroupingRuleConfigResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(requestContext, request);
      responseObserver.onNext(
          CreateApplicationGroupingRuleConfigResponse.newBuilder()
              .setApplicationGroupingRuleConfig(
                  applicationGroupingRuleConfigService.createApplicationGroupingRuleConfig(
                      requestContext, request))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Error occurred while creating application grouping rule config for request {} with context {}",
          request,
          requestContext,
          e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateApplicationGroupingRuleConfig(
      UpdateApplicationGroupingRuleConfigRequest request,
      StreamObserver<UpdateApplicationGroupingRuleConfigResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(requestContext, request);
      responseObserver.onNext(
          UpdateApplicationGroupingRuleConfigResponse.newBuilder()
              .setApplicationGroupingRuleConfig(
                  applicationGroupingRuleConfigService.updateApplicationGroupingRuleConfig(
                      requestContext, request))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Error occurred while updating application grouping rule config for request {} with context {}",
          request,
          requestContext,
          e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void deleteApplicationGroupingRuleConfigs(
      DeleteApplicationGroupingRuleConfigsRequest request,
      StreamObserver<DeleteApplicationGroupingRuleConfigsResponse> responseStreamObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(requestContext, request);
      applicationGroupingRuleConfigService.deleteApplicationGroupingRuleConfigs(
          requestContext, request);
      responseStreamObserver.onNext(
          DeleteApplicationGroupingRuleConfigsResponse.getDefaultInstance());
      responseStreamObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Error occurred while deleting application grouping rule configs for request {} with context {}",
          request,
          requestContext,
          e);
      responseStreamObserver.onError(e);
    }
  }
}
