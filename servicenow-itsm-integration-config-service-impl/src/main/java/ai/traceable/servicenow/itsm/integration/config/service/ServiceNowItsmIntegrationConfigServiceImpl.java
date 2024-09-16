package ai.traceable.servicenow.itsm.integration.config.service;

import ai.traceable.servicenow.itsm.integration.config.service.api.v1.CreateServiceNowItsmIntegrationRequest;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.CreateServiceNowItsmIntegrationResponse;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.DeleteServiceNowItsmIntegrationRequest;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.DeleteServiceNowItsmIntegrationResponse;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.GetServiceNowItsmIntegrationRequest;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.GetServiceNowItsmIntegrationResponse;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.GetServiceNowItsmIntegrationWithAuthCredentialsRequest;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.GetServiceNowItsmIntegrationWithAuthCredentialsResponse;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.GetServiceNowItsmIntegrationsWithAuthCredentialsRequest;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.GetServiceNowItsmIntegrationsWithAuthCredentialsResponse;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.ServiceNowItsmIntegrationConfigServiceGrpc;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.UpdateServiceNowItsmIntegrationRequest;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.UpdateServiceNowItsmIntegrationResponse;
import com.google.inject.Inject;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class ServiceNowItsmIntegrationConfigServiceImpl
    extends ServiceNowItsmIntegrationConfigServiceGrpc
        .ServiceNowItsmIntegrationConfigServiceImplBase {
  private final ServiceNowItsmIntegrationConfigServiceValidator validator;
  private final ServiceNowItsmIntegrationCoordinator coordinator;

  @Inject
  public ServiceNowItsmIntegrationConfigServiceImpl(
      ServiceNowItsmIntegrationConfigServiceValidator validator,
      ServiceNowItsmIntegrationCoordinator coordinator) {
    this.validator = validator;
    this.coordinator = coordinator;
  }

  @Override
  public void createServiceNowItsmIntegration(
      CreateServiceNowItsmIntegrationRequest request,
      StreamObserver<CreateServiceNowItsmIntegrationResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      validator.validateCreateServiceNowItsmIntegration(request, requestContext);
      responseObserver.onNext(coordinator.createServiceNowItsmIntegration(request, requestContext));
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Failed during creating integration for request: {} within context: {}",
          request,
          requestContext,
          e);
      responseObserver.onError(e);
    }
  }

  // GET Api for fetching all integrations without auth credentials for a given filter
  @Override
  public void getServiceNowItsmIntegration(
      GetServiceNowItsmIntegrationRequest request,
      StreamObserver<GetServiceNowItsmIntegrationResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      validator.validateGetServiceNowItsmIntegration(request, requestContext);
      responseObserver.onNext(
          GetServiceNowItsmIntegrationResponse.newBuilder()
              .addAllServiceNowItsmIntegration(
                  coordinator.getServiceNowItsmIntegration(request, requestContext))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Failed during fetching integrations for request: {} within context: {}",
          request,
          requestContext,
          e);
      responseObserver.onError(e);
    }
  }

  // GET Api for fetching integration with auth credential for a given integration id
  @Override
  public void getServiceNowItsmIntegrationWithAuthCredentials(
      GetServiceNowItsmIntegrationWithAuthCredentialsRequest request,
      StreamObserver<GetServiceNowItsmIntegrationWithAuthCredentialsResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      validator.validateGetServiceNowItsmIntegrationWithAuthCredentials(request, requestContext);
      responseObserver.onNext(
          GetServiceNowItsmIntegrationWithAuthCredentialsResponse.newBuilder()
              .setServiceNowItsmIntegrationWithAuthCredentials(
                  coordinator.getServiceNowItsmIntegrationWithAuthCredentials(
                      request, requestContext))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Failed during fetching integrations for request: {} within context: {}",
          request,
          requestContext,
          e);
      responseObserver.onError(e);
    }
  }

  // GET Api for fetching all integrations with auth credentials for a given filter
  @Override
  public void getServiceNowItsmIntegrationsWithAuthCredentials(
      GetServiceNowItsmIntegrationsWithAuthCredentialsRequest request,
      StreamObserver<GetServiceNowItsmIntegrationsWithAuthCredentialsResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      validator.validateGetServiceNowItsmIntegrationsWithAuthCredentials(request, requestContext);
      responseObserver.onNext(
          GetServiceNowItsmIntegrationsWithAuthCredentialsResponse.newBuilder()
              .addAllServiceNowItsmIntegrationWithAuthCredentials(
                  coordinator.getServiceNowItsmIntegrationsWithAuthCredentials(
                      request, requestContext))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Failed during fetching integrations for request: {} within context: {}",
          request,
          requestContext,
          e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateServiceNowItsmIntegration(
      UpdateServiceNowItsmIntegrationRequest request,
      StreamObserver<UpdateServiceNowItsmIntegrationResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      validator.validateUpdateServiceNowItsmIntegration(request, requestContext);
      responseObserver.onNext(
          UpdateServiceNowItsmIntegrationResponse.newBuilder()
              .setServiceNowItsmIntegration(
                  coordinator.updateServiceNowItsmIntegration(request, requestContext))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Failed during updating integration for request: {} within context: {}",
          request,
          requestContext,
          e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void deleteServiceNowItsmIntegration(
      DeleteServiceNowItsmIntegrationRequest request,
      StreamObserver<DeleteServiceNowItsmIntegrationResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      validator.validateDeleteServiceNowItsmIntegration(request, requestContext);
      coordinator.deleteServiceNowItsmIntegration(request, requestContext);
      responseObserver.onNext(DeleteServiceNowItsmIntegrationResponse.newBuilder().build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Failed during deleting integration for request: {} within context: {}",
          request,
          requestContext,
          e);
      responseObserver.onError(e);
    }
  }
}
