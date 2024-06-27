package ai.traceable.jira.integration.config.service;

import ai.traceable.jira.integration.config.service.api.v1.AddJiraTemplateRequest;
import ai.traceable.jira.integration.config.service.api.v1.AddJiraTemplateResponse;
import ai.traceable.jira.integration.config.service.api.v1.CreateJiraIntegrationRequest;
import ai.traceable.jira.integration.config.service.api.v1.CreateJiraIntegrationResponse;
import ai.traceable.jira.integration.config.service.api.v1.DeleteJiraIntegrationRequest;
import ai.traceable.jira.integration.config.service.api.v1.DeleteJiraIntegrationResponse;
import ai.traceable.jira.integration.config.service.api.v1.DeleteJiraTemplateRequest;
import ai.traceable.jira.integration.config.service.api.v1.DeleteJiraTemplateResponse;
import ai.traceable.jira.integration.config.service.api.v1.GetJiraIntegrationsRequest;
import ai.traceable.jira.integration.config.service.api.v1.GetJiraIntegrationsResponse;
import ai.traceable.jira.integration.config.service.api.v1.JiraIntegrationConfigServiceGrpc;
import ai.traceable.jira.integration.config.service.api.v1.UpdateJiraIntegrationRequest;
import ai.traceable.jira.integration.config.service.api.v1.UpdateJiraIntegrationResponse;
import ai.traceable.jira.integration.config.service.api.v1.UpdateJiraTemplateRequest;
import ai.traceable.jira.integration.config.service.api.v1.UpdateJiraTemplateResponse;
import com.google.inject.Inject;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class JiraIntegrationConfigServiceImpl
    extends JiraIntegrationConfigServiceGrpc.JiraIntegrationConfigServiceImplBase {
  private final JiraIntegrationConfigServiceValidator validator;
  private final JiraIntegrationCoordinator jiraIntegrationCoordinator;

  @Inject
  public JiraIntegrationConfigServiceImpl(
      JiraIntegrationConfigServiceValidator validator,
      JiraIntegrationCoordinator jiraIntegrationCoordinator) {
    this.validator = validator;
    this.jiraIntegrationCoordinator = jiraIntegrationCoordinator;
  }

  @Override
  public void createJiraIntegration(
      CreateJiraIntegrationRequest request,
      StreamObserver<CreateJiraIntegrationResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      validator.validateCreateJiraIntegration(request, requestContext);
      responseObserver.onNext(
          jiraIntegrationCoordinator.createJiraIntegration(request, requestContext));
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

  @Override
  public void getJiraIntegrations(
      GetJiraIntegrationsRequest request,
      StreamObserver<GetJiraIntegrationsResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      validator.validateGetJiraIntegrations(request, requestContext);
      responseObserver.onNext(
          GetJiraIntegrationsResponse.newBuilder()
              .addAllIntegration(
                  jiraIntegrationCoordinator.getJiraIntegration(requestContext, request))
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
  public void updateJiraIntegration(
      UpdateJiraIntegrationRequest request,
      StreamObserver<UpdateJiraIntegrationResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      validator.validateUpdateJiraIntegration(request, requestContext);
      responseObserver.onNext(
          UpdateJiraIntegrationResponse.newBuilder()
              .setIntegration(
                  jiraIntegrationCoordinator.updateJiraIntegration(request, requestContext))
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
  public void deleteJiraIntegration(
      DeleteJiraIntegrationRequest request,
      StreamObserver<DeleteJiraIntegrationResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      validator.validateDeleteJiraIntegration(request, requestContext);
      jiraIntegrationCoordinator.deleteIntegration(requestContext, request);
      responseObserver.onNext(DeleteJiraIntegrationResponse.newBuilder().build());
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

  @Override
  public void addJiraTemplate(
      AddJiraTemplateRequest request, StreamObserver<AddJiraTemplateResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      this.validator.validateAddJiraTemplate(request, requestContext);
      responseObserver.onNext(
          this.jiraIntegrationCoordinator.addJiraTemplate(requestContext, request));
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Failed during adding jira template for request: {} within context: {}",
          request,
          requestContext,
          e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateJiraTemplate(
      UpdateJiraTemplateRequest request,
      StreamObserver<UpdateJiraTemplateResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      validator.validateUpdateJiraTemplate(request, requestContext);
      responseObserver.onNext(
          this.jiraIntegrationCoordinator.updateJiraTemplate(requestContext, request));
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Failed during updating jira template for request: {} within context: {}",
          request,
          requestContext,
          e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void deleteJiraTemplate(
      DeleteJiraTemplateRequest request,
      StreamObserver<DeleteJiraTemplateResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      validator.validateDeleteJiraTemplate(request, requestContext);
      responseObserver.onNext(
          this.jiraIntegrationCoordinator.deleteJiraTemplate(requestContext, request));
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Failed during deleting jira template for request: {} within context: {}",
          request,
          requestContext,
          e);
      responseObserver.onError(e);
    }
  }
}
