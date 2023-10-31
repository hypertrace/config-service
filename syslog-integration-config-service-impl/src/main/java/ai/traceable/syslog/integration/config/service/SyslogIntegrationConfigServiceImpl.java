package ai.traceable.syslog.integration.config.service;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.syslog.integration.config.service.api.v1.CreateSyslogServerIntegrationRequest;
import ai.traceable.syslog.integration.config.service.api.v1.CreateSyslogServerIntegrationResponse;
import ai.traceable.syslog.integration.config.service.api.v1.DeleteSyslogServerIntegrationRequest;
import ai.traceable.syslog.integration.config.service.api.v1.DeleteSyslogServerIntegrationResponse;
import ai.traceable.syslog.integration.config.service.api.v1.GetSyslogServerIntegrationsRequest;
import ai.traceable.syslog.integration.config.service.api.v1.GetSyslogServerIntegrationsResponse;
import ai.traceable.syslog.integration.config.service.api.v1.SyslogIntegrationConfigServiceGrpc;
import ai.traceable.syslog.integration.config.service.api.v1.SyslogServerConnectionDetails;
import ai.traceable.syslog.integration.config.service.api.v1.SyslogServerIntegration;
import ai.traceable.syslog.integration.config.service.api.v1.SyslogServerIntegrationDetails;
import ai.traceable.syslog.integration.config.service.api.v1.UpdateSyslogServerIntegrationRequest;
import ai.traceable.syslog.integration.config.service.api.v1.UpdateSyslogServerIntegrationResponse;
import ai.traceable.syslog.integration.config.service.store.SyslogIntegrationConfigStore;
import ai.traceable.syslog.integration.config.service.validator.SyslogIntegrationConfigRequestValidator;
import com.google.inject.Inject;
import io.grpc.stub.StreamObserver;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = {@Inject})
public class SyslogIntegrationConfigServiceImpl
    extends SyslogIntegrationConfigServiceGrpc.SyslogIntegrationConfigServiceImplBase {

  private final SyslogIntegrationConfigRequestValidator requestValidator;
  private final SyslogIntegrationConfigStore syslogIntegrationConfigStore;
  private final UuidGenerator uuidGenerator;

  @Override
  public void createSyslogServerIntegration(
      CreateSyslogServerIntegrationRequest request,
      StreamObserver<CreateSyslogServerIntegrationResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(requestContext, request);
      final SyslogServerIntegration syslogIntegration =
          SyslogServerIntegration.newBuilder()
              .setId(uuidGenerator.generateRandomId())
              .setDetails(request.getIntegrationDetails())
              .build();

      SyslogServerIntegration createdSyslogServerIntegration =
          syslogIntegrationConfigStore.upsertObject(requestContext, syslogIntegration).getData();

      responseObserver.onNext(
          CreateSyslogServerIntegrationResponse.newBuilder()
              .setIntegration(createdSyslogServerIntegration)
              .build());
      responseObserver.onCompleted();
    } catch (Throwable throwable) {
      log.warn(
          "Could not create syslog integration in context:{} for request:{}",
          requestContext,
          request,
          throwable);
      responseObserver.onError(throwable);
    }
  }

  @Override
  public void getSyslogServerIntegrations(
      GetSyslogServerIntegrationsRequest request,
      StreamObserver<GetSyslogServerIntegrationsResponse> responseObserver) {

    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(requestContext, request);

      List<SyslogServerIntegration> integrations =
          request.hasFilter()
              ? syslogIntegrationConfigStore.getAllConfigData(requestContext, request.getFilter())
              : syslogIntegrationConfigStore.getAllConfigData(requestContext);
      responseObserver.onNext(
          GetSyslogServerIntegrationsResponse.newBuilder()
              .addAllIntegrations(integrations)
              .build());
      responseObserver.onCompleted();
    } catch (Throwable throwable) {
      log.warn(
          "Could not get syslog integration in context:{} for request:{}",
          requestContext,
          request,
          throwable);
      responseObserver.onError(throwable);
    }
  }

  @Override
  public void updateSyslogServerIntegration(
      UpdateSyslogServerIntegrationRequest request,
      StreamObserver<UpdateSyslogServerIntegrationResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(requestContext, request);
      SyslogServerIntegration integration = request.getIntegration();

      SyslogServerIntegration.Builder builder = SyslogServerIntegration.newBuilder();
      builder.setId(integration.getId());
      SyslogServerIntegrationDetails.Builder detailsBuilder =
          SyslogServerIntegrationDetails.newBuilder();
      SyslogServerIntegrationDetails details = integration.getDetails();

      detailsBuilder.setName(details.getName());
      detailsBuilder.setDescription(details.getDescription());
      detailsBuilder.setLogFormat(details.getLogFormat());

      SyslogServerConnectionDetails connectionDetails = details.getServerConnectionDetails();
      if (!connectionDetails.hasCredentials()) {
        SyslogServerConnectionDetails existingConnectionDetails =
            syslogIntegrationConfigStore
                .getData(requestContext, integration.getId())
                .map(SyslogServerIntegration::getDetails)
                .map(SyslogServerIntegrationDetails::getServerConnectionDetails)
                .orElseThrow();
        if (existingConnectionDetails.hasCredentials()) {
          connectionDetails =
              connectionDetails.toBuilder()
                  .setCredentials(existingConnectionDetails.getCredentials())
                  .build();
        }
      }
      detailsBuilder.setServerConnectionDetails(connectionDetails);

      final SyslogServerIntegration updatedSyslogServerIntegration =
          builder.setDetails(detailsBuilder).build();
      SyslogServerIntegration upserted =
          syslogIntegrationConfigStore
              .upsertObject(requestContext, updatedSyslogServerIntegration)
              .getData();

      responseObserver.onNext(
          UpdateSyslogServerIntegrationResponse.newBuilder().setIntegration(upserted).build());
      responseObserver.onCompleted();
    } catch (Throwable throwable) {
      log.warn(
          "Could not update syslog integration in context:{} for request:{}",
          requestContext,
          request,
          throwable);
      responseObserver.onError(throwable);
    }
  }

  @Override
  public void deleteSyslogServerIntegration(
      DeleteSyslogServerIntegrationRequest request,
      StreamObserver<DeleteSyslogServerIntegrationResponse> responseObserver) {

    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(requestContext, request);
      syslogIntegrationConfigStore.deleteObject(requestContext, request.getId());
      responseObserver.onNext(DeleteSyslogServerIntegrationResponse.newBuilder().build());
      responseObserver.onCompleted();
    } catch (Throwable throwable) {
      log.warn(
          "Could not delete syslog integration in context:{} for request:{}",
          requestContext,
          request,
          throwable);
      responseObserver.onError(throwable);
    }
  }
}
