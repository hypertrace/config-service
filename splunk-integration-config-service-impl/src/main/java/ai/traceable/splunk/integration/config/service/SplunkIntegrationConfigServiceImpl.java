package ai.traceable.splunk.integration.config.service;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.splunk.integration.config.service.api.v1.*;
import ai.traceable.splunk.integration.config.service.api.v1.SplunkIntegrationConfigServiceGrpc.SplunkIntegrationConfigServiceImplBase;
import ai.traceable.splunk.integration.config.service.store.SplunkIntegrationConfigStore;
import ai.traceable.splunk.integration.config.service.validation.SplunkIntegrationConfigRequestValidator;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = {@Inject})
public class SplunkIntegrationConfigServiceImpl extends SplunkIntegrationConfigServiceImplBase {

  public static final String SPLUNK_INTEGRATION_CONFIG_SERVICE_CONFIG =
      "splunk.integration.config.service";

  private final SplunkIntegrationConfigRequestValidator requestValidator;
  private final SplunkIntegrationConfigStore splunkIntegrationConfigStore;

  private final UuidGenerator uuidGenerator;
  private final Config config;

  @Override
  public void createSplunkIntegration(
      ai.traceable.splunk.integration.config.service.api.v1.CreateSplunkIntegrationRequest request,
      io.grpc.stub.StreamObserver<
              ai.traceable.splunk.integration.config.service.api.v1.CreateSplunkIntegrationResponse>
          responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(
          requestContext, request, config.getConfig(SPLUNK_INTEGRATION_CONFIG_SERVICE_CONFIG));
      final SplunkIntegration splunkIntegration =
          SplunkIntegration.newBuilder()
              .setId(uuidGenerator.generateRandomId())
              .setDetails(
                  SplunkIntegrationDetails.newBuilder()
                      .setName(request.getName())
                      .setDescription(request.getDescription())
                      .setApiToken(request.getApiToken())
                      .setHttpEventCollectorUrl(request.getHttpEventCollectorUrl())
                      .build())
              .build();
      SplunkIntegration created =
          splunkIntegrationConfigStore.upsertObject(requestContext, splunkIntegration).getData();

      responseObserver.onNext(
          CreateSplunkIntegrationResponse.newBuilder().setIntegration(created).build());
      responseObserver.onCompleted();
    } catch (Throwable throwable) {
      log.warn(
          "Could not create splunk integration in context:{} for request:{}",
          requestContext,
          request,
          throwable);
      responseObserver.onError(throwable);
    }
  }

  @Override
  public void getSplunkIntegrations(
      ai.traceable.splunk.integration.config.service.api.v1.GetSplunkIntegrationsRequest request,
      io.grpc.stub.StreamObserver<
              ai.traceable.splunk.integration.config.service.api.v1.GetSplunkIntegrationsResponse>
          responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(requestContext, request);

      List<SplunkIntegration> integrations =
          request.hasFilter()
              ? splunkIntegrationConfigStore.getAllConfigData(requestContext, request.getFilter())
              : splunkIntegrationConfigStore.getAllConfigData(requestContext);
      responseObserver.onNext(
          GetSplunkIntegrationsResponse.newBuilder().addAllIntegrations(integrations).build());
      responseObserver.onCompleted();
    } catch (Throwable throwable) {
      log.warn(
          "Could not get splunk integration in context:{} for request:{}",
          requestContext,
          request,
          throwable);
      responseObserver.onError(throwable);
    }
  }

  @Override
  public void updateSplunkIntegration(
      ai.traceable.splunk.integration.config.service.api.v1.UpdateSplunkIntegrationRequest request,
      io.grpc.stub.StreamObserver<
              ai.traceable.splunk.integration.config.service.api.v1.UpdateSplunkIntegrationResponse>
          responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(
          requestContext, request, config.getConfig(SPLUNK_INTEGRATION_CONFIG_SERVICE_CONFIG));
      SplunkIntegration.Builder builder = SplunkIntegration.newBuilder();
      builder.setId(request.getId());
      SplunkIntegrationDetails.Builder detailsBuilder = SplunkIntegrationDetails.newBuilder();

      detailsBuilder.setName(request.getName());
      detailsBuilder.setDescription(request.getDescription());
      detailsBuilder.setHttpEventCollectorUrl(request.getHttpEventCollectorUrl());

      EncryptedText apiToken =
          request.hasApiToken()
              ? request.getApiToken()
              : splunkIntegrationConfigStore
                  .getData(requestContext, request.getId())
                  .map(SplunkIntegration::getDetails)
                  .map(SplunkIntegrationDetails::getApiToken)
                  .orElseThrow();
      detailsBuilder.setApiToken(apiToken);

      final SplunkIntegration splunkIntegration =
          builder.setDetails(detailsBuilder.build()).build();
      SplunkIntegration upserted =
          splunkIntegrationConfigStore.upsertObject(requestContext, splunkIntegration).getData();

      responseObserver.onNext(
          UpdateSplunkIntegrationResponse.newBuilder().setIntegration(upserted).build());
      responseObserver.onCompleted();
    } catch (Throwable throwable) {
      log.warn(
          "Could not update splunk integration in context:{} for request:{}",
          requestContext,
          request,
          throwable);
      responseObserver.onError(throwable);
    }
  }

  @Override
  public void deleteSplunkIntegration(
      ai.traceable.splunk.integration.config.service.api.v1.DeleteSplunkIntegrationRequest request,
      io.grpc.stub.StreamObserver<
              ai.traceable.splunk.integration.config.service.api.v1.DeleteSplunkIntegrationResponse>
          responseObserver) {

    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(requestContext, request);
      splunkIntegrationConfigStore.deleteObject(requestContext, request.getId());
      responseObserver.onNext(DeleteSplunkIntegrationResponse.newBuilder().build());
      responseObserver.onCompleted();
    } catch (Throwable throwable) {
      log.warn(
          "Could not delete splunk integration in context:{} for request:{}",
          requestContext,
          request,
          throwable);
      responseObserver.onError(throwable);
    }
  }
}
