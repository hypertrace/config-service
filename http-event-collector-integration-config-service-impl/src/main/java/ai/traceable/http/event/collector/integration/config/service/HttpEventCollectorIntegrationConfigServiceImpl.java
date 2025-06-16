package ai.traceable.http.event.collector.integration.config.service;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.http.event.collector.integration.config.service.store.HttpEventCollectorIntegrationConfigServiceStore;
import ai.traceable.http.event.collector.integration.config.service.v1.CreateHttpEventCollectorIntegrationRequest;
import ai.traceable.http.event.collector.integration.config.service.v1.CreateHttpEventCollectorIntegrationResponse;
import ai.traceable.http.event.collector.integration.config.service.v1.DeleteHttpEventCollectorIntegrationRequest;
import ai.traceable.http.event.collector.integration.config.service.v1.DeleteHttpEventCollectorIntegrationResponse;
import ai.traceable.http.event.collector.integration.config.service.v1.GetHttpEventCollectorIntegrationsRequest;
import ai.traceable.http.event.collector.integration.config.service.v1.GetHttpEventCollectorIntegrationsResponse;
import ai.traceable.http.event.collector.integration.config.service.v1.HttpEventCollectorIntegration;
import ai.traceable.http.event.collector.integration.config.service.v1.HttpEventCollectorIntegrationConfigServiceGrpc.HttpEventCollectorIntegrationConfigServiceImplBase;
import ai.traceable.http.event.collector.integration.config.service.v1.HttpEventCollectorIntegrationDetails;
import ai.traceable.http.event.collector.integration.config.service.v1.UpdateHttpEventCollectorIntegrationRequest;
import ai.traceable.http.event.collector.integration.config.service.v1.UpdateHttpEventCollectorIntegrationResponse;
import ai.traceable.http.event.collector.integration.config.service.validator.HttpEventCollectorIntegrationConfigRequestValidator;
import com.typesafe.config.Config;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = {@Inject})
public class HttpEventCollectorIntegrationConfigServiceImpl
    extends HttpEventCollectorIntegrationConfigServiceImplBase {
  private final HttpEventCollectorIntegrationConfigServiceStore
      httpEventCollectorIntegrationConfigServiceStore;
  private final HttpEventCollectorIntegrationConfigRequestValidator requestValidator;

  private final UuidGenerator uuidGenerator;
  private final Config config;

  @Override
  public void createHttpEventCollectorIntegration(
      CreateHttpEventCollectorIntegrationRequest request,
      StreamObserver<CreateHttpEventCollectorIntegrationResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(requestContext, request);
      final HttpEventCollectorIntegration httpEventCollectorIntegration =
          HttpEventCollectorIntegration.newBuilder()
              .setId(uuidGenerator.generateRandomId())
              .setDetails(
                  HttpEventCollectorIntegrationDetails.newBuilder()
                      .setName(request.getName())
                      .setDescription(request.getDescription())
                      .setHttpEventCollectorUrl(request.getHttpEventCollectorUrl())
                      .setApiToken(request.getApiToken())
                      .setThirdPartyVendorValue(request.getThirdPartyVendorValue()))
              .build();
      HttpEventCollectorIntegration created =
          httpEventCollectorIntegrationConfigServiceStore
              .upsertObject(requestContext, httpEventCollectorIntegration)
              .getData();
      responseObserver.onNext(
          CreateHttpEventCollectorIntegrationResponse.newBuilder().setIntegration(created).build());
      responseObserver.onCompleted();
    } catch (Throwable throwable) {
      log.warn(
          "Could not create http event collector integration in context:{} for request:{}",
          requestContext,
          request,
          throwable);
      responseObserver.onError(throwable);
    }
  }

  @Override
  public void getHttpEventCollectorIntegrations(
      GetHttpEventCollectorIntegrationsRequest request,
      StreamObserver<GetHttpEventCollectorIntegrationsResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(requestContext, request);

      List<HttpEventCollectorIntegration> integrations =
          request.hasFilter()
              ? httpEventCollectorIntegrationConfigServiceStore.getAllConfigData(
                  requestContext, request.getFilter())
              : httpEventCollectorIntegrationConfigServiceStore.getAllConfigData(requestContext);
      responseObserver.onNext(
          GetHttpEventCollectorIntegrationsResponse.newBuilder()
              .addAllIntegrations(integrations)
              .build());
      responseObserver.onCompleted();
    } catch (Throwable throwable) {
      log.warn(
          "Could not get http event collector integration in context:{} for request:{}",
          requestContext,
          request,
          throwable);
      responseObserver.onError(throwable);
    }
  }

  @Override
  public void deleteHttpEventCollectorIntegration(
      DeleteHttpEventCollectorIntegrationRequest request,
      StreamObserver<DeleteHttpEventCollectorIntegrationResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(requestContext, request);
      httpEventCollectorIntegrationConfigServiceStore.deleteObject(requestContext, request.getId());
      responseObserver.onNext(DeleteHttpEventCollectorIntegrationResponse.newBuilder().build());
      responseObserver.onCompleted();
    } catch (Throwable throwable) {
      log.warn(
          "Could not delete http event collector integration in context:{} for request:{}",
          requestContext,
          request,
          throwable);
      responseObserver.onError(throwable);
    }
  }

  @Override
  public void updateHttpEventCollectorIntegration(
      UpdateHttpEventCollectorIntegrationRequest request,
      StreamObserver<UpdateHttpEventCollectorIntegrationResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(requestContext, request);
      HttpEventCollectorIntegration existing =
          httpEventCollectorIntegrationConfigServiceStore
              .getData(requestContext, request.getId())
              .orElseThrow();
      HttpEventCollectorIntegrationDetails.Builder detailsBuilder =
          HttpEventCollectorIntegrationDetails.newBuilder();
      detailsBuilder.setName(request.getName());
      detailsBuilder.setDescription(request.getDescription());
      detailsBuilder.setHttpEventCollectorUrl(request.getHttpEventCollectorUrl());
      detailsBuilder.setApiToken(
          request.hasApiToken() ? request.getApiToken() : existing.getDetails().getApiToken());
      detailsBuilder.setThirdPartyVendorValue(existing.getDetails().getThirdPartyVendorValue());
      HttpEventCollectorIntegration httpEventCollectorIntegration =
          HttpEventCollectorIntegration.newBuilder()
              .setId(request.getId())
              .setDetails(detailsBuilder.build())
              .build();
      HttpEventCollectorIntegration created =
          httpEventCollectorIntegrationConfigServiceStore
              .upsertObject(requestContext, httpEventCollectorIntegration)
              .getData();
      responseObserver.onNext(
          UpdateHttpEventCollectorIntegrationResponse.newBuilder().setIntegration(created).build());
      responseObserver.onCompleted();
    } catch (Throwable throwable) {
      log.warn(
          "Could not update http event collector integration in context:{} for request:{}",
          requestContext,
          request,
          throwable);
      responseObserver.onError(throwable);
    }
  }
}
