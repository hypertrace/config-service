package ai.traceable.waf.provider.integration.service;

import ai.traceable.waf.integration.service.api.v1.CreateWafIntegrationRequest;
import ai.traceable.waf.integration.service.api.v1.CreateWafIntegrationResponse;
import ai.traceable.waf.integration.service.api.v1.DeleteWafIntegrationRequest;
import ai.traceable.waf.integration.service.api.v1.DeleteWafIntegrationResponse;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationRequest;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationResponse;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationsDetailsRequest;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationsDetailsResponse;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationsFilter;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationsRequest;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationsResponse;
import ai.traceable.waf.integration.service.api.v1.UpdateWafIntegrationRequest;
import ai.traceable.waf.integration.service.api.v1.UpdateWafIntegrationResponse;
import ai.traceable.waf.integration.service.api.v1.WafIntegration;
import ai.traceable.waf.integration.service.api.v1.WafIntegrationDetails;
import ai.traceable.waf.integration.service.api.v1.WafProviderServiceGrpc.WafProviderServiceImplBase;
import com.google.inject.Inject;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class WafIntegrationConfigServiceImpl extends WafProviderServiceImplBase {
  private final IdentifiedObjectStoreWithFilter<WafIntegration, GetWafIntegrationsFilter>
      wafIntegrationStore;
  private final WafIntegrationConfigRequestValidator wafIntegrationConfigRequestValidator;

  @Inject
  public WafIntegrationConfigServiceImpl(
      WafIntegrationStore wafIntegrationStore,
      WafIntegrationConfigRequestValidator wafIntegrationConfigRequestValidator) {
    this.wafIntegrationStore = wafIntegrationStore;
    this.wafIntegrationConfigRequestValidator = wafIntegrationConfigRequestValidator;
  }

  @Override
  public void createWafIntegration(
      CreateWafIntegrationRequest request,
      StreamObserver<CreateWafIntegrationResponse> responseStreamObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      List<WafIntegration> existingWafIntegrations =
          wafIntegrationStore.getAllConfigData(
              requestContext, GetWafIntegrationsFilter.newBuilder().build());
      wafIntegrationConfigRequestValidator.validateOrThrow(
          request, requestContext, existingWafIntegrations);

      WafIntegrationDetails.Builder detailsBuilder = request.getWafIntegrationDetails().toBuilder();
      if (!request.getWafIntegrationDetails().hasEnabled()) {
        detailsBuilder.setEnabled(true);
      }

      WafIntegration wafIntegration =
          WafIntegration.newBuilder()
              .setId(UUID.randomUUID().toString())
              .setWafIntegrationDetails(detailsBuilder.build())
              .build();
      WafIntegration createdWafIntegration =
          wafIntegrationStore
              .upsertObject(
                  requestContext,
                  WafIntegrationBuilderUtils.getCreateWafIntegrationV2(wafIntegration))
              .getData();
      responseStreamObserver.onNext(
          CreateWafIntegrationResponse.newBuilder()
              .setWafIntegration(WafIntegrationBuilderUtils.stripSecrets(createdWafIntegration))
              .build());
      responseStreamObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Failed while creating waf integration for request: {} with exception: ", request, e);
      responseStreamObserver.onError(e);
    }
  }

  @Override
  public void getWafIntegration(
      GetWafIntegrationRequest request,
      StreamObserver<GetWafIntegrationResponse> responseStreamObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      wafIntegrationConfigRequestValidator.validateOrThrow(request, requestContext);
      WafIntegration wafIntegration =
          wafIntegrationStore
              .getData(requestContext, request.getId())
              .orElseThrow(Status.NOT_FOUND::asRuntimeException);
      responseStreamObserver.onNext(
          GetWafIntegrationResponse.newBuilder().setWafIntegration(wafIntegration).build());
      responseStreamObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Failed while getting waf integration for request: {} with exception: ", request, e);
      responseStreamObserver.onError(e);
    }
  }

  @Override
  public void getWafIntegrations(
      GetWafIntegrationsRequest request,
      StreamObserver<GetWafIntegrationsResponse> responseStreamObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      wafIntegrationConfigRequestValidator.validateOrThrow(request, requestContext);
      List<WafIntegration> wafIntegrationList =
          wafIntegrationStore.getAllConfigData(requestContext, request.getFilter());
      List<WafIntegration> wafIntegrations =
          wafIntegrationList.stream()
              .map(WafIntegrationBuilderUtils::stripSecrets)
              .collect(Collectors.toList());
      responseStreamObserver.onNext(
          GetWafIntegrationsResponse.newBuilder().addAllWafIntegration(wafIntegrations).build());
      responseStreamObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Failed while getting waf integrations for request: {} with exception: ", request, e);
      responseStreamObserver.onError(e);
    }
  }

  @Override
  public void getWafIntegrationsDetails(
      GetWafIntegrationsDetailsRequest request,
      StreamObserver<GetWafIntegrationsDetailsResponse> responseStreamObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      wafIntegrationConfigRequestValidator.validateOrThrow(request, requestContext);
      List<WafIntegration> wafIntegrationList =
          wafIntegrationStore.getAllConfigData(requestContext, request.getFilter());
      responseStreamObserver.onNext(
          GetWafIntegrationsDetailsResponse.newBuilder()
              .addAllWafIntegration(wafIntegrationList)
              .build());
      responseStreamObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Failed while getting waf integration details for request: {} with exception: ",
          request,
          e);
      responseStreamObserver.onError(e);
    }
  }

  @Override
  public void updateWafIntegration(
      UpdateWafIntegrationRequest request,
      StreamObserver<UpdateWafIntegrationResponse> responseStreamObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      List<WafIntegration> existingWafIntegrations =
          wafIntegrationStore.getAllConfigData(
              requestContext, GetWafIntegrationsFilter.newBuilder().build());
      wafIntegrationConfigRequestValidator.validateOrThrow(
          request, requestContext, existingWafIntegrations);
      WafIntegration existingWafIntegration =
          wafIntegrationStore
              .getData(requestContext, request.getId())
              .orElseThrow(Status.NOT_FOUND::asRuntimeException);

      WafIntegration updatedWafIntegration =
          WafIntegrationBuilderUtils.getUpdatedIntegration(request, existingWafIntegration);

      WafIntegration upsertedWafIntegration =
          wafIntegrationStore.upsertObject(requestContext, updatedWafIntegration).getData();
      responseStreamObserver.onNext(
          UpdateWafIntegrationResponse.newBuilder()
              .setWafIntegration(WafIntegrationBuilderUtils.stripSecrets(upsertedWafIntegration))
              .build());

      responseStreamObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Failed while updating waf integration for request: {} with exception: ", request, e);
      responseStreamObserver.onError(e);
    }
  }

  @Override
  public void deleteWafIntegration(
      DeleteWafIntegrationRequest request,
      StreamObserver<DeleteWafIntegrationResponse> responseStreamObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      wafIntegrationConfigRequestValidator.validateOrThrow(request, requestContext);
      wafIntegrationStore
          .deleteObject(requestContext, request.getId())
          .orElseThrow(Status.NOT_FOUND::asRuntimeException);
      responseStreamObserver.onNext(DeleteWafIntegrationResponse.getDefaultInstance());
      responseStreamObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Failed while deleting waf integration for request: {} with exception: ", request, e);
      responseStreamObserver.onError(e);
    }
  }
}
