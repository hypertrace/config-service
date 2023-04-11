package ai.traceable.waf.provider.integration.service;

import ai.traceable.waf.integration.service.api.v1.CreateWafIntegrationRequest;
import ai.traceable.waf.integration.service.api.v1.CreateWafIntegrationResponse;
import ai.traceable.waf.integration.service.api.v1.DeleteWafIntegrationRequest;
import ai.traceable.waf.integration.service.api.v1.DeleteWafIntegrationResponse;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationRequest;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationResponse;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationsFilter.WafProviderType;
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
import org.hypertrace.config.objectstore.ConfigObject;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class WafIntegrationConfigServiceImpl extends WafProviderServiceImplBase {
  private final IdentifiedObjectStore<WafIntegration> wafIntegrationStore;
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
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      wafIntegrationConfigRequestValidator.validateOrThrow(request, requestContext);
      if (isIntegrationConfigured(requestContext)) {
        throw Status.ALREADY_EXISTS
            .withDescription("Only a single integration can be configured")
            .asRuntimeException();
      }
      WafIntegration wafIntegration =
          WafIntegration.newBuilder()
              .setId(UUID.randomUUID().toString())
              .setWafIntegrationDetails(request.getWafIntegrationDetails())
              .build();
      WafIntegration createdWafIntegration =
          wafIntegrationStore.upsertObject(requestContext, wafIntegration).getData();
      responseStreamObserver.onNext(
          CreateWafIntegrationResponse.newBuilder()
              .setWafIntegration(createdWafIntegration)
              .build());
      responseStreamObserver.onCompleted();
    } catch (Exception e) {
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
      List<String> requiredIds = request.getFilter().getIdsList();
      List<WafProviderType> requiredTypes = request.getFilter().getWafProviderTypesList();
      List<WafIntegration> tenantWafIntegrations =
          wafIntegrationStore.getAllObjects(requestContext).stream()
              .map(ConfigObject::getData)
              .filter(wafIntegration -> checkWafIdPresence(wafIntegration.getId(), requiredIds))
              .filter(
                  wafIntegration ->
                      checkWafTypePresence(
                          getWafProviderTypeFromDetails(wafIntegration.getWafIntegrationDetails()),
                          requiredTypes))
              .collect(Collectors.toList());

      // TODO: Return empty secrets in integration params
      responseStreamObserver.onNext(
          GetWafIntegrationsResponse.newBuilder()
              .addAllWafIntegration(tenantWafIntegrations)
              .build());
      responseStreamObserver.onCompleted();
    } catch (Exception e) {
      responseStreamObserver.onError(e);
    }
  }

  @Override
  public void updateWafIntegration(
      UpdateWafIntegrationRequest request,
      StreamObserver<UpdateWafIntegrationResponse> responseStreamObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      wafIntegrationConfigRequestValidator.validateOrThrow(request, requestContext);
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
              .setWafIntegration(upsertedWafIntegration)
              .build());

      responseStreamObserver.onCompleted();
    } catch (Exception e) {
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
      responseStreamObserver.onError(e);
    }
  }

  private WafProviderType getWafProviderTypeFromDetails(WafIntegrationDetails details) {
    switch (details.getIntegrationParamsCase()) {
      case CLOUDFLARE_INTEGRATION_PARAMS:
        return WafProviderType.WAF_PROVIDER_TYPE_CLOUDFLARE;
      case AWS_INTEGRATION_PARAMS:
        return WafProviderType.WAF_PROVIDER_TYPE_AWS;
      case IMPERVA_INTEGRATION_PARAMS:
        return WafProviderType.WAF_PROVIDER_TYPE_IMPERVA;
      case INTEGRATIONPARAMS_NOT_SET:
      default:
        return WafProviderType.WAF_PROVIDER_TYPE_UNSPECIFIED;
    }
  }

  private boolean checkWafIdPresence(String id, List<String> requiredIds) {
    // empty list is treated as no filter
    if (requiredIds.isEmpty()) {
      return true;
    }
    return requiredIds.contains(id);
  }

  private boolean checkWafTypePresence(WafProviderType type, List<WafProviderType> requiredTypes) {
    if (requiredTypes.isEmpty()) {
      return true;
    }
    return requiredTypes.contains(type);
  }

  private boolean isIntegrationConfigured(RequestContext requestContext) {
    return wafIntegrationStore.getAllObjects(requestContext).stream().findAny().isPresent();
  }
}
