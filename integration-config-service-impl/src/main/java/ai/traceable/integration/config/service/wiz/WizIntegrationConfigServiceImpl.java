package ai.traceable.integration.config.service.wiz;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.integration.config.service.wiz.store.WizIntegrationConfigStore;
import ai.traceable.integration.config.service.wiz.v1.CreateWizIntegrationRequest;
import ai.traceable.integration.config.service.wiz.v1.CreateWizIntegrationResponse;
import ai.traceable.integration.config.service.wiz.v1.DeleteWizIntegrationRequest;
import ai.traceable.integration.config.service.wiz.v1.DeleteWizIntegrationResponse;
import ai.traceable.integration.config.service.wiz.v1.EncryptedText;
import ai.traceable.integration.config.service.wiz.v1.GetWizIntegrationSummariesRequest;
import ai.traceable.integration.config.service.wiz.v1.GetWizIntegrationSummariesResponse;
import ai.traceable.integration.config.service.wiz.v1.GetWizIntegrationsRequest;
import ai.traceable.integration.config.service.wiz.v1.GetWizIntegrationsResponse;
import ai.traceable.integration.config.service.wiz.v1.UpdateWizIntegrationRequest;
import ai.traceable.integration.config.service.wiz.v1.UpdateWizIntegrationResponse;
import ai.traceable.integration.config.service.wiz.v1.WizIntegration;
import ai.traceable.integration.config.service.wiz.v1.WizIntegrationConfigServiceGrpc.WizIntegrationConfigServiceImplBase;
import ai.traceable.integration.config.service.wiz.v1.WizIntegrationInfo;
import ai.traceable.integration.config.service.wiz.v1.WizIntegrationPreferences;
import ai.traceable.integration.config.service.wiz.v1.WizIntegrationSummary;
import ai.traceable.integration.config.service.wiz.v1.WizIssuePullConfiguration;
import ai.traceable.integration.config.service.wiz.validation.WizIntegrationConfigRequestValidator;
import ai.traceable.integration.config.service.wiz.validation.WizIntegrationConfigServiceStateValidator;
import com.google.inject.Inject;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = {@Inject})
public class WizIntegrationConfigServiceImpl extends WizIntegrationConfigServiceImplBase {

  private final WizIntegrationConfigRequestValidator requestValidator;
  private final WizIntegrationConfigServiceStateValidator serviceStateValidator;
  private final WizIntegrationConfigStore wizIntegrationConfigStore;
  private final UuidGenerator uuidGenerator;

  @Override
  public void createWizIntegration(
      CreateWizIntegrationRequest request,
      StreamObserver<CreateWizIntegrationResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(requestContext, request);
      serviceStateValidator.validateNoExistingWizIntegration(requestContext);

      final WizIntegration wizIntegration =
          WizIntegration.newBuilder()
              .setId(uuidGenerator.generateRandomId())
              .setClientSecret(request.getClientSecret())
              .setInfo(
                  WizIntegrationInfo.newBuilder()
                      .setName(request.getName())
                      .setClientId(request.getClientId())
                      .setTokenUrl(request.getTokenUrl())
                      .setDescription(request.getDescription())
                      .setApiEndpointUrl(request.getApiEndpointUrl())
                      .setWizIntegrationPreferences(
                          request.hasWizIntegrationPreferences()
                              ? request.getWizIntegrationPreferences()
                              : getDefaultWizIntegrationPreferences())
                      .build())
              .build();
      WizIntegration createdWizIntegrationConfig =
          wizIntegrationConfigStore.upsertObject(requestContext, wizIntegration).getData();

      responseObserver.onNext(
          CreateWizIntegrationResponse.newBuilder()
              .setIntegration(createdWizIntegrationConfig)
              .build());
      responseObserver.onCompleted();
    } catch (Throwable throwable) {
      log.warn(
          "Could not create wiz Integration for request: {} within context: {}",
          request,
          requestContext,
          throwable);
      responseObserver.onError(throwable);
    }
  }

  @Override
  public void getWizIntegrationSummaries(
      GetWizIntegrationSummariesRequest request,
      StreamObserver<GetWizIntegrationSummariesResponse> responseStreamObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(requestContext, request);
      List<WizIntegration> integrations =
          request.hasFilter()
              ? wizIntegrationConfigStore.getAllConfigData(requestContext, request.getFilter())
              : wizIntegrationConfigStore.getAllConfigData(requestContext);

      List<WizIntegrationSummary> integrationSummaries =
          integrations.stream()
              .map(this::asBackwardCompatible)
              .map(
                  integration ->
                      WizIntegrationSummary.newBuilder()
                          .setId(integration.getId())
                          .setInfo(integration.getInfo())
                          .build())
              .collect(Collectors.toUnmodifiableList());

      responseStreamObserver.onNext(
          GetWizIntegrationSummariesResponse.newBuilder()
              .addAllIntegrationSummaries(integrationSummaries)
              .build());
      responseStreamObserver.onCompleted();
    } catch (Throwable throwable) {
      log.warn(
          "Could not get wiz Integration summaries for request: {} within context: {}",
          request,
          requestContext,
          throwable);
      responseStreamObserver.onError(throwable);
    }
  }

  @Override
  public void getWizIntegrations(
      GetWizIntegrationsRequest request,
      StreamObserver<GetWizIntegrationsResponse> responseStreamObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(requestContext, request);
      List<WizIntegration> integrations =
          (request.hasFilter()
                  ? wizIntegrationConfigStore.getAllConfigData(requestContext, request.getFilter())
                  : wizIntegrationConfigStore.getAllConfigData(requestContext))
              .stream().map(this::asBackwardCompatible).collect(Collectors.toUnmodifiableList());
      responseStreamObserver.onNext(
          GetWizIntegrationsResponse.newBuilder().addAllIntegrations(integrations).build());
      responseStreamObserver.onCompleted();
    } catch (Throwable throwable) {
      log.warn(
          "Could not create wiz Integrations for request: {} within context: {}",
          request,
          requestContext,
          throwable);
      responseStreamObserver.onError(throwable);
    }
  }

  @Override
  public void updateWizIntegration(
      UpdateWizIntegrationRequest request,
      StreamObserver<UpdateWizIntegrationResponse> responseStreamObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(requestContext, request);

      WizIntegration.Builder builder = WizIntegration.newBuilder();
      builder.setId(request.getId());
      WizIntegrationInfo.Builder infoBuilder = WizIntegrationInfo.newBuilder();
      infoBuilder.setName(request.getName());
      infoBuilder.setDescription(request.getDescription());
      infoBuilder.setClientId(request.getClientId());
      infoBuilder.setTokenUrl(request.getTokenUrl());
      infoBuilder.setApiEndpointUrl(request.getApiEndpointUrl());
      infoBuilder.setWizIntegrationPreferences(
          request.hasWizIntegrationPreferences()
              ? request.getWizIntegrationPreferences()
              : this.getDefaultWizIntegrationPreferences());

      EncryptedText clientSecret =
          request.hasClientSecret()
              ? request.getClientSecret()
              : wizIntegrationConfigStore
                  .getData(requestContext, request.getId())
                  .map(WizIntegration::getClientSecret)
                  .orElseThrow();
      builder.setClientSecret(clientSecret);
      final WizIntegration wizIntegration = builder.setInfo(infoBuilder.build()).build();
      WizIntegration updatedWizIntegration =
          wizIntegrationConfigStore.upsertObject(requestContext, wizIntegration).getData();

      responseStreamObserver.onNext(
          UpdateWizIntegrationResponse.newBuilder().setIntegration(updatedWizIntegration).build());
      responseStreamObserver.onCompleted();
    } catch (Throwable throwable) {
      log.warn(
          "Could not update wiz Integration for request: {} within context: {}",
          request,
          requestContext,
          throwable);
      responseStreamObserver.onError(throwable);
    }
  }

  @Override
  public void deleteWizIntegration(
      DeleteWizIntegrationRequest request,
      StreamObserver<DeleteWizIntegrationResponse> responseStreamObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(requestContext, request);
      wizIntegrationConfigStore.deleteObject(requestContext, request.getId());
      responseStreamObserver.onNext(DeleteWizIntegrationResponse.getDefaultInstance());
      responseStreamObserver.onCompleted();
    } catch (Throwable throwable) {
      log.warn(
          "Could not delete wiz Integration for request: {} within context: {}",
          request,
          requestContext,
          throwable);
      responseStreamObserver.onError(throwable);
    }
  }

  private WizIntegration asBackwardCompatible(WizIntegration integration) {
    // older integrations will not have the preferences, they default to pulling wiz issues
    if (integration.getInfo().hasWizIntegrationPreferences()) {
      return integration;
    }
    return integration.toBuilder()
        .setInfo(
            integration.getInfo().toBuilder()
                .setWizIntegrationPreferences(getDefaultWizIntegrationPreferences()))
        .build();
  }

  private WizIntegrationPreferences getDefaultWizIntegrationPreferences() {
    return WizIntegrationPreferences.newBuilder()
        .setWizIssuePullConfiguration(WizIssuePullConfiguration.newBuilder().setEnabled(true))
        .build();
  }
}
