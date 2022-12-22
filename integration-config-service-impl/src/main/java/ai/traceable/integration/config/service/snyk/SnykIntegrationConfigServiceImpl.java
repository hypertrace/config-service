package ai.traceable.integration.config.service.snyk;

import ai.traceable.integration.config.service.snyk.store.SnykIntegrationConfigStore;
import ai.traceable.integration.config.service.snyk.v1.CreateSnykIntegrationRequest;
import ai.traceable.integration.config.service.snyk.v1.CreateSnykIntegrationResponse;
import ai.traceable.integration.config.service.snyk.v1.DeleteSnykIntegrationRequest;
import ai.traceable.integration.config.service.snyk.v1.DeleteSnykIntegrationResponse;
import ai.traceable.integration.config.service.snyk.v1.GetSnykIntegrationDetailsRequest;
import ai.traceable.integration.config.service.snyk.v1.GetSnykIntegrationDetailsResponse;
import ai.traceable.integration.config.service.snyk.v1.GetSnykIntegrationSummaryRequest;
import ai.traceable.integration.config.service.snyk.v1.GetSnykIntegrationSummaryResponse;
import ai.traceable.integration.config.service.snyk.v1.SnykIntegration;
import ai.traceable.integration.config.service.snyk.v1.SnykIntegrationServiceGrpc;
import ai.traceable.integration.config.service.snyk.v1.SnykIntegrationSummary;
import ai.traceable.integration.config.service.snyk.v1.UpdateSnykIntegrationRequest;
import ai.traceable.integration.config.service.snyk.v1.UpdateSnykIntegrationResponse;
import ai.traceable.integration.config.service.snyk.validation.SnykIntegrationConfigRequestValidator;
import com.google.inject.Inject;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = {@Inject})
public class SnykIntegrationConfigServiceImpl
    extends SnykIntegrationServiceGrpc.SnykIntegrationServiceImplBase {

  private final SnykIntegrationConfigRequestValidator requestValidator;
  private final SnykIntegrationConfigStore snykIntegrationConfigStore;

  @Override
  public void getSnykIntegrationSummary(
      GetSnykIntegrationSummaryRequest request,
      StreamObserver<GetSnykIntegrationSummaryResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(requestContext, request);

      final SnykIntegration snykIntegration =
          snykIntegrationConfigStore
              .getData(requestContext)
              .orElseThrow(Status.NOT_FOUND::asRuntimeException);

      responseObserver.onNext(
          GetSnykIntegrationSummaryResponse.newBuilder()
              .setSnykIntegrationSummary(snykIntegration.getSnykIntegrationSummary())
              .build());
      responseObserver.onCompleted();
    } catch (Throwable throwable) {
      log.warn(
          "Could not fetch snyk integration summary in context:{} for request:{}",
          requestContext,
          request,
          throwable);
      responseObserver.onError(throwable);
    }
  }

  @Override
  public void getSnykIntegrationDetails(
      GetSnykIntegrationDetailsRequest request,
      StreamObserver<GetSnykIntegrationDetailsResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(requestContext, request);

      final SnykIntegration snykIntegration =
          snykIntegrationConfigStore
              .getData(requestContext)
              .orElseThrow(Status.NOT_FOUND::asRuntimeException);

      responseObserver.onNext(
          GetSnykIntegrationDetailsResponse.newBuilder()
              .setSnykIntegration(snykIntegration)
              .build());
      responseObserver.onCompleted();
    } catch (Throwable throwable) {
      log.warn(
          "Could not fetch snyk integration details in context:{} for request:{}",
          requestContext,
          request,
          throwable);
      responseObserver.onError(throwable);
    }
  }

  @Override
  public void createSnykIntegration(
      CreateSnykIntegrationRequest request,
      StreamObserver<CreateSnykIntegrationResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(requestContext, request);

      Optional<SnykIntegration> existingSnykIntegration =
          snykIntegrationConfigStore.getData(requestContext);
      if (existingSnykIntegration.isPresent()) {
        throw Status.ALREADY_EXISTS.asRuntimeException();
      }

      final SnykIntegration snykIntegration = buildSnykIntegrationConfig(request);
      final SnykIntegrationSummary snykIntegrationSummary =
          snykIntegrationConfigStore
              .upsertObject(requestContext, snykIntegration)
              .getData()
              .getSnykIntegrationSummary();

      responseObserver.onNext(
          CreateSnykIntegrationResponse.newBuilder()
              .setSnykIntegrationSummary(snykIntegrationSummary)
              .build());
      responseObserver.onCompleted();
    } catch (Throwable throwable) {
      log.warn(
          "Could not create snyk integration in context:{} for request:{}",
          requestContext,
          request,
          throwable);
      responseObserver.onError(throwable);
    }
  }

  @Override
  public void updateSnykIntegration(
      UpdateSnykIntegrationRequest request,
      StreamObserver<UpdateSnykIntegrationResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(requestContext, request);

      final SnykIntegration existingSnykIntegration =
          snykIntegrationConfigStore
              .getData(requestContext)
              .orElseThrow(Status.NOT_FOUND::asRuntimeException);
      final SnykIntegration updatedSnykIntegration =
          buildUpdatedSnykIntegrationConfig(request, existingSnykIntegration);
      final SnykIntegrationSummary updatedSnykIntegrationSummary =
          snykIntegrationConfigStore
              .upsertObject(requestContext, updatedSnykIntegration)
              .getData()
              .getSnykIntegrationSummary();

      responseObserver.onNext(
          UpdateSnykIntegrationResponse.newBuilder()
              .setSnykIntegrationSummary(updatedSnykIntegrationSummary)
              .build());
      responseObserver.onCompleted();
    } catch (Throwable throwable) {
      log.warn(
          "Could not update snyk integration in context:{} for request:{}",
          requestContext,
          request,
          throwable);
      responseObserver.onError(throwable);
    }
  }

  @Override
  public void deleteSnykIntegration(
      DeleteSnykIntegrationRequest request,
      StreamObserver<DeleteSnykIntegrationResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(requestContext, request);

      snykIntegrationConfigStore
          .deleteObject(requestContext)
          .orElseThrow(Status.NOT_FOUND::asRuntimeException);

      responseObserver.onNext(DeleteSnykIntegrationResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Throwable throwable) {
      log.warn(
          "Could not delete snyk integration in context:{} for request:{}",
          requestContext,
          request,
          throwable);
      responseObserver.onError(throwable);
    }
  }

  private SnykIntegration buildSnykIntegrationConfig(CreateSnykIntegrationRequest request) {
    SnykIntegration.Builder builder =
        SnykIntegration.newBuilder()
            .setSnykIntegrationSummary(
                SnykIntegrationSummary.newBuilder().setName(request.getName()))
            .setApiToken(request.getApiToken());

    if (request.hasDescription()) {
      builder.getSnykIntegrationSummaryBuilder().setDescription(request.getDescription());
    }

    return builder.build();
  }

  private SnykIntegration buildUpdatedSnykIntegrationConfig(
      UpdateSnykIntegrationRequest request, SnykIntegration existingSnykIntegration) {
    SnykIntegration.Builder builder =
        SnykIntegration.newBuilder(existingSnykIntegration)
            .setSnykIntegrationSummary(
                SnykIntegrationSummary.newBuilder(
                        existingSnykIntegration.getSnykIntegrationSummary())
                    .setName(request.getName()));

    if (request.hasDescription()) {
      builder.getSnykIntegrationSummaryBuilder().setDescription(request.getDescription());
    }
    if (request.hasApiToken()) {
      builder.setApiToken(request.getApiToken());
    }

    return builder.build();
  }
}
