package ai.traceable.cloud.bot.deployment.config.service.v1;

import ai.traceable.cloud.bot.deployment.config.service.v1.CloudBotDeploymentConfigServiceGrpc.CloudBotDeploymentConfigServiceImplBase;
import ai.traceable.cloud.bot.deployment.config.service.v1.manager.CloudBotDeploymentConfigManager;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = {@Inject})
public class CloudBotDeploymentConfigServiceImpl extends CloudBotDeploymentConfigServiceImplBase {

  private final CloudBotDeploymentConfigManager cloudBotDeploymentConfigManager;

  @Override
  public void createCloudBotDeploymentConfig(
      CreateCloudBotDeploymentConfigRequest request,
      StreamObserver<CreateCloudBotDeploymentConfigResponse> responseObserver) {
    RequestContext ctx = RequestContext.CURRENT.get();
    try {
      CloudBotDeploymentConfig config =
          cloudBotDeploymentConfigManager.createCloudBotDeploymentConfig(ctx, request);

      responseObserver.onNext(
          CreateCloudBotDeploymentConfigResponse.newBuilder()
              .setCloudBotDeployment(config)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Create cloud bot deployment config failed with request: {} and context: {}",
          request,
          ctx,
          e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateCloudBotDeploymentConfig(
      UpdateCloudBotDeploymentConfigRequest request,
      StreamObserver<UpdateCloudBotDeploymentConfigResponse> responseObserver) {
    RequestContext ctx = RequestContext.CURRENT.get();
    try {
      CloudBotDeploymentConfig config =
          cloudBotDeploymentConfigManager.updateCloudBotDeploymentConfig(
              ctx, request.getId(), request);

      responseObserver.onNext(
          UpdateCloudBotDeploymentConfigResponse.newBuilder()
              .setCloudBotDeployment(config)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Update cloud bot deployment config failed with request: {} and context: {}",
          request,
          ctx,
          e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateCloudBotDeploymentStatus(
      UpdateCloudBotDeploymentStatusRequest request,
      StreamObserver<UpdateCloudBotDeploymentStatusResponse> responseObserver) {
    RequestContext ctx = RequestContext.CURRENT.get();
    try {
      CloudBotDeploymentConfig config =
          cloudBotDeploymentConfigManager.updateCloudBotDeploymentStatus(
              ctx, request.getId(), request);

      responseObserver.onNext(
          UpdateCloudBotDeploymentStatusResponse.newBuilder()
              .setCloudBotDeployment(config)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Update cloud bot deployment status failed with request: {} and context: {}",
          request,
          ctx,
          e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void deleteCloudBotDeploymentConfig(
      DeleteCloudBotDeploymentConfigRequest request,
      StreamObserver<DeleteCloudBotDeploymentConfigResponse> responseObserver) {
    RequestContext ctx = RequestContext.CURRENT.get();
    try {
      cloudBotDeploymentConfigManager.deleteCloudBotDeploymentConfig(ctx, request.getId());
      responseObserver.onNext(DeleteCloudBotDeploymentConfigResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Delete cloud bot deployment config failed with request: {} and context: {}",
          request,
          ctx,
          e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void rotateApiToken(
      RotateApiTokenRequest request, StreamObserver<RotateApiTokenResponse> responseObserver) {
    RequestContext ctx = RequestContext.CURRENT.get();
    try {
      CloudBotDeploymentConfig config =
          cloudBotDeploymentConfigManager.rotateApiToken(ctx, request.getId());
      responseObserver.onNext(
          RotateApiTokenResponse.newBuilder().setCloudBotDeployment(config).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Rotate API token failed with request: {} and context: {}", request, ctx, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getCloudBotDeploymentConfigs(
      GetCloudBotDeploymentConfigsRequest request,
      StreamObserver<GetCloudBotDeploymentConfigsResponse> responseObserver) {
    RequestContext ctx = RequestContext.CURRENT.get();
    try {
      java.util.List<CloudBotDeploymentConfig> configs =
          cloudBotDeploymentConfigManager.getCloudBotDeploymentConfigs(ctx, request.getIdsList());

      responseObserver.onNext(
          GetCloudBotDeploymentConfigsResponse.newBuilder()
              .addAllCloudBotDeployments(configs)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Get cloud bot deployment configs failed with request: {} and context: {}",
          request,
          ctx,
          e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void enableCloudBotDeployment(
      EnableCloudBotDeploymentRequest request,
      StreamObserver<EnableCloudBotDeploymentResponse> responseObserver) {
    RequestContext ctx = RequestContext.CURRENT.get();
    try {
      cloudBotDeploymentConfigManager.enableCloudBotDeployment(ctx, request);
      responseObserver.onNext(EnableCloudBotDeploymentResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "EnableCloudBotDeployment request failed with id: {} and context: {}",
          request.getId(),
          ctx,
          e);
      responseObserver.onError(e);
    }
  }
}
