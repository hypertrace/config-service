package ai.traceable.cloud.edge.deployment.config.service.v1;

import ai.traceable.cloud.edge.deployment.config.service.v1.CloudEdgeDeploymentConfigServiceGrpc.CloudEdgeDeploymentConfigServiceImplBase;
import ai.traceable.cloud.edge.deployment.config.service.v1.manager.CloudEdgeDeploymentConfigManager;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = {@Inject})
public class CloudEdgeDeploymentConfigServiceImpl extends CloudEdgeDeploymentConfigServiceImplBase {

  private final CloudEdgeDeploymentConfigManager cloudEdgeDeploymentConfigManager;

  @Override
  public void createCloudEdgeDeploymentConfig(
      CreateCloudEdgeDeploymentConfigRequest request,
      StreamObserver<CreateCloudEdgeDeploymentConfigResponse> responseObserver) {
    RequestContext ctx = RequestContext.CURRENT.get();
    try {
      CloudEdgeDeploymentConfig config =
          cloudEdgeDeploymentConfigManager.createCloudEdgeDeploymentConfig(ctx, request);

      responseObserver.onNext(
          CreateCloudEdgeDeploymentConfigResponse.newBuilder()
              .setCloudEdgeDeployment(config)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Create cloud edge deployment config failed with request: {} and context: {}",
          request,
          ctx,
          e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateCloudEdgeDeploymentConfig(
      UpdateCloudEdgeDeploymentConfigRequest request,
      StreamObserver<UpdateCloudEdgeDeploymentConfigResponse> responseObserver) {
    RequestContext ctx = RequestContext.CURRENT.get();
    try {
      CloudEdgeDeploymentConfig config =
          cloudEdgeDeploymentConfigManager.updateCloudEdgeDeploymentConfig(
              ctx, request.getId(), request);

      responseObserver.onNext(
          UpdateCloudEdgeDeploymentConfigResponse.newBuilder()
              .setCloudEdgeDeployment(config)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Update cloud edge deployment config failed with request: {} and context: {}",
          request,
          ctx,
          e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getCloudEdgeDeploymentConfigs(
      GetCloudEdgeDeploymentConfigsRequest request,
      StreamObserver<GetCloudEdgeDeploymentConfigsResponse> responseObserver) {
    RequestContext ctx = RequestContext.CURRENT.get();
    try {
      // Create a filter that combines both explicit IDs and any other filter criteria
      GetCloudEdgeDeploymentConfigsFilter.Builder filterBuilder =
          request.hasFilter()
              ? request.getFilter().toBuilder()
              : GetCloudEdgeDeploymentConfigsFilter.newBuilder();

      // Add any explicit IDs to the filter
      if (request.getIdsCount() > 0) {
        filterBuilder.addAllIds(request.getIdsList());
      }

      // Use the combined filter for all requests
      GetCloudEdgeDeploymentConfigsResponse response =
          cloudEdgeDeploymentConfigManager.getCloudEdgeDeploymentConfigsWithActions(
              ctx, filterBuilder.build(), request.getReadAccess());

      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Get cloud edge deployment configs failed with request: {} and context: {}",
          request,
          ctx,
          e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void deleteCloudEdgeDeploymentConfig(
      DeleteCloudEdgeDeploymentConfigRequest request,
      StreamObserver<DeleteCloudEdgeDeploymentConfigResponse> responseObserver) {
    RequestContext ctx = RequestContext.CURRENT.get();
    try {
      cloudEdgeDeploymentConfigManager.deleteCloudEdgeDeploymentConfig(ctx, request.getId());

      responseObserver.onNext(DeleteCloudEdgeDeploymentConfigResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Delete cloud edge deployment config failed with request: {} and context: {}",
          request,
          ctx,
          e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void cancelCloudEdgeDeploymentConfigAction(
      CancelCloudEdgeDeploymentConfigActionRequest request,
      StreamObserver<CancelCloudEdgeDeploymentConfigActionResponse> responseObserver) {
    RequestContext ctx = RequestContext.CURRENT.get();
    try {
      cloudEdgeDeploymentConfigManager.cancelCloudEdgeDeploymentConfigAction(ctx, request);

      responseObserver.onNext(CancelCloudEdgeDeploymentConfigActionResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Cancel cloud edge deployment config action failed with request: {} and context: {}",
          request,
          ctx,
          e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getSharedConfigMetadata(
      GetSharedConfigMetadataRequest request,
      StreamObserver<GetSharedConfigMetadataResponse> responseObserver) {
    RequestContext ctx = RequestContext.CURRENT.get();
    try {
      SharedConfigMetadata metadata =
          cloudEdgeDeploymentConfigManager.getSharedConfigMetadata(ctx, request.getReadAccess());

      responseObserver.onNext(
          GetSharedConfigMetadataResponse.newBuilder().setSharedConfigMetadata(metadata).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Get shared config metadata failed with request: {} and context: {}", request, ctx, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void removeCloudEdgeDeploymentConfig(
      RemoveCloudEdgeDeploymentConfigRequest request,
      StreamObserver<RemoveCloudEdgeDeploymentConfigResponse> responseObserver) {
    RequestContext ctx = RequestContext.CURRENT.get();
    try {
      cloudEdgeDeploymentConfigManager.removeCloudEdgeDeploymentConfig(ctx, request);

      responseObserver.onNext(RemoveCloudEdgeDeploymentConfigResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Remove cloud edge deployment config failed with request: {} and context: {}",
          request,
          ctx,
          e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void holdCloudEdgeDeploymentConfig(
      HoldCloudEdgeDeploymentConfigRequest request,
      StreamObserver<HoldCloudEdgeDeploymentConfigResponse> responseObserver) {
    RequestContext ctx = RequestContext.CURRENT.get();
    try {
      cloudEdgeDeploymentConfigManager.holdCloudEdgeDeploymentConfig(ctx, request);

      responseObserver.onNext(HoldCloudEdgeDeploymentConfigResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Remove cloud edge deployment config failed with request: {} and context: {}",
          request,
          ctx,
          e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void deployCloudEdgeDeploymentConfig(
      DeployCloudEdgeDeploymentConfigRequest request,
      StreamObserver<DeployCloudEdgeDeploymentConfigResponse> responseObserver) {
    RequestContext ctx = RequestContext.CURRENT.get();
    try {
      cloudEdgeDeploymentConfigManager.deployCloudEdgeDeploymentConfig(ctx, request);

      responseObserver.onNext(DeployCloudEdgeDeploymentConfigResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Remove cloud edge deployment config failed with request: {} and context: {}",
          request,
          ctx,
          e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void cloudEdgeDeploymentConfigAction(
      CloudEdgeDeploymentConfigActionRequest request,
      StreamObserver<CloudEdgeDeploymentConfigActionResponse> responseObserver) {
    RequestContext ctx = RequestContext.CURRENT.get();
    try {
      cloudEdgeDeploymentConfigManager.performCloudEdgeDeploymentConfigAction(ctx, request);

      responseObserver.onNext(CloudEdgeDeploymentConfigActionResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Cloud edge deployment config action failed with request: {} and context: {}",
          request,
          ctx,
          e);
      responseObserver.onError(e);
    }
  }
}
