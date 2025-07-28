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
      java.util.List<CloudEdgeDeploymentConfig> configs =
          cloudEdgeDeploymentConfigManager.getCloudEdgeDeploymentConfigs(
              ctx, filterBuilder.build(), request.getReadAccess());

      responseObserver.onNext(
          GetCloudEdgeDeploymentConfigsResponse.newBuilder()
              .addAllCloudEdgeDeployments(configs)
              .build());
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
}
