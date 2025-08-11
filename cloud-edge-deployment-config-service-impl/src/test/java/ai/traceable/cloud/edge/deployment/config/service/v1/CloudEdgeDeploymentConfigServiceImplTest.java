package ai.traceable.cloud.edge.deployment.config.service.v1;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import ai.traceable.cloud.edge.deployment.config.service.v1.manager.CloudEdgeDeploymentConfigManager;
import ai.traceable.cloud.edge.deployment.config.service.v1.validator.CloudEdgeDeploymentValidator;
import io.grpc.stub.StreamObserver;
import java.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CloudEdgeDeploymentConfigServiceImplTest {

  @Mock private CloudEdgeDeploymentConfigManager configManager;
  @Mock private CloudEdgeDeploymentValidator validator;

  private CloudEdgeDeploymentConfigServiceImpl configService;

  @BeforeEach
  void setUp() {
    configService = new CloudEdgeDeploymentConfigServiceImpl(configManager);
  }

  @Test
  void testCreateCloudEdgeDeploymentConfig() {
    CreateCloudEdgeDeploymentConfigRequest request =
        CreateCloudEdgeDeploymentConfigRequest.newBuilder()
            .setCloudEdgeDeploymentInputConfig(
                CloudEdgeDeploymentInputConfig.newBuilder()
                    .setClusterConfig(
                        ClusterConfig.newBuilder().setClusterName("test-cluster").build())
                    .addServiceConfigs(
                        ServiceConfig.newBuilder().setServiceName("test-service").build())
                    .build())
            .setConfigPermission(
                ConfigPermission.newBuilder()
                    .setRead(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
                    .setWrite(ConfigAccessType.CONFIG_ACCESS_TYPE_TRACEABLE)
                    .build())
            .build();

    StreamObserver<CreateCloudEdgeDeploymentConfigResponse> responseObserver =
        mock(StreamObserver.class);

    // Test case 1: When an exception occurs during creation
    doThrow(new RuntimeException("Creation failed"))
        .when(configManager)
        .createCloudEdgeDeploymentConfig(any(), any());

    configService.createCloudEdgeDeploymentConfig(request, responseObserver);
    verify(responseObserver, times(1))
        .onError(argThat(err -> err.getMessage().contains("Creation failed")));

    // Test case 2: Successful creation
    CloudEdgeDeploymentConfig config =
        CloudEdgeDeploymentConfig.newBuilder()
            .setId("test-id")
            .setCloudEdgeDeploymentInputConfig(request.getCloudEdgeDeploymentInputConfig())
            .build();

    doReturn(config).when(configManager).createCloudEdgeDeploymentConfig(any(), any());

    configService.createCloudEdgeDeploymentConfig(request, responseObserver);
    verify(responseObserver, times(1))
        .onNext(
            CreateCloudEdgeDeploymentConfigResponse.newBuilder()
                .setCloudEdgeDeployment(config)
                .build());
    verify(responseObserver, times(1)).onCompleted();
  }

  @Test
  void testUpdateCloudEdgeDeploymentConfig() {
    UpdateCloudEdgeDeploymentConfigRequest request =
        UpdateCloudEdgeDeploymentConfigRequest.newBuilder()
            .setId("test-id")
            .setCloudEdgeDeploymentInputConfig(
                CloudEdgeDeploymentInputConfig.newBuilder()
                    .setClusterConfig(
                        ClusterConfig.newBuilder().setClusterName("updated-cluster").build())
                    .addServiceConfigs(
                        ServiceConfig.newBuilder().setServiceName("updated-service").build())
                    .build())
            .setConfigPermission(
                ConfigPermission.newBuilder()
                    .setRead(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
                    .setWrite(ConfigAccessType.CONFIG_ACCESS_TYPE_TRACEABLE)
                    .build())
            .build();

    StreamObserver<UpdateCloudEdgeDeploymentConfigResponse> responseObserver =
        mock(StreamObserver.class);

    // Test case 1: When an exception occurs during update
    doThrow(new RuntimeException("Update failed"))
        .when(configManager)
        .updateCloudEdgeDeploymentConfig(any(), any(), any());

    configService.updateCloudEdgeDeploymentConfig(request, responseObserver);
    verify(responseObserver, times(1))
        .onError(argThat(err -> err.getMessage().contains("Update failed")));

    // Test case 2: Successful update
    CloudEdgeDeploymentConfig updatedConfig =
        CloudEdgeDeploymentConfig.newBuilder()
            .setId("test-id")
            .setCloudEdgeDeploymentInputConfig(request.getCloudEdgeDeploymentInputConfig())
            .build();

    doReturn(updatedConfig)
        .when(configManager)
        .updateCloudEdgeDeploymentConfig(any(), any(), any());

    configService.updateCloudEdgeDeploymentConfig(request, responseObserver);
    verify(responseObserver, times(1))
        .onNext(
            UpdateCloudEdgeDeploymentConfigResponse.newBuilder()
                .setCloudEdgeDeployment(updatedConfig)
                .build());
    verify(responseObserver, times(1)).onCompleted();
  }

  @Test
  void testDeleteCloudEdgeDeploymentConfig() {
    DeleteCloudEdgeDeploymentConfigRequest request =
        DeleteCloudEdgeDeploymentConfigRequest.newBuilder().setId("test-id").build();

    StreamObserver<DeleteCloudEdgeDeploymentConfigResponse> responseObserver =
        mock(StreamObserver.class);

    // Test case 1: When an exception occurs during deletion
    doThrow(new RuntimeException("Deletion failed"))
        .when(configManager)
        .deleteCloudEdgeDeploymentConfig(any(), any());

    configService.deleteCloudEdgeDeploymentConfig(request, responseObserver);
    verify(responseObserver, times(1))
        .onError(argThat(err -> err.getMessage().contains("Deletion failed")));

    // Test case 2: Successful deletion
    // Reset the mock behavior to not throw an exception
    doNothing().when(configManager).deleteCloudEdgeDeploymentConfig(any(), any());

    configService.deleteCloudEdgeDeploymentConfig(request, responseObserver);
    verify(responseObserver, times(1))
        .onNext(DeleteCloudEdgeDeploymentConfigResponse.getDefaultInstance());
    verify(responseObserver, times(1)).onCompleted();
  }

  @Test
  void testGetCloudEdgeDeploymentConfigs() {
    GetCloudEdgeDeploymentConfigsRequest request =
        GetCloudEdgeDeploymentConfigsRequest.newBuilder()
            .addIds("test-id-1")
            .addIds("test-id-2")
            .setReadAccess(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
            .build();

    StreamObserver<GetCloudEdgeDeploymentConfigsResponse> responseObserver =
        mock(StreamObserver.class);

    // Test case 1: When an exception occurs during retrieval
    doThrow(new RuntimeException("Retrieval failed"))
        .when(configManager)
        .getCloudEdgeDeploymentConfigsWithActions(any(), any(), any());

    configService.getCloudEdgeDeploymentConfigs(request, responseObserver);
    verify(responseObserver, times(1))
        .onError(argThat(err -> err.getMessage().contains("Retrieval failed")));

    // Test case 2: Successful retrieval
    CloudEdgeDeploymentConfig config1 =
        CloudEdgeDeploymentConfig.newBuilder().setId("test-id-1").build();
    CloudEdgeDeploymentConfig config2 =
        CloudEdgeDeploymentConfig.newBuilder().setId("test-id-2").build();

    CloudEdgeDeploymentConfigWithActions configWithActions1 =
        CloudEdgeDeploymentConfigWithActions.newBuilder()
            .setCloudEdgeDeploymentConfig(config1)
            .addAllowedActions(Action.ACTION_VIEW)
            .build();
    CloudEdgeDeploymentConfigWithActions configWithActions2 =
        CloudEdgeDeploymentConfigWithActions.newBuilder()
            .setCloudEdgeDeploymentConfig(config2)
            .addAllowedActions(Action.ACTION_VIEW)
            .build();

    GetCloudEdgeDeploymentConfigsResponse managerResponse =
        GetCloudEdgeDeploymentConfigsResponse.newBuilder()
            .addAllCloudEdgeDeployments(Arrays.asList(config1, config2))
            .addAllCloudEdgeDeploymentsWithActions(
                Arrays.asList(configWithActions1, configWithActions2))
            .build();

    doReturn(managerResponse)
        .when(configManager)
        .getCloudEdgeDeploymentConfigsWithActions(any(), any(), any());

    configService.getCloudEdgeDeploymentConfigs(request, responseObserver);
    verify(responseObserver, times(1)).onNext(managerResponse);
    verify(responseObserver, times(1)).onCompleted();
  }

  @Test
  void testCancelCloudEdgeDeploymentConfigAction() {
    CancelCloudEdgeDeploymentConfigActionRequest request =
        CancelCloudEdgeDeploymentConfigActionRequest.newBuilder()
            .setId("test-id")
            .setAccessType(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
            .build();

    StreamObserver<CancelCloudEdgeDeploymentConfigActionResponse> responseObserver =
        mock(StreamObserver.class);

    // Test case 1: When an exception occurs during cancellation
    doThrow(new RuntimeException("Cancellation failed"))
        .when(configManager)
        .cancelCloudEdgeDeploymentConfigAction(any(), any());

    configService.cancelCloudEdgeDeploymentConfigAction(request, responseObserver);
    verify(responseObserver, times(1))
        .onError(argThat(err -> err.getMessage().contains("Cancellation failed")));

    // Test case 2: Successful cancellation
    doNothing().when(configManager).cancelCloudEdgeDeploymentConfigAction(any(), any());

    configService.cancelCloudEdgeDeploymentConfigAction(request, responseObserver);
    verify(responseObserver, times(1))
        .onNext(CancelCloudEdgeDeploymentConfigActionResponse.getDefaultInstance());
    verify(responseObserver, times(1)).onCompleted();
  }

  @Test
  void testGetSharedConfigMetadata() {
    GetSharedConfigMetadataRequest request =
        GetSharedConfigMetadataRequest.newBuilder()
            .setReadAccess(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
            .build();

    StreamObserver<GetSharedConfigMetadataResponse> responseObserver = mock(StreamObserver.class);

    // Test case 1: When an exception occurs during retrieval
    doThrow(new RuntimeException("Metadata retrieval failed"))
        .when(configManager)
        .getSharedConfigMetadata(any(), any());

    configService.getSharedConfigMetadata(request, responseObserver);
    verify(responseObserver, times(1))
        .onError(argThat(err -> err.getMessage().contains("Metadata retrieval failed")));

    // Test case 2: Successful retrieval
    SharedConfigMetadata metadata =
        SharedConfigMetadata.newBuilder()
            .putClusterConfigDetails(
                "name",
                ConfigValueDescriptor.newBuilder()
                    .setDescription("Cluster name")
                    .setDisplayName("Cluster Name")
                    .build())
            .build();

    doReturn(metadata).when(configManager).getSharedConfigMetadata(any(), any());

    configService.getSharedConfigMetadata(request, responseObserver);
    verify(responseObserver, times(1))
        .onNext(
            GetSharedConfigMetadataResponse.newBuilder().setSharedConfigMetadata(metadata).build());
    verify(responseObserver, times(1)).onCompleted();
  }

  @Test
  void testRemoveCloudEdgeDeploymentConfig() {
    RemoveCloudEdgeDeploymentConfigRequest request =
        RemoveCloudEdgeDeploymentConfigRequest.newBuilder()
            .setId("test-id")
            .setAccessType(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
            .build();

    StreamObserver<RemoveCloudEdgeDeploymentConfigResponse> responseObserver =
        mock(StreamObserver.class);

    // Test case 1: When an exception occurs during removal request
    doThrow(new RuntimeException("Removal request failed"))
        .when(configManager)
        .removeCloudEdgeDeploymentConfig(any(), any());

    configService.removeCloudEdgeDeploymentConfig(request, responseObserver);
    verify(responseObserver, times(1))
        .onError(argThat(err -> err.getMessage().contains("Removal request failed")));

    // Test case 2: Successful removal request
    doNothing().when(configManager).removeCloudEdgeDeploymentConfig(any(), any());

    configService.removeCloudEdgeDeploymentConfig(request, responseObserver);
    verify(responseObserver, times(1))
        .onNext(RemoveCloudEdgeDeploymentConfigResponse.getDefaultInstance());
    verify(responseObserver, times(1)).onCompleted();
  }
}
