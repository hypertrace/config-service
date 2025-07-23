package ai.traceable.cloud.bot.deployment.config.service.v1;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.cloud.bot.deployment.config.service.v1.manager.CloudBotDeploymentConfigManager;
import io.grpc.stub.StreamObserver;
import java.util.Arrays;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CloudBotDeploymentConfigServiceImplTest {
  private static final String TENANT_ID = "tenant-cloud-bot-test";
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);

  private CloudBotDeploymentConfigManager cloudBotDeploymentConfigManager;
  private CloudBotDeploymentConfigServiceImpl cloudBotDeploymentConfigService;

  @BeforeEach
  void setup() {
    cloudBotDeploymentConfigManager = mock(CloudBotDeploymentConfigManager.class);
    cloudBotDeploymentConfigService =
        new CloudBotDeploymentConfigServiceImpl(cloudBotDeploymentConfigManager);
  }

  @Test
  void createCloudBotDeploymentConfig() {
    CloudBotDeploymentConfigInput input = mock(CloudBotDeploymentConfigInput.class);
    CloudBotDeploymentConfig config = mock(CloudBotDeploymentConfig.class);
    CreateCloudBotDeploymentConfigRequest request =
        CreateCloudBotDeploymentConfigRequest.newBuilder()
            .setCloudBotDeploymentConfigInput(input)
            .build();

    when(cloudBotDeploymentConfigManager.createCloudBotDeploymentConfig(any(), eq(request)))
        .thenReturn(config);

    StreamObserver<CreateCloudBotDeploymentConfigResponse> responseStreamObserver =
        mock(StreamObserver.class);

    Runnable runnable =
        () ->
            cloudBotDeploymentConfigService.createCloudBotDeploymentConfig(
                request, responseStreamObserver);

    REQUEST_CONTEXT.run(runnable);

    verify(responseStreamObserver, times(1))
        .onNext(
            CreateCloudBotDeploymentConfigResponse.newBuilder()
                .setCloudBotDeployment(config)
                .build());
    verify(responseStreamObserver, times(1)).onCompleted();
  }

  @Test
  void updateCloudBotDeploymentConfig() {
    String id = "config-123";
    CloudBotDeploymentConfigInput input = mock(CloudBotDeploymentConfigInput.class);
    CloudBotDeploymentConfig config = mock(CloudBotDeploymentConfig.class);
    UpdateCloudBotDeploymentConfigRequest request =
        UpdateCloudBotDeploymentConfigRequest.newBuilder()
            .setId(id)
            .setCloudBotDeploymentConfigInput(input)
            .build();

    when(cloudBotDeploymentConfigManager.updateCloudBotDeploymentConfig(any(), eq(id), eq(request)))
        .thenReturn(config);

    StreamObserver<UpdateCloudBotDeploymentConfigResponse> responseStreamObserver =
        mock(StreamObserver.class);

    Runnable runnable =
        () ->
            cloudBotDeploymentConfigService.updateCloudBotDeploymentConfig(
                request, responseStreamObserver);

    REQUEST_CONTEXT.run(runnable);

    verify(responseStreamObserver, times(1))
        .onNext(
            UpdateCloudBotDeploymentConfigResponse.newBuilder()
                .setCloudBotDeployment(config)
                .build());
    verify(responseStreamObserver, times(1)).onCompleted();
  }

  @Test
  void updateCloudBotDeploymentStatus() {
    String id = "config-123";
    CloudBotDeploymentStatus status = mock(CloudBotDeploymentStatus.class);
    CaptchaProviderDetails captchaProviderDetails = mock(CaptchaProviderDetails.class);
    CloudBotDeploymentConfig config = mock(CloudBotDeploymentConfig.class);
    UpdateCloudBotDeploymentStatusRequest request =
        UpdateCloudBotDeploymentStatusRequest.newBuilder()
            .setId(id)
            .setCloudBotDeploymentStatus(status)
            .setCaptchaProviderDetails(captchaProviderDetails)
            .build();

    when(cloudBotDeploymentConfigManager.updateCloudBotDeploymentStatus(any(), eq(id), eq(request)))
        .thenReturn(config);

    StreamObserver<UpdateCloudBotDeploymentStatusResponse> responseStreamObserver =
        mock(StreamObserver.class);

    Runnable runnable =
        () ->
            cloudBotDeploymentConfigService.updateCloudBotDeploymentStatus(
                request, responseStreamObserver);

    REQUEST_CONTEXT.run(runnable);

    verify(responseStreamObserver, times(1))
        .onNext(
            UpdateCloudBotDeploymentStatusResponse.newBuilder()
                .setCloudBotDeployment(config)
                .build());
    verify(responseStreamObserver, times(1)).onCompleted();
  }

  @Test
  void deleteCloudBotDeploymentConfig() {
    String id = "config-123";
    DeleteCloudBotDeploymentConfigRequest request =
        DeleteCloudBotDeploymentConfigRequest.newBuilder().setId(id).build();

    StreamObserver<DeleteCloudBotDeploymentConfigResponse> responseStreamObserver =
        mock(StreamObserver.class);

    Runnable runnable =
        () ->
            cloudBotDeploymentConfigService.deleteCloudBotDeploymentConfig(
                request, responseStreamObserver);

    REQUEST_CONTEXT.run(runnable);

    verify(cloudBotDeploymentConfigManager, times(1)).deleteCloudBotDeploymentConfig(any(), eq(id));
    verify(responseStreamObserver, times(1))
        .onNext(DeleteCloudBotDeploymentConfigResponse.getDefaultInstance());
    verify(responseStreamObserver, times(1)).onCompleted();
  }

  @Test
  void getCloudBotDeploymentConfigs() {
    List<String> ids = Arrays.asList("config-1", "config-2");
    CloudBotDeploymentConfig config1 = mock(CloudBotDeploymentConfig.class);
    CloudBotDeploymentConfig config2 = mock(CloudBotDeploymentConfig.class);
    List<CloudBotDeploymentConfig> configs = Arrays.asList(config1, config2);
    GetCloudBotDeploymentConfigsRequest request =
        GetCloudBotDeploymentConfigsRequest.newBuilder().addAllIds(ids).build();

    when(cloudBotDeploymentConfigManager.getCloudBotDeploymentConfigs(any(), eq(ids)))
        .thenReturn(configs);

    StreamObserver<GetCloudBotDeploymentConfigsResponse> responseStreamObserver =
        mock(StreamObserver.class);

    Runnable runnable =
        () ->
            cloudBotDeploymentConfigService.getCloudBotDeploymentConfigs(
                request, responseStreamObserver);

    REQUEST_CONTEXT.run(runnable);

    verify(responseStreamObserver, times(1))
        .onNext(
            GetCloudBotDeploymentConfigsResponse.newBuilder()
                .addAllCloudBotDeployments(configs)
                .build());
    verify(responseStreamObserver, times(1)).onCompleted();
  }
}
