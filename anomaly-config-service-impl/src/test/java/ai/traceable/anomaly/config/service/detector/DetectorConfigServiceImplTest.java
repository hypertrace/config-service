package ai.traceable.anomaly.config.service.detector;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import ai.traceable.anomaly.config.service.detector.anomalydetection.AnomalyDetectionConfigManager;
import ai.traceable.anomaly.config.service.detector.anomalydetection.AnomalyDetectionConfigManagerImpl;
import ai.traceable.anomaly.config.service.detector.anomalydetection.AnomalyDetectionConfigValidator;
import ai.traceable.anomaly.config.service.v1.detector.DeleteScopedAnomalyDetectionConfigRequest;
import ai.traceable.anomaly.config.service.v1.detector.DeleteScopedAnomalyDetectionConfigResponse;
import ai.traceable.anomaly.config.service.v1.detector.GetAllGlobalResolvedScopedAnomalyDetectionConfigsRequest;
import ai.traceable.anomaly.config.service.v1.detector.GetAllGlobalResolvedScopedAnomalyDetectionConfigsResponse;
import ai.traceable.anomaly.config.service.v1.detector.GetAllScopedAnomalyDetectionConfigsRequest;
import ai.traceable.anomaly.config.service.v1.detector.GetAllScopedAnomalyDetectionConfigsResponse;
import ai.traceable.anomaly.config.service.v1.detector.GetAllUnresolvedScopedAnomalyDetectionConfigsRequest;
import ai.traceable.anomaly.config.service.v1.detector.GetAllUnresolvedScopedAnomalyDetectionConfigsResponse;
import ai.traceable.anomaly.config.service.v1.detector.GetGlobalResolvedScopedAnomalyDetectionConfigRequest;
import ai.traceable.anomaly.config.service.v1.detector.GetGlobalResolvedScopedAnomalyDetectionConfigResponse;
import ai.traceable.anomaly.config.service.v1.detector.GetScopedAnomalyDetectionConfigRequest;
import ai.traceable.anomaly.config.service.v1.detector.GetScopedAnomalyDetectionConfigResponse;
import ai.traceable.anomaly.config.service.v1.detector.GetUnresolvedScopedAnomalyDetectionConfigRequest;
import ai.traceable.anomaly.config.service.v1.detector.GetUnresolvedScopedAnomalyDetectionConfigResponse;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.UpdateScopedAnomalyDetectionConfigRequest;
import ai.traceable.anomaly.config.service.v1.detector.UpdateScopedAnomalyDetectionConfigResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

public class DetectorConfigServiceImplTest {
  private final AnomalyDetectionConfigValidator validator =
      mock(AnomalyDetectionConfigValidator.class);
  private final AnomalyDetectionConfigManager configManager =
      mock(AnomalyDetectionConfigManagerImpl.class);
  private final DetectorConfigServiceImpl detectorConfigService =
      new DetectorConfigServiceImpl(validator, configManager);

  @Test
  void testGetScopedDetectionConfig() {
    GetScopedAnomalyDetectionConfigRequest request =
        GetScopedAnomalyDetectionConfigRequest.newBuilder().build();
    StreamObserver<GetScopedAnomalyDetectionConfigResponse> responseObserver =
        mock(StreamObserver.class);

    doReturn(Status.INVALID_ARGUMENT).when(validator).validate(request);
    detectorConfigService.getScopedAnomalyDetectionConfig(request, responseObserver);
    verify(responseObserver, times(1))
        .onError(
            argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));

    doReturn(Status.OK).when(validator).validate(request);
    doThrow(new RuntimeException("msg"))
        .when(configManager)
        .getScopedAnomalyDetectionConfig(any(), any(), any());
    detectorConfigService.getScopedAnomalyDetectionConfig(request, responseObserver);
    verify(responseObserver, times(1)).onError(argThat(err -> err.getMessage().equals("msg")));

    GetScopedAnomalyDetectionConfigResponse response =
        GetScopedAnomalyDetectionConfigResponse.newBuilder()
            .setScopedAnomalyDetectionConfig(ScopedAnomalyDetectionConfig.newBuilder().build())
            .build();
    doReturn(response.getScopedAnomalyDetectionConfig())
        .when(configManager)
        .getScopedAnomalyDetectionConfig(any(), any(), any());
    detectorConfigService.getScopedAnomalyDetectionConfig(request, responseObserver);
    verify(responseObserver, times(1)).onNext(response);
    verify(responseObserver, times(1)).onCompleted();
  }

  @Test
  void testGetAllScopedDetectionConfig() {
    GetAllScopedAnomalyDetectionConfigsRequest request =
        GetAllScopedAnomalyDetectionConfigsRequest.newBuilder().build();
    StreamObserver<GetAllScopedAnomalyDetectionConfigsResponse> responseObserver =
        mock(StreamObserver.class);

    doThrow(new RuntimeException("msg"))
        .when(configManager)
        .getAllScopedAnomalyDetectionConfig(any(), any());
    detectorConfigService.getAllScopedAnomalyDetectionConfigs(request, responseObserver);
    verify(responseObserver, times(1)).onError(argThat(err -> err.getMessage().equals("msg")));

    GetAllScopedAnomalyDetectionConfigsResponse response =
        GetAllScopedAnomalyDetectionConfigsResponse.newBuilder()
            .addScopedAnomalyDetectionConfigs(ScopedAnomalyDetectionConfig.newBuilder().build())
            .build();
    doReturn(response.getScopedAnomalyDetectionConfigsList())
        .when(configManager)
        .getAllScopedAnomalyDetectionConfig(any(), any());
    detectorConfigService.getAllScopedAnomalyDetectionConfigs(request, responseObserver);
    verify(responseObserver, times(1)).onNext(response);
    verify(responseObserver, times(1)).onCompleted();
  }

  @Test
  void testGetGlobalResolvedScopedAnomalyDetectionConfig() {
    GetGlobalResolvedScopedAnomalyDetectionConfigRequest request =
        GetGlobalResolvedScopedAnomalyDetectionConfigRequest.newBuilder().build();
    StreamObserver<GetGlobalResolvedScopedAnomalyDetectionConfigResponse> responseObserver =
        mock(StreamObserver.class);

    doReturn(Status.INVALID_ARGUMENT).when(validator).validate(request);
    detectorConfigService.getGlobalResolvedScopedAnomalyDetectionConfig(request, responseObserver);
    verify(responseObserver, times(1))
        .onError(
            argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));

    doReturn(Status.OK).when(validator).validate(request);
    doThrow(new RuntimeException("msg"))
        .when(configManager)
        .getGlobalResolvedScopedAnomalyDetectionConfig(any(), any(), any());
    detectorConfigService.getGlobalResolvedScopedAnomalyDetectionConfig(request, responseObserver);
    verify(responseObserver, times(1)).onError(argThat(err -> err.getMessage().equals("msg")));

    GetGlobalResolvedScopedAnomalyDetectionConfigResponse response =
        GetGlobalResolvedScopedAnomalyDetectionConfigResponse.newBuilder()
            .setScopedAnomalyDetectionConfig(ScopedAnomalyDetectionConfig.getDefaultInstance())
            .build();
    doReturn(response.getScopedAnomalyDetectionConfig())
        .when(configManager)
        .getGlobalResolvedScopedAnomalyDetectionConfig(any(), any(), any());
    detectorConfigService.getGlobalResolvedScopedAnomalyDetectionConfig(request, responseObserver);
    verify(responseObserver, times(1)).onNext(response);
    verify(responseObserver, times(1)).onCompleted();
  }

  @Test
  void testGetAllGlobalResolvedScopedAnomalyDetectionConfig() {
    GetAllGlobalResolvedScopedAnomalyDetectionConfigsRequest request =
        GetAllGlobalResolvedScopedAnomalyDetectionConfigsRequest.newBuilder().build();
    StreamObserver<GetAllGlobalResolvedScopedAnomalyDetectionConfigsResponse> responseObserver =
        mock(StreamObserver.class);

    doThrow(new RuntimeException("msg"))
        .when(configManager)
        .getAllGlobalResolvedScopedAnomalyDetectionConfigs(any(), any());
    detectorConfigService.getAllGlobalResolvedScopedAnomalyDetectionConfigs(
        request, responseObserver);
    verify(responseObserver, times(1)).onError(argThat(err -> err.getMessage().equals("msg")));

    GetAllGlobalResolvedScopedAnomalyDetectionConfigsResponse response =
        GetAllGlobalResolvedScopedAnomalyDetectionConfigsResponse.newBuilder()
            .addScopedAnomalyDetectionConfigs(ScopedAnomalyDetectionConfig.newBuilder().build())
            .build();
    doReturn(response.getScopedAnomalyDetectionConfigsList())
        .when(configManager)
        .getAllGlobalResolvedScopedAnomalyDetectionConfigs(any(), any());
    detectorConfigService.getAllGlobalResolvedScopedAnomalyDetectionConfigs(
        request, responseObserver);
    verify(responseObserver, times(1)).onNext(response);
    verify(responseObserver, times(1)).onCompleted();
  }

  @Test
  void testUpdateScopedDetectionConfig() {
    UpdateScopedAnomalyDetectionConfigRequest request =
        UpdateScopedAnomalyDetectionConfigRequest.newBuilder().build();
    StreamObserver<UpdateScopedAnomalyDetectionConfigResponse> responseObserver =
        mock(StreamObserver.class);

    doReturn(Status.INVALID_ARGUMENT).when(validator).validate(request);
    detectorConfigService.updateScopedAnomalyDetectionConfig(request, responseObserver);
    verify(responseObserver, times(1))
        .onError(
            argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));

    doReturn(Status.OK).when(validator).validate(request);
    doThrow(new RuntimeException("msg"))
        .when(configManager)
        .updateScopedAnomalyDetectionConfig(any(), any());
    detectorConfigService.updateScopedAnomalyDetectionConfig(request, responseObserver);
    verify(responseObserver, times(1)).onError(argThat(err -> err.getMessage().equals("msg")));

    UpdateScopedAnomalyDetectionConfigResponse response =
        UpdateScopedAnomalyDetectionConfigResponse.newBuilder()
            .setScopedAnomalyDetectionConfig(ScopedAnomalyDetectionConfig.newBuilder().build())
            .build();
    doReturn(response.getScopedAnomalyDetectionConfig())
        .when(configManager)
        .updateScopedAnomalyDetectionConfig(any(), any());
    detectorConfigService.updateScopedAnomalyDetectionConfig(request, responseObserver);
    verify(responseObserver, times(1)).onNext(response);
    verify(responseObserver, times(1)).onCompleted();
  }

  @Test
  void testGetUnresolvedScopedDetectionConfig() {
    GetUnresolvedScopedAnomalyDetectionConfigRequest request =
        GetUnresolvedScopedAnomalyDetectionConfigRequest.newBuilder().build();
    StreamObserver<GetUnresolvedScopedAnomalyDetectionConfigResponse> responseObserver =
        mock(StreamObserver.class);

    doReturn(Status.INVALID_ARGUMENT).when(validator).validate(request);
    detectorConfigService.getUnresolvedScopedAnomalyDetectionConfig(request, responseObserver);
    verify(responseObserver, times(1))
        .onError(
            argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));

    doReturn(Status.OK).when(validator).validate(request);
    doThrow(new RuntimeException("msg"))
        .when(configManager)
        .getUnresolvedScopedAnomalyDetectionConfig(any(), any(), any());
    detectorConfigService.getUnresolvedScopedAnomalyDetectionConfig(request, responseObserver);
    verify(responseObserver, times(1)).onError(argThat(err -> err.getMessage().equals("msg")));

    GetUnresolvedScopedAnomalyDetectionConfigResponse response =
        GetUnresolvedScopedAnomalyDetectionConfigResponse.newBuilder()
            .setScopedAnomalyDetectionConfig(ScopedAnomalyDetectionConfig.getDefaultInstance())
            .build();
    doReturn(response.getScopedAnomalyDetectionConfig())
        .when(configManager)
        .getUnresolvedScopedAnomalyDetectionConfig(any(), any(), any());
    detectorConfigService.getUnresolvedScopedAnomalyDetectionConfig(request, responseObserver);
    verify(responseObserver, times(1)).onNext(response);
    verify(responseObserver, times(1)).onCompleted();
  }

  @Test
  void testGetAllUnresolvedScopedDetectionConfig() {
    GetAllUnresolvedScopedAnomalyDetectionConfigsRequest request =
        GetAllUnresolvedScopedAnomalyDetectionConfigsRequest.newBuilder().build();
    StreamObserver<GetAllUnresolvedScopedAnomalyDetectionConfigsResponse> responseObserver =
        mock(StreamObserver.class);

    doThrow(new RuntimeException("msg"))
        .when(configManager)
        .getAllUnresolvedScopedAnomalyDetectionConfigs(any(), any());
    detectorConfigService.getAllUnresolvedScopedAnomalyDetectionConfigs(request, responseObserver);
    verify(responseObserver, times(1)).onError(argThat(err -> err.getMessage().equals("msg")));

    GetAllUnresolvedScopedAnomalyDetectionConfigsResponse response =
        GetAllUnresolvedScopedAnomalyDetectionConfigsResponse.newBuilder()
            .addScopedAnomalyDetectionConfigs(ScopedAnomalyDetectionConfig.newBuilder().build())
            .build();
    doReturn(response.getScopedAnomalyDetectionConfigsList())
        .when(configManager)
        .getAllUnresolvedScopedAnomalyDetectionConfigs(any(), any());
    detectorConfigService.getAllUnresolvedScopedAnomalyDetectionConfigs(request, responseObserver);
    verify(responseObserver, times(1)).onNext(response);
    verify(responseObserver, times(1)).onCompleted();
  }

  @Test
  void testDeleteScopedDetectionConfig() {
    DeleteScopedAnomalyDetectionConfigRequest request =
        DeleteScopedAnomalyDetectionConfigRequest.newBuilder().build();
    StreamObserver<DeleteScopedAnomalyDetectionConfigResponse> responseObserver =
        mock(StreamObserver.class);

    doReturn(Status.INVALID_ARGUMENT).when(validator).validate(request);
    detectorConfigService.deleteScopedAnomalyDetectionConfig(request, responseObserver);
    verify(responseObserver, times(1))
        .onError(
            argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));

    doReturn(Status.OK).when(validator).validate(request);
    doThrow(new RuntimeException("msg"))
        .when(configManager)
        .deleteScopedAnomalyDetectionConfig(any(), any(), any());
    detectorConfigService.deleteScopedAnomalyDetectionConfig(request, responseObserver);
    verify(responseObserver, times(1)).onError(argThat(err -> err.getMessage().equals("msg")));

    ScopedAnomalyDetectionConfig deletedScopedAnomalyDetectionConfig =
        ScopedAnomalyDetectionConfig.getDefaultInstance();
    DeleteScopedAnomalyDetectionConfigResponse response =
        DeleteScopedAnomalyDetectionConfigResponse.newBuilder()
            .setDeletedScopedAnomalyDetectionConfig(deletedScopedAnomalyDetectionConfig)
            .build();
    doReturn(deletedScopedAnomalyDetectionConfig)
        .when(configManager)
        .deleteScopedAnomalyDetectionConfig(any(), any(), any());
    detectorConfigService.deleteScopedAnomalyDetectionConfig(request, responseObserver);
    verify(responseObserver, times(1)).onNext(response);
    verify(responseObserver, times(1)).onCompleted();
  }
}
