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
import ai.traceable.anomaly.config.service.v1.detector.GetAllScopedAnomalyDetectionConfigsRequest;
import ai.traceable.anomaly.config.service.v1.detector.GetAllScopedAnomalyDetectionConfigsResponse;
import ai.traceable.anomaly.config.service.v1.detector.GetScopedAnomalyDetectionConfigRequest;
import ai.traceable.anomaly.config.service.v1.detector.GetScopedAnomalyDetectionConfigResponse;
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
  void testGetScopedTrainingConfig() {
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
  void testGetAllScopedTrainingConfig() {
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
  void testUpdateScopedTrainingConfig() {
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
}
