package ai.traceable.anomaly.config.service.trainer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import ai.traceable.anomaly.config.service.trainer.trainingconfig.TrainingConfigManager;
import ai.traceable.anomaly.config.service.trainer.trainingconfig.TrainingConfigManagerImpl;
import ai.traceable.anomaly.config.service.trainer.trainingconfig.TrainingConfigValidator;
import ai.traceable.anomaly.config.service.v1.trainer.GetAllScopedTrainingConfigsRequest;
import ai.traceable.anomaly.config.service.v1.trainer.GetAllScopedTrainingConfigsResponse;
import ai.traceable.anomaly.config.service.v1.trainer.GetScopedTrainingConfigRequest;
import ai.traceable.anomaly.config.service.v1.trainer.GetScopedTrainingConfigResponse;
import ai.traceable.anomaly.config.service.v1.trainer.ScopedTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.UpdateScopedTrainingConfigRequest;
import ai.traceable.anomaly.config.service.v1.trainer.UpdateScopedTrainingConfigResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

public class TrainerConfigServiceImplTest {
  private final TrainingConfigValidator validator = mock(TrainingConfigValidator.class);
  private final TrainingConfigManager configManager = mock(TrainingConfigManagerImpl.class);
  private final TrainerConfigServiceImpl trainerConfigService =
      new TrainerConfigServiceImpl(validator, configManager);

  @Test
  void testGetScopedTrainingConfig() {
    GetScopedTrainingConfigRequest request = GetScopedTrainingConfigRequest.newBuilder().build();
    StreamObserver<GetScopedTrainingConfigResponse> responseObserver = mock(StreamObserver.class);

    doReturn(Status.INVALID_ARGUMENT).when(validator).validate(request);
    trainerConfigService.getScopedTrainingConfig(request, responseObserver);
    verify(responseObserver, times(1))
        .onError(
            argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));

    doReturn(Status.OK).when(validator).validate(request);
    doThrow(new RuntimeException("msg"))
        .when(configManager)
        .getScopedTrainingConfig(any(), any(), any());
    trainerConfigService.getScopedTrainingConfig(request, responseObserver);
    verify(responseObserver, times(1)).onError(argThat(err -> err.getMessage().equals("msg")));

    GetScopedTrainingConfigResponse response =
        GetScopedTrainingConfigResponse.newBuilder()
            .setScopedTrainingConfig(ScopedTrainingConfig.newBuilder().build())
            .build();
    doReturn(response.getScopedTrainingConfig())
        .when(configManager)
        .getScopedTrainingConfig(any(), any(), any());
    trainerConfigService.getScopedTrainingConfig(request, responseObserver);
    verify(responseObserver, times(1)).onNext(response);
    verify(responseObserver, times(1)).onCompleted();
  }

  @Test
  void testGetAllScopedTrainingConfig() {
    GetAllScopedTrainingConfigsRequest request =
        GetAllScopedTrainingConfigsRequest.newBuilder().build();
    StreamObserver<GetAllScopedTrainingConfigsResponse> responseObserver =
        mock(StreamObserver.class);

    doThrow(new RuntimeException("msg"))
        .when(configManager)
        .getAllScopedTrainingConfig(any(), any());
    trainerConfigService.getAllScopedTrainingConfigs(request, responseObserver);
    verify(responseObserver, times(1)).onError(argThat(err -> err.getMessage().equals("msg")));

    GetAllScopedTrainingConfigsResponse response =
        GetAllScopedTrainingConfigsResponse.newBuilder()
            .addScopedTrainingConfigs(ScopedTrainingConfig.newBuilder().build())
            .build();
    doReturn(response.getScopedTrainingConfigsList())
        .when(configManager)
        .getAllScopedTrainingConfig(any(), any());
    trainerConfigService.getAllScopedTrainingConfigs(request, responseObserver);
    verify(responseObserver, times(1)).onNext(response);
    verify(responseObserver, times(1)).onCompleted();
  }

  @Test
  void testUpdateScopedTrainingConfig() {
    UpdateScopedTrainingConfigRequest request =
        UpdateScopedTrainingConfigRequest.newBuilder().build();
    StreamObserver<UpdateScopedTrainingConfigResponse> responseObserver =
        mock(StreamObserver.class);

    doReturn(Status.INVALID_ARGUMENT).when(validator).validate(request);
    trainerConfigService.updateScopedTrainingConfig(request, responseObserver);
    verify(responseObserver, times(1))
        .onError(
            argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));

    doReturn(Status.OK).when(validator).validate(request);
    doThrow(new RuntimeException("msg"))
        .when(configManager)
        .updateScopedTrainingConfig(any(), any());
    trainerConfigService.updateScopedTrainingConfig(request, responseObserver);
    verify(responseObserver, times(1)).onError(argThat(err -> err.getMessage().equals("msg")));

    UpdateScopedTrainingConfigResponse response =
        UpdateScopedTrainingConfigResponse.newBuilder()
            .setScopedTrainingConfig(ScopedTrainingConfig.newBuilder().build())
            .build();
    doReturn(response.getScopedTrainingConfig())
        .when(configManager)
        .updateScopedTrainingConfig(any(), any());
    trainerConfigService.updateScopedTrainingConfig(request, responseObserver);
    verify(responseObserver, times(1)).onNext(response);
    verify(responseObserver, times(1)).onCompleted();
  }
}
