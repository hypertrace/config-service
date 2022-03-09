package ai.traceable.anomaly.config.service.trainer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.anomaly.config.service.trainer.trainingaction.TrainingActionManager;
import ai.traceable.anomaly.config.service.trainer.trainingaction.TrainingActionValidator;
import ai.traceable.anomaly.config.service.trainer.trainingconfig.TrainingConfigManager;
import ai.traceable.anomaly.config.service.trainer.trainingconfig.TrainingConfigManagerImpl;
import ai.traceable.anomaly.config.service.trainer.trainingconfig.TrainingConfigValidator;
import ai.traceable.anomaly.config.service.v1.trainer.DeleteScopedTrainingConfigRequest;
import ai.traceable.anomaly.config.service.v1.trainer.DeleteScopedTrainingConfigResponse;
import ai.traceable.anomaly.config.service.v1.trainer.GetAllScopedTrainingConfigsRequest;
import ai.traceable.anomaly.config.service.v1.trainer.GetAllScopedTrainingConfigsResponse;
import ai.traceable.anomaly.config.service.v1.trainer.GetAllUnresolvedScopedTrainingConfigRequest;
import ai.traceable.anomaly.config.service.v1.trainer.GetAllUnresolvedScopedTrainingConfigResponse;
import ai.traceable.anomaly.config.service.v1.trainer.GetScopedTrainingConfigRequest;
import ai.traceable.anomaly.config.service.v1.trainer.GetScopedTrainingConfigResponse;
import ai.traceable.anomaly.config.service.v1.trainer.GetUnresolvedScopedTrainingConfigRequest;
import ai.traceable.anomaly.config.service.v1.trainer.GetUnresolvedScopedTrainingConfigResponse;
import ai.traceable.anomaly.config.service.v1.trainer.ScopedTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.UpdateScopedTrainingConfigRequest;
import ai.traceable.anomaly.config.service.v1.trainer.UpdateScopedTrainingConfigResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.List;
import org.junit.jupiter.api.Test;

public class TrainerConfigServiceImplTest {
  private final TrainingConfigValidator validator = mock(TrainingConfigValidator.class);
  private final TrainingActionValidator actionValidator = mock(TrainingActionValidator.class);
  private final TrainingConfigManager configManager = mock(TrainingConfigManagerImpl.class);
  private final TrainingActionManager actionManager = mock(TrainingActionManager.class);
  private final TrainerConfigServiceImpl trainerConfigService =
      new TrainerConfigServiceImpl(validator, actionValidator, configManager, actionManager);

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
  void testGetUnresolvedScopedTrainingConfig() {
    GetUnresolvedScopedTrainingConfigRequest request =
        GetUnresolvedScopedTrainingConfigRequest.newBuilder().build();
    StreamObserver<GetUnresolvedScopedTrainingConfigResponse> responseObserver =
        mock(StreamObserver.class);

    when(validator.validate(request)).thenReturn(Status.INVALID_ARGUMENT);
    trainerConfigService.getUnresolvedScopedTrainingConfig(request, responseObserver);
    verify(responseObserver, times(1))
        .onError(
            argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));

    when(validator.validate(request)).thenReturn(Status.OK);
    when(configManager.getUnresolvedTrainingConfig(any(), any(), any()))
        .thenThrow(new RuntimeException("exception"));
    trainerConfigService.getUnresolvedScopedTrainingConfig(request, responseObserver);
    verify(responseObserver, times(1))
        .onError(argThat(err -> err.getMessage().equals("exception")));

    GetUnresolvedScopedTrainingConfigResponse response =
        GetUnresolvedScopedTrainingConfigResponse.newBuilder()
            .setScopedTrainingConfig(ScopedTrainingConfig.getDefaultInstance())
            .build();
    when(validator.validate(request)).thenReturn(Status.OK);
    doReturn(response.getScopedTrainingConfig())
        .when(configManager)
        .getUnresolvedTrainingConfig(any(), any(), any());
    trainerConfigService.getUnresolvedScopedTrainingConfig(request, responseObserver);
    verify(responseObserver, times(1)).onNext(response);
    verify(responseObserver, times(1)).onCompleted();
  }

  @Test
  void testGetAllUnresolvedScopedTrainingConfig() {
    GetAllUnresolvedScopedTrainingConfigRequest request =
        GetAllUnresolvedScopedTrainingConfigRequest.newBuilder().build();
    StreamObserver<GetAllUnresolvedScopedTrainingConfigResponse> responseObserver =
        mock(StreamObserver.class);

    doThrow(new RuntimeException("exception"))
        .when(configManager)
        .getAllUnresolvedTrainingConfig(any(), any());
    trainerConfigService.getAllUnresolvedScopedTrainingConfig(request, responseObserver);
    verify(responseObserver, times(1))
        .onError(argThat(err -> err.getMessage().equals("exception")));

    GetAllUnresolvedScopedTrainingConfigResponse response =
        GetAllUnresolvedScopedTrainingConfigResponse.newBuilder()
            .addAllScopedTrainingConfigs(List.of(ScopedTrainingConfig.getDefaultInstance()))
            .build();
    doReturn(response.getScopedTrainingConfigsList())
        .when(configManager)
        .getAllUnresolvedTrainingConfig(any(), any());
    trainerConfigService.getAllUnresolvedScopedTrainingConfig(request, responseObserver);
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

  @Test
  void testDeleteScopedTrainingConfig() {
    DeleteScopedTrainingConfigRequest request =
        DeleteScopedTrainingConfigRequest.newBuilder().build();
    StreamObserver<DeleteScopedTrainingConfigResponse> responseObserver =
        mock(StreamObserver.class);

    when(validator.validate(request)).thenReturn(Status.INVALID_ARGUMENT);
    trainerConfigService.deleteScopedTrainingConfig(request, responseObserver);
    verify(responseObserver, times(1))
        .onError(
            argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));

    when(validator.validate(request)).thenReturn(Status.OK);
    doThrow(new RuntimeException("exception"))
        .when(configManager)
        .deleteTrainingConfig(any(), any(), any());
    trainerConfigService.deleteScopedTrainingConfig(request, responseObserver);
    verify(responseObserver, times(1))
        .onError(argThat(err -> err.getMessage().equals("exception")));

    DeleteScopedTrainingConfigResponse response =
        DeleteScopedTrainingConfigResponse.newBuilder()
            .setDeletedScopedTrainingConfig(ScopedTrainingConfig.getDefaultInstance())
            .build();
    doReturn(response.getDeletedScopedTrainingConfig())
        .when(configManager)
        .deleteTrainingConfig(any(), any(), any());
    trainerConfigService.deleteScopedTrainingConfig(request, responseObserver);
    verify(responseObserver, times(1)).onNext(response);
    verify(responseObserver, times(1)).onCompleted();
  }
}
