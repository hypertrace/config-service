package ai.traceable.anomaly.config.service.apidef;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;

import ai.traceable.anomaly.config.service.apidef.trainer.ApiDefinitionTrainerConfigManager;
import ai.traceable.anomaly.config.service.apidef.trainer.ApiDefinitionTrainerConfigServiceValidator;
import ai.traceable.anomaly.config.service.apidef.trainer.ConfigManager;
import ai.traceable.anomaly.config.service.v1.apidef.*;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

class ApiDefinitionConfigServiceImplTest {
  private final ApiDefinitionTrainerConfigServiceValidator validator =
      mock(ApiDefinitionTrainerConfigServiceValidator.class);
  private final ConfigManager configManager = mock(ApiDefinitionTrainerConfigManager.class);
  private final ApiDefinitionConfigServiceImpl apiDefinitionTrainerConfigService =
      new ApiDefinitionConfigServiceImpl(validator, configManager);

  @Test
  void testGetApiDefinitionTrainerConfig() {
    GetApiDefinitionTrainerConfigsRequest request =
        GetApiDefinitionTrainerConfigsRequest.newBuilder().build();
    StreamObserver<GetApiDefinitionTrainerConfigsResponse> responseObserver =
        mock(StreamObserver.class);

    doReturn(Status.INVALID_ARGUMENT).when(validator).validate(request);
    apiDefinitionTrainerConfigService.getApiDefinitionTrainerConfigs(request, responseObserver);
    verify(responseObserver, times(1))
        .onError(
            argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));

    doReturn(Status.OK).when(validator).validate(request);
    doThrow(new RuntimeException("msg"))
        .when(configManager)
        .getApiDefinitionTrainerConfig(any(), any());
    apiDefinitionTrainerConfigService.getApiDefinitionTrainerConfigs(request, responseObserver);
    verify(responseObserver, times(1)).onError(argThat(err -> err.getMessage().equals("msg")));

    GetApiDefinitionTrainerConfigsResponse response =
        GetApiDefinitionTrainerConfigsResponse.newBuilder()
            .setApiDefinitionTrainerConfig(ApiDefinitionTrainerConfig.newBuilder().build())
            .build();
    doReturn(response.getApiDefinitionTrainerConfig())
        .when(configManager)
        .getApiDefinitionTrainerConfig(any(), any());
    apiDefinitionTrainerConfigService.getApiDefinitionTrainerConfigs(request, responseObserver);
    verify(responseObserver, times(1)).onNext(response);
    verify(responseObserver, times(1)).onCompleted();
  }

  @Test
  void testUpdateApiDefinitionTrainerConfig() {
    UpdateApiDefinitionTrainerConfigsRequest request =
        UpdateApiDefinitionTrainerConfigsRequest.newBuilder().build();
    StreamObserver<UpdateApiDefinitionTrainerConfigsResponse> responseObserver =
        mock(StreamObserver.class);

    doReturn(Status.INVALID_ARGUMENT).when(validator).validate(request);
    apiDefinitionTrainerConfigService.updateApiDefinitionTrainerConfigs(request, responseObserver);
    verify(responseObserver, times(1))
        .onError(
            argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));

    doReturn(Status.OK).when(validator).validate(request);
    doThrow(new RuntimeException("msg"))
        .when(configManager)
        .updateApiDefinitionTrainerConfig(any(), any(), any());
    apiDefinitionTrainerConfigService.updateApiDefinitionTrainerConfigs(request, responseObserver);
    verify(responseObserver, times(1)).onError(argThat(err -> err.getMessage().equals("msg")));

    UpdateApiDefinitionTrainerConfigsResponse response =
        UpdateApiDefinitionTrainerConfigsResponse.newBuilder()
            .setApiDefinitionTrainerConfig(ApiDefinitionTrainerConfig.newBuilder().build())
            .build();
    doReturn(response.getApiDefinitionTrainerConfig())
        .when(configManager)
        .updateApiDefinitionTrainerConfig(any(), any(), any());
    apiDefinitionTrainerConfigService.updateApiDefinitionTrainerConfigs(request, responseObserver);
    verify(responseObserver, times(1)).onNext(response);
    verify(responseObserver, times(1)).onCompleted();
  }
}
