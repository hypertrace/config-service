package ai.traceable.anomaly.config.service.global;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import ai.traceable.anomaly.config.service.global.status.AnomalyGlobalConfigStatusManager;
import ai.traceable.anomaly.config.service.global.status.AnomalyGlobalConfigStatusValidator;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.global.GetAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.GetAnomalyGlobalConfigStatusResponse;
import ai.traceable.anomaly.config.service.v1.global.UpdateAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.UpdateAnomalyGlobalConfigStatusResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

public class AnomalyGlobalConfigServiceImplTest {

  private final AnomalyGlobalConfigStatusValidator configValidator =
      mock(AnomalyGlobalConfigStatusValidator.class);
  private final AnomalyGlobalConfigStatusManager configStatusManager =
      mock(AnomalyGlobalConfigStatusManager.class);
  private final AnomalyGlobalConfigServiceImpl globalConfigService =
      new AnomalyGlobalConfigServiceImpl(configValidator, configStatusManager);

  @Test
  public void testGetAnomalyGlobalConfigStatus() {
    GetAnomalyGlobalConfigStatusRequest getRequest =
        GetAnomalyGlobalConfigStatusRequest.newBuilder().build();
    StreamObserver<GetAnomalyGlobalConfigStatusResponse> responseObserver =
        mock(StreamObserver.class);

    doReturn(Status.INVALID_ARGUMENT).when(configValidator).validate(getRequest);
    globalConfigService.getAnomalyGlobalConfigStatus(getRequest, responseObserver);
    verify(responseObserver, times(1))
        .onError(
            argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));

    doReturn(Status.OK).when(configValidator).validate(getRequest);

    doThrow(new RuntimeException("msg"))
        .when(configStatusManager)
        .getAnomalyConfigStatus(any(), any());
    globalConfigService.getAnomalyGlobalConfigStatus(getRequest, responseObserver);
    verify(responseObserver, times(1)).onError(argThat(err -> err.getMessage().equals("msg")));

    GetAnomalyGlobalConfigStatusResponse response =
        GetAnomalyGlobalConfigStatusResponse.newBuilder()
            .setConfigStatus(AnomalyConfigStatus.newBuilder().build())
            .build();
    doReturn(response.getConfigStatus())
        .when(configStatusManager)
        .getAnomalyConfigStatus(any(), any());
    globalConfigService.getAnomalyGlobalConfigStatus(getRequest, responseObserver);
    verify(responseObserver, times(1)).onNext(response);
    verify(responseObserver, times(1)).onCompleted();
  }

  @Test
  public void testUpdateAnomalyGlobalConfigStatus() {
    UpdateAnomalyGlobalConfigStatusRequest updateRequest =
        UpdateAnomalyGlobalConfigStatusRequest.newBuilder().build();
    StreamObserver<UpdateAnomalyGlobalConfigStatusResponse> responseObserver =
        mock(StreamObserver.class);

    doReturn(Status.INVALID_ARGUMENT).when(configValidator).validate(updateRequest);
    globalConfigService.updateAnomalyGlobalConfigStatus(updateRequest, responseObserver);
    verify(responseObserver, times(1))
        .onError(
            argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));

    doReturn(Status.OK).when(configValidator).validate(updateRequest);

    doThrow(new RuntimeException("msg"))
        .when(configStatusManager)
        .updateAnomalyConfigStatus(any(), any(), any());
    globalConfigService.updateAnomalyGlobalConfigStatus(updateRequest, responseObserver);
    verify(responseObserver, times(1)).onError(argThat(err -> err.getMessage().equals("msg")));

    UpdateAnomalyGlobalConfigStatusResponse response =
        UpdateAnomalyGlobalConfigStatusResponse.newBuilder()
            .setConfigStatus(AnomalyConfigStatusChange.newBuilder().build())
            .build();
    doReturn(response.getConfigStatus())
        .when(configStatusManager)
        .updateAnomalyConfigStatus(any(), any(), any());
    globalConfigService.updateAnomalyGlobalConfigStatus(updateRequest, responseObserver);
    verify(responseObserver, times(1)).onNext(response);
    verify(responseObserver, times(1)).onCompleted();
  }
}
