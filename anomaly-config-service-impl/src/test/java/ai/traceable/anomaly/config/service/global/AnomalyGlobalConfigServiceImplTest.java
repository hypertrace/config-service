package ai.traceable.anomaly.config.service.global;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.anomaly.config.service.global.ruleinfo.AnomalyRuleInfoManagerImpl;
import ai.traceable.anomaly.config.service.global.ruleinfo.RuleInfoManager;
import ai.traceable.anomaly.config.service.global.status.AnomalyGlobalConfigStatusManager;
import ai.traceable.anomaly.config.service.global.validator.AnomalyGlobalConfigServiceValidator;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.global.GetAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.GetAnomalyGlobalConfigStatusResponse;
import ai.traceable.anomaly.config.service.v1.global.GetAnomalyRuleInfosRequest;
import ai.traceable.anomaly.config.service.v1.global.GetAnomalyRuleInfosResponse;
import ai.traceable.anomaly.config.service.v1.global.UpdateAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.UpdateAnomalyGlobalConfigStatusResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class AnomalyGlobalConfigServiceImplTest {
  private final AnomalyGlobalConfigServiceValidator globalValidator =
      mock(AnomalyGlobalConfigServiceValidator.class);
  private final AnomalyGlobalConfigStatusManager configStatusManager =
      mock(AnomalyGlobalConfigStatusManager.class);
  private final RuleInfoManager ruleInfoManager = mock(AnomalyRuleInfoManagerImpl.class);
  private final AnomalyGlobalConfigServiceImpl globalConfigService =
      new AnomalyGlobalConfigServiceImpl(globalValidator, configStatusManager, ruleInfoManager);

  @Test
  void testGetAnomalyGlobalConfigStatus() {
    GetAnomalyGlobalConfigStatusRequest getRequest =
        GetAnomalyGlobalConfigStatusRequest.newBuilder().build();
    StreamObserver<GetAnomalyGlobalConfigStatusResponse> responseObserver =
        mock(StreamObserver.class);

    doReturn(Status.INVALID_ARGUMENT).when(globalValidator).validate(getRequest);
    globalConfigService.getAnomalyGlobalConfigStatus(getRequest, responseObserver);
    verify(responseObserver, times(1))
        .onError(
            argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));

    doReturn(Status.OK).when(globalValidator).validate(getRequest);

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
  void testUpdateAnomalyGlobalConfigStatus() {
    UpdateAnomalyGlobalConfigStatusRequest updateRequest =
        UpdateAnomalyGlobalConfigStatusRequest.newBuilder().build();
    StreamObserver<UpdateAnomalyGlobalConfigStatusResponse> responseObserver =
        mock(StreamObserver.class);

    doReturn(Status.INVALID_ARGUMENT).when(globalValidator).validate(updateRequest);
    globalConfigService.updateAnomalyGlobalConfigStatus(updateRequest, responseObserver);
    verify(responseObserver, times(1))
        .onError(
            argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));

    doReturn(Status.OK).when(globalValidator).validate(updateRequest);

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

  @Test
  @DisplayName("Should get all the anomaly rules infos if valid request")
  void getAnomalyRuleInfos() {
    StreamObserver<GetAnomalyRuleInfosResponse> responseStreamObserver = mock(StreamObserver.class);
    when(globalValidator.validate(GetAnomalyRuleInfosRequest.getDefaultInstance()))
        .thenReturn(Status.OK);
    globalConfigService.getAnomalyRuleInfos(
        GetAnomalyRuleInfosRequest.getDefaultInstance(), responseStreamObserver);

    verify(responseStreamObserver, times(1))
        .onNext(GetAnomalyRuleInfosResponse.newBuilder().build());
    verify(responseStreamObserver, times(1)).onCompleted();

    reset(responseStreamObserver);

    when(ruleInfoManager.getAnomalyRuleInfos(any(), any()))
        .thenReturn(List.of(AnomalyRuleInfo.getDefaultInstance()));

    globalConfigService.getAnomalyRuleInfos(
        GetAnomalyRuleInfosRequest.getDefaultInstance(), responseStreamObserver);
    verify(responseStreamObserver, times(1))
        .onNext(
            GetAnomalyRuleInfosResponse.newBuilder()
                .addRuleInfos(AnomalyRuleInfo.getDefaultInstance())
                .build());
    verify(responseStreamObserver, times(1)).onCompleted();
  }
}
