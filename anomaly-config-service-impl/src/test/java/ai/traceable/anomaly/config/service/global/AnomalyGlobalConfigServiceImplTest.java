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

import ai.traceable.anomaly.config.service.common.AnomalyConfigScopeUtils;
import ai.traceable.anomaly.config.service.global.ruleinfo.AnomalyRuleInfoManagerImpl;
import ai.traceable.anomaly.config.service.global.ruleinfo.RuleInfoManager;
import ai.traceable.anomaly.config.service.global.status.AnomalyGlobalConfigStatusManager;
import ai.traceable.anomaly.config.service.global.status.GlobalAnomalyConfigStatusManager;
import ai.traceable.anomaly.config.service.global.validator.AnomalyGlobalConfigServiceValidator;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.global.GetAllScopedAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.GetAllScopedAnomalyGlobalConfigStatusResponse;
import ai.traceable.anomaly.config.service.v1.global.GetAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.GetAnomalyGlobalConfigStatusResponse;
import ai.traceable.anomaly.config.service.v1.global.GetAnomalyRuleInfosRequest;
import ai.traceable.anomaly.config.service.v1.global.GetAnomalyRuleInfosResponse;
import ai.traceable.anomaly.config.service.v1.global.GetScopedAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.GetScopedAnomalyGlobalConfigStatusResponse;
import ai.traceable.anomaly.config.service.v1.global.ScopedAnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.global.ScopedAnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.global.UpdateAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.UpdateAnomalyGlobalConfigStatusResponse;
import ai.traceable.anomaly.config.service.v1.global.UpdateScopedAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.UpdateScopedAnomalyGlobalConfigStatusResponse;
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
  private final GlobalAnomalyConfigStatusManager anomalyConfigStatusManager =
      mock(GlobalAnomalyConfigStatusManager.class);
  private final RuleInfoManager ruleInfoManager = mock(AnomalyRuleInfoManagerImpl.class);
  private final AnomalyGlobalConfigServiceImpl globalConfigService =
      new AnomalyGlobalConfigServiceImpl(
          globalValidator, configStatusManager, anomalyConfigStatusManager, ruleInfoManager);

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
  void test_getScopedAnomalyGlobalConfigStatus() {
    GetScopedAnomalyGlobalConfigStatusRequest getRequest =
        GetScopedAnomalyGlobalConfigStatusRequest.newBuilder().build();
    StreamObserver<GetScopedAnomalyGlobalConfigStatusResponse> responseObserver =
        mock(StreamObserver.class);

    doReturn(Status.INVALID_ARGUMENT).when(globalValidator).validate(getRequest);
    globalConfigService.getScopedAnomalyGlobalConfigStatus(getRequest, responseObserver);
    verify(responseObserver, times(1))
        .onError(
            argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));

    doReturn(Status.OK).when(globalValidator).validate(getRequest);

    doThrow(new RuntimeException("msg"))
        .when(anomalyConfigStatusManager)
        .getScopedAnomalyConfigStatus(any(), any());
    globalConfigService.getScopedAnomalyGlobalConfigStatus(getRequest, responseObserver);
    verify(responseObserver, times(1)).onError(argThat(err -> err.getMessage().equals("msg")));

    GetScopedAnomalyGlobalConfigStatusResponse response =
        GetScopedAnomalyGlobalConfigStatusResponse.newBuilder()
            .setScopedConfig(ScopedAnomalyConfigStatus.newBuilder().build())
            .build();
    doReturn(response.getScopedConfig())
        .when(anomalyConfigStatusManager)
        .getScopedAnomalyConfigStatus(any(), any());
    globalConfigService.getScopedAnomalyGlobalConfigStatus(getRequest, responseObserver);
    verify(responseObserver, times(1)).onNext(response);
    verify(responseObserver, times(1)).onCompleted();
  }

  @Test
  void test_getAllScopedAnomalyGlobalConfigStatus() {
    GetAllScopedAnomalyGlobalConfigStatusRequest getRequest =
        GetAllScopedAnomalyGlobalConfigStatusRequest.newBuilder().build();
    StreamObserver<GetAllScopedAnomalyGlobalConfigStatusResponse> responseObserver =
        mock(StreamObserver.class);

    doThrow(new RuntimeException("msg"))
        .when(anomalyConfigStatusManager)
        .getAllScopedAnomalyConfigStatusConfigs(any());
    globalConfigService.getAllScopedAnomalyGlobalConfigStatus(getRequest, responseObserver);
    verify(responseObserver, times(1)).onError(argThat(err -> err.getMessage().equals("msg")));

    List<ScopedAnomalyConfigStatus> scopedConfigs =
        List.of(
            ScopedAnomalyConfigStatus.newBuilder().build(),
            ScopedAnomalyConfigStatus.newBuilder()
                .setConfigScope(new AnomalyConfigScopeUtils().getDefaultCustomerConfigScope())
                .build());
    GetAllScopedAnomalyGlobalConfigStatusResponse response =
        GetAllScopedAnomalyGlobalConfigStatusResponse.newBuilder()
            .addAllScopedConfigs(scopedConfigs)
            .build();
    doReturn(response.getScopedConfigsList())
        .when(anomalyConfigStatusManager)
        .getAllScopedAnomalyConfigStatusConfigs(any());
    globalConfigService.getAllScopedAnomalyGlobalConfigStatus(getRequest, responseObserver);
    verify(responseObserver, times(1)).onNext(response);
    verify(responseObserver, times(1)).onCompleted();
  }

  @Test
  void test_updateScopedAnomalyGlobalConfigStatus() {
    UpdateScopedAnomalyGlobalConfigStatusRequest updateRequest =
        UpdateScopedAnomalyGlobalConfigStatusRequest.newBuilder().build();
    StreamObserver<UpdateScopedAnomalyGlobalConfigStatusResponse> responseObserver =
        mock(StreamObserver.class);

    doReturn(Status.INVALID_ARGUMENT).when(globalValidator).validate(updateRequest);
    globalConfigService.updateScopedAnomalyGlobalConfigStatus(updateRequest, responseObserver);
    verify(responseObserver, times(1))
        .onError(
            argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));

    doReturn(Status.OK).when(globalValidator).validate(updateRequest);

    doThrow(new RuntimeException("msg"))
        .when(anomalyConfigStatusManager)
        .updateScopedAnomalyConfigStatus(any(), any());
    globalConfigService.updateScopedAnomalyGlobalConfigStatus(updateRequest, responseObserver);
    verify(responseObserver, times(1)).onError(argThat(err -> err.getMessage().equals("msg")));

    UpdateScopedAnomalyGlobalConfigStatusResponse response =
        UpdateScopedAnomalyGlobalConfigStatusResponse.newBuilder()
            .setScopedConfig(ScopedAnomalyConfigStatusChange.newBuilder().build())
            .build();
    doReturn(response.getScopedConfig())
        .when(anomalyConfigStatusManager)
        .updateScopedAnomalyConfigStatus(any(), any());
    globalConfigService.updateScopedAnomalyGlobalConfigStatus(updateRequest, responseObserver);
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
