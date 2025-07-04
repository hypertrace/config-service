package ai.traceable.anomaly.config.service.global;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
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
import ai.traceable.anomaly.config.service.global.status.GlobalAnomalyConfigStatusManager;
import ai.traceable.anomaly.config.service.global.validator.AnomalyGlobalConfigServiceValidator;
import ai.traceable.anomaly.config.service.global.version.RuleVersionManager;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.RuleType;
import ai.traceable.anomaly.config.service.v1.RuleVersionType;
import ai.traceable.anomaly.config.service.v1.global.AvailableRuleVersions;
import ai.traceable.anomaly.config.service.v1.global.AvailableRuleVersionsFilter;
import ai.traceable.anomaly.config.service.v1.global.DeleteScopedAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.DeleteScopedAnomalyGlobalConfigStatusResponse;
import ai.traceable.anomaly.config.service.v1.global.GetAllScopedAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.GetAllScopedAnomalyGlobalConfigStatusResponse;
import ai.traceable.anomaly.config.service.v1.global.GetAllUnresolvedScopedAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.GetAllUnresolvedScopedAnomalyGlobalConfigStatusResponse;
import ai.traceable.anomaly.config.service.v1.global.GetAnomalyRuleInfosRequest;
import ai.traceable.anomaly.config.service.v1.global.GetAnomalyRuleInfosResponse;
import ai.traceable.anomaly.config.service.v1.global.GetAvailableRuleVersionsRequest;
import ai.traceable.anomaly.config.service.v1.global.GetAvailableRuleVersionsResponse;
import ai.traceable.anomaly.config.service.v1.global.GetScopedAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.GetScopedAnomalyGlobalConfigStatusResponse;
import ai.traceable.anomaly.config.service.v1.global.GetUnresolvedScopedAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.GetUnresolvedScopedAnomalyGlobalConfigStatusResponse;
import ai.traceable.anomaly.config.service.v1.global.ScopedAnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.global.ScopedAnomalyConfigStatusChange;
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
  private final GlobalAnomalyConfigStatusManager anomalyConfigStatusManager =
      mock(GlobalAnomalyConfigStatusManager.class);
  private final RuleInfoManager ruleInfoManager = mock(AnomalyRuleInfoManagerImpl.class);
  private final RuleVersionManager ruleVersionManager = mock(RuleVersionManager.class);
  private final AnomalyGlobalConfigServiceImpl globalConfigService =
      new AnomalyGlobalConfigServiceImpl(
          globalValidator, anomalyConfigStatusManager, ruleInfoManager, ruleVersionManager);

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
  void test_getUnresolvedScopedAnomalyGlobalConfigStatus() {
    GetUnresolvedScopedAnomalyGlobalConfigStatusRequest getRequest =
        GetUnresolvedScopedAnomalyGlobalConfigStatusRequest.newBuilder().build();
    StreamObserver<GetUnresolvedScopedAnomalyGlobalConfigStatusResponse> responseObserver =
        mock(StreamObserver.class);

    doReturn(Status.INVALID_ARGUMENT).when(globalValidator).validate(getRequest);
    globalConfigService.getUnresolvedScopedAnomalyGlobalConfigStatus(getRequest, responseObserver);
    verify(responseObserver, times(1))
        .onError(
            argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));

    doReturn(Status.OK).when(globalValidator).validate(getRequest);

    doThrow(new RuntimeException("msg"))
        .when(anomalyConfigStatusManager)
        .getUnresolvedScopedAnomalyConfigStatus(any(), any());
    globalConfigService.getUnresolvedScopedAnomalyGlobalConfigStatus(getRequest, responseObserver);
    verify(responseObserver, times(1)).onError(argThat(err -> err.getMessage().equals("msg")));

    GetUnresolvedScopedAnomalyGlobalConfigStatusResponse response =
        GetUnresolvedScopedAnomalyGlobalConfigStatusResponse.newBuilder()
            .setScopedConfig(ScopedAnomalyConfigStatusChange.newBuilder().build())
            .build();
    doReturn(response.getScopedConfig())
        .when(anomalyConfigStatusManager)
        .getUnresolvedScopedAnomalyConfigStatus(any(), any());
    globalConfigService.getUnresolvedScopedAnomalyGlobalConfigStatus(getRequest, responseObserver);
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
        .getAllScopedAnomalyConfigStatusConfigs(any(), any());
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
        .getAllScopedAnomalyConfigStatusConfigs(any(), any());
    globalConfigService.getAllScopedAnomalyGlobalConfigStatus(getRequest, responseObserver);
    verify(responseObserver, times(1)).onNext(response);
    verify(responseObserver, times(1)).onCompleted();
  }

  @Test
  void test_getAllUnresolvedScopedAnomalyGlobalConfigStatus() {
    GetAllUnresolvedScopedAnomalyGlobalConfigStatusRequest getRequest =
        GetAllUnresolvedScopedAnomalyGlobalConfigStatusRequest.newBuilder().build();
    StreamObserver<GetAllUnresolvedScopedAnomalyGlobalConfigStatusResponse> responseObserver =
        mock(StreamObserver.class);

    doThrow(new RuntimeException("msg"))
        .when(anomalyConfigStatusManager)
        .getAllUnresolvedScopedAnomalyConfigStatusConfigs(any(), any());
    globalConfigService.getAllUnresolvedScopedAnomalyGlobalConfigStatus(
        getRequest, responseObserver);
    verify(responseObserver, times(1)).onError(argThat(err -> err.getMessage().equals("msg")));

    List<ScopedAnomalyConfigStatusChange> scopedConfigs =
        List.of(ScopedAnomalyConfigStatusChange.newBuilder().build());
    GetAllUnresolvedScopedAnomalyGlobalConfigStatusResponse response =
        GetAllUnresolvedScopedAnomalyGlobalConfigStatusResponse.newBuilder()
            .addAllScopedConfigs(scopedConfigs)
            .build();
    doReturn(response.getScopedConfigsList())
        .when(anomalyConfigStatusManager)
        .getAllUnresolvedScopedAnomalyConfigStatusConfigs(any(), any());
    globalConfigService.getAllUnresolvedScopedAnomalyGlobalConfigStatus(
        getRequest, responseObserver);
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

    when(ruleInfoManager.getAnomalyRuleInfos(any(), any(), any(), any(), anyBoolean()))
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

  @Test
  @DisplayName("Delete scoped anomaly global config status")
  void deleteScopedAnomalyGlobalConfigStatus() {
    StreamObserver<DeleteScopedAnomalyGlobalConfigStatusResponse> responseStreamObserver =
        mock(StreamObserver.class);
    when(globalValidator.validate(
            DeleteScopedAnomalyGlobalConfigStatusRequest.getDefaultInstance()))
        .thenReturn(Status.OK);
    globalConfigService.deleteScopedAnomalyGlobalConfigStatus(
        DeleteScopedAnomalyGlobalConfigStatusRequest.getDefaultInstance(), responseStreamObserver);

    verify(responseStreamObserver, times(1))
        .onNext(DeleteScopedAnomalyGlobalConfigStatusResponse.newBuilder().build());
    verify(responseStreamObserver, times(1)).onCompleted();
  }

  @Test
  void test_getAvailableRuleVersions() {
    AvailableRuleVersionsFilter filter =
        AvailableRuleVersionsFilter.newBuilder()
            .addVersionTypes(RuleVersionType.RULE_VERSION_TYPE_STABLE)
            .build();
    StreamObserver<GetAvailableRuleVersionsResponse> responseObserver = mock(StreamObserver.class);
    when(globalValidator.validate(
            GetAvailableRuleVersionsRequest.newBuilder()
                .setRuleType(RuleType.RULE_TYPE_WEB_APPLICATION)
                .setFilter(filter)
                .build()))
        .thenReturn(Status.OK);
    doReturn(
            AvailableRuleVersions.newBuilder()
                .setRuleType(RuleType.RULE_TYPE_WEB_APPLICATION)
                .build())
        .when(ruleVersionManager)
        .getAvailableRuleVersions(RuleType.RULE_TYPE_WEB_APPLICATION, filter);
    globalConfigService.getAvailableRuleVersions(
        GetAvailableRuleVersionsRequest.newBuilder()
            .setRuleType(RuleType.RULE_TYPE_WEB_APPLICATION)
            .setFilter(filter)
            .build(),
        responseObserver);
    verify(responseObserver, times(1)).onNext(any(GetAvailableRuleVersionsResponse.class));
    verify(responseObserver, times(1)).onCompleted();
  }
}
