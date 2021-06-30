package ai.traceable.anomaly.config.service.modsec;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.anomaly.config.service.modsec.rules.ModsecManager;
import ai.traceable.anomaly.config.service.modsec.rules.ModsecValidator;
import ai.traceable.anomaly.config.service.v1.modsec.GetModsecCrsRulesRequest;
import ai.traceable.anomaly.config.service.v1.modsec.GetModsecCrsRulesResponse;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecCrsRulesData;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecCrsRulesType;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.List;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class AnomalyModsecConfigServiceImplTest {
  private static final String TENANT_ID = "tenant-anomaly-modsec-test";
  private ModsecValidator modsecValidator;
  private ModsecManager modsecManager;
  private AnomalyModsecConfigServiceImpl modsecConfigService;

  @BeforeEach
  void setUp() {
    modsecValidator = mock(ModsecValidator.class);
    modsecManager = mock(ModsecManager.class);
    modsecConfigService = new AnomalyModsecConfigServiceImpl(modsecValidator, modsecManager);
  }

  @Test
  @DisplayName("Should get all the modsec rules if valid request")
  void getModsecRules() {
    when(modsecValidator.validate(any())).thenReturn(Status.OK);

    ModsecCrsRulesData rule1 =
        ModsecCrsRulesData.newBuilder()
            .setModsecCrsRulesType(ModsecCrsRulesType.MODSEC_CRS_RULES_TYPE_SAFE)
            .setModsecCrsRulesBlob("safe")
            .build();
    ModsecCrsRulesData rule2 =
        ModsecCrsRulesData.newBuilder()
            .setModsecCrsRulesType(ModsecCrsRulesType.MODSEC_CRS_RULES_TYPE_REGULAR)
            .setModsecCrsRulesBlob("regular")
            .build();

    StreamObserver<GetModsecCrsRulesResponse> responseStreamObserver = mock(StreamObserver.class);

    Runnable runnable =
        () ->
            modsecConfigService.getModsecCrsRules(
                GetModsecCrsRulesRequest.getDefaultInstance(), responseStreamObserver);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(responseStreamObserver, times(1)).onNext(GetModsecCrsRulesResponse.newBuilder().build());
    verify(responseStreamObserver, times(1)).onCompleted();

    reset(responseStreamObserver);

    when(modsecManager.getModsecCrsRules(any(), any())).thenReturn(List.of(rule1, rule2));

    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(responseStreamObserver, times(1))
        .onNext(
            GetModsecCrsRulesResponse.newBuilder()
                .addAllModsecCrsRules(List.of(rule1, rule2))
                .build());
    verify(responseStreamObserver, times(1)).onCompleted();
  }

  @Test
  @DisplayName("Should return Invalid Argument if not a valid request")
  void should_fail_GetModsecRule_onInvalidRequest() {
    when(modsecValidator.validate(any())).thenReturn(Status.INVALID_ARGUMENT);

    StreamObserver<GetModsecCrsRulesResponse> responseStreamObserver = mock(StreamObserver.class);

    Runnable runnable =
        () ->
            modsecConfigService.getModsecCrsRules(
                GetModsecCrsRulesRequest.getDefaultInstance(), responseStreamObserver);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(responseStreamObserver, times(1))
        .onError(
            argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));
  }

  @Test
  @DisplayName("Should propagate expection on occured manager")
  void should_propagate_error() {
    when(modsecValidator.validate(any())).thenReturn(Status.OK);
    when(modsecManager.getModsecCrsRules(any(), any())).thenThrow(RuntimeException.class);

    StreamObserver<GetModsecCrsRulesResponse> responseStreamObserver = mock(StreamObserver.class);

    Runnable runnable =
        () ->
            modsecConfigService.getModsecCrsRules(
                GetModsecCrsRulesRequest.getDefaultInstance(), responseStreamObserver);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(responseStreamObserver, times(1))
        .onError(argThat(err -> err.getClass() == RuntimeException.class));
  }
}
