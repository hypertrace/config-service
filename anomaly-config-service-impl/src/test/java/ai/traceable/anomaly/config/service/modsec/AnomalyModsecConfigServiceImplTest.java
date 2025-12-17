package ai.traceable.anomaly.config.service.modsec;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.anomaly.config.service.modsec.rules.ModsecManager;
import ai.traceable.anomaly.config.service.modsec.rules.ModsecValidator;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleType;
import ai.traceable.anomaly.config.service.v1.RuleVersion;
import ai.traceable.anomaly.config.service.v1.modsec.GetImpactScoringRulesRequest;
import ai.traceable.anomaly.config.service.v1.modsec.GetImpactScoringRulesResponse;
import ai.traceable.anomaly.config.service.v1.modsec.GetModsecCrsRulesRequest;
import ai.traceable.anomaly.config.service.v1.modsec.GetModsecCrsRulesResponse;
import ai.traceable.anomaly.config.service.v1.modsec.ImpactScoringRules;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecCrsRulesData;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.context.RequestContext;
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
    doAnswer(args -> new ModsecManager.ModsecCrsRules(args.getArgument(3)))
        .when(modsecManager)
        .getModsecCrsRules(any(), any(), any(), any(), anyBoolean(), any());
    modsecConfigService =
        new AnomalyModsecConfigServiceImpl(
            modsecValidator,
            modsecManager,
            null,
            ModsecRuleVersion.MODSEC_RULE_VERSION_V3_SECARG_LIMITS_DETECTION_ONLY_MODE);
  }

  @Test
  @DisplayName("Should get all the modsec rules if valid request")
  void getModsecRules() {
    when(modsecValidator.validate(any(GetModsecCrsRulesRequest.class))).thenReturn(Status.OK);

    ModsecCrsRulesData rule1 =
        ModsecCrsRulesData.newBuilder()
            .setSubRuleType(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE)
            .setModsecCrsRulesBlob("safe")
            .build();
    ModsecCrsRulesData rule2 =
        ModsecCrsRulesData.newBuilder()
            .setSubRuleType(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_REGULAR)
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

    Map<AnomalySubRuleType, String> modsecBlobsForRuleTypes = new LinkedHashMap<>();
    modsecBlobsForRuleTypes.put(rule1.getSubRuleType(), rule1.getModsecCrsRulesBlob());
    modsecBlobsForRuleTypes.put(rule2.getSubRuleType(), rule2.getModsecCrsRulesBlob());
    doReturn(
            new ModsecManager.ModsecCrsRules(
                modsecBlobsForRuleTypes,
                rule1.getModsecCrsRulesBlob() + rule2.getModsecCrsRulesBlob()))
        .when(modsecManager)
        .getModsecCrsRules(any(), any(), any(), any(), eq(false), any());

    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(responseStreamObserver, times(1))
        .onNext(
            GetModsecCrsRulesResponse.newBuilder()
                .addAllModsecCrsRules(List.of(rule1, rule2))
                .setAggregatedModsecCrsRulesBlob(
                    rule1.getModsecCrsRulesBlob() + rule2.getModsecCrsRulesBlob())
                .build());
    verify(responseStreamObserver, times(1)).onCompleted();
  }

  @Test
  @DisplayName("Should return Invalid Argument if not a valid request")
  void should_fail_GetModsecRule_onInvalidRequest() {
    when(modsecValidator.validate(any(GetModsecCrsRulesRequest.class)))
        .thenReturn(Status.INVALID_ARGUMENT);

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
    when(modsecValidator.validate(any(GetModsecCrsRulesRequest.class))).thenReturn(Status.OK);
    doThrow(RuntimeException.class)
        .when(modsecManager)
        .getModsecCrsRules(any(), any(), any(), any(), eq(false), any());

    StreamObserver<GetModsecCrsRulesResponse> responseStreamObserver = mock(StreamObserver.class);

    Runnable runnable =
        () ->
            modsecConfigService.getModsecCrsRules(
                GetModsecCrsRulesRequest.getDefaultInstance(), responseStreamObserver);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(responseStreamObserver, times(1))
        .onError(argThat(err -> err.getClass() == RuntimeException.class));
  }

  @Test
  @DisplayName("Should get impact scoring rules successfully")
  void getImpactScoringRules_success() {
    String expectedBlob = "impact scoring rules blob content";
    when(modsecManager.getImpactScoringRulesBlob(RuleVersion.getDefaultInstance()))
        .thenReturn(expectedBlob);

    StreamObserver<GetImpactScoringRulesResponse> responseStreamObserver =
        mock(StreamObserver.class);

    Runnable runnable =
        () ->
            modsecConfigService.getImpactScoringRules(
                GetImpactScoringRulesRequest.getDefaultInstance(), responseStreamObserver);
    RequestContext.forTenantId(TENANT_ID).run(runnable);

    GetImpactScoringRulesResponse expectedResponse =
        GetImpactScoringRulesResponse.newBuilder()
            .setImpactScoringRules(
                ImpactScoringRules.newBuilder().setImpactScoringBlob(expectedBlob).build())
            .build();

    verify(responseStreamObserver, times(1)).onNext(expectedResponse);
    verify(responseStreamObserver, times(1)).onCompleted();
  }

  @Test
  @DisplayName("Should propagate exception from manager in getImpactScoringRules")
  void getImpactScoringRules_error() {
    RuntimeException expectedException = new RuntimeException("Test exception");
    when(modsecManager.getImpactScoringRulesBlob(any(RuleVersion.class)))
        .thenThrow(expectedException);

    StreamObserver<GetImpactScoringRulesResponse> responseStreamObserver =
        mock(StreamObserver.class);

    Runnable runnable =
        () ->
            modsecConfigService.getImpactScoringRules(
                GetImpactScoringRulesRequest.getDefaultInstance(), responseStreamObserver);
    RequestContext.forTenantId(TENANT_ID).run(runnable);

    verify(responseStreamObserver, times(1)).onError(expectedException);
    verify(responseStreamObserver, times(0)).onCompleted();
  }
}
