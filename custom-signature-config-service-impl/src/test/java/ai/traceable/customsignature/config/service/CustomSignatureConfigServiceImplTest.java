package ai.traceable.customsignature.config.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.customsignature.config.service.modsec.ModsecRulesManager;
import ai.traceable.customsignature.config.service.rules.RulesManager;
import ai.traceable.customsignature.config.service.rules.RulesValidator;
import ai.traceable.customsignature.config.service.rules.converter.CustomSignatureEdgeDecisionConverter;
import ai.traceable.customsignature.config.service.v1.CreateCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.CreateCustomSignatureRuleResponse;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.DeleteCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.DeleteCustomSignatureRuleResponse;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesRequest;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesResponse;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureRulesRequest;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureRulesResponse;
import ai.traceable.customsignature.config.service.v1.UpdateCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.UpdateCustomSignatureRuleResponse;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CustomSignatureConfigServiceImplTest {
  private static final String TENANT_ID = "default tenant";

  private RulesValidator rulesValidator;
  private RulesManager rulesManager;
  private ModsecRulesManager modsecRulesManager;
  private CustomSignatureEdgeDecisionConverter edgeDecisionConverter;

  private CustomSignatureConfigServiceImpl configService;
  private FeatureCachingClient featureCachingClient;

  @BeforeEach
  public void setup() {
    rulesValidator = mock(RulesValidator.class);
    rulesManager = mock(RulesManager.class);
    modsecRulesManager = mock(ModsecRulesManager.class);
    edgeDecisionConverter = mock(CustomSignatureEdgeDecisionConverter.class);
    featureCachingClient = mock(FeatureCachingClient.class);
    when(featureCachingClient.isEdgeDecisionEnabledForTenant(any())).thenReturn(true);
    configService =
        new CustomSignatureConfigServiceImpl(
            rulesValidator,
            rulesManager,
            modsecRulesManager,
            edgeDecisionConverter,
            featureCachingClient);
  }

  @Test
  public void testGetRules() {
    List<CustomSignatureRule> rules =
        List.of(
            CustomSignatureRule.newBuilder().setId("id1").build(),
            CustomSignatureRule.newBuilder().setId("id2").build());

    StreamObserver<GetCustomSignatureRulesResponse> responseObserver = mock(StreamObserver.class);
    Runnable runnable =
        () ->
            configService.getCustomSignatureRules(
                GetCustomSignatureRulesRequest.getDefaultInstance(), responseObserver);

    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(responseObserver, times(1)).onNext(GetCustomSignatureRulesResponse.newBuilder().build());
    verify(responseObserver, times(1)).onCompleted();

    reset(responseObserver);
    when(rulesManager.getCustomSignatureRules(any(), any())).thenReturn(rules);

    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(responseObserver, times(1))
        .onNext(GetCustomSignatureRulesResponse.newBuilder().addAllRules(rules).build());
    verify(responseObserver, times(1)).onCompleted();
  }

  @Test
  public void testCreateRule() {
    CustomSignatureRule rule = CustomSignatureRule.newBuilder().setId("id1").build();

    StreamObserver<CreateCustomSignatureRuleResponse> responseObserver = mock(StreamObserver.class);
    Runnable runnable =
        () ->
            configService.createCustomSignatureRule(
                CreateCustomSignatureRuleRequest.getDefaultInstance(), responseObserver);

    when(rulesValidator.validate((CreateCustomSignatureRuleRequest) any()))
        .thenReturn(Status.INVALID_ARGUMENT);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(responseObserver, times(1))
        .onError(
            argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));

    reset(responseObserver);
    when(rulesValidator.validate((CreateCustomSignatureRuleRequest) any())).thenReturn(Status.OK);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(responseObserver, times(1))
        .onError(argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INTERNAL));

    reset(responseObserver);
    when(rulesManager.createCustomSignatureRule(any(), any())).thenReturn(Optional.of(rule));
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(responseObserver, times(1))
        .onNext(CreateCustomSignatureRuleResponse.newBuilder().setRule(rule).build());
    verify(responseObserver, times(1)).onCompleted();
  }

  @Test
  public void testUpdateRule() {
    CustomSignatureRule rule = CustomSignatureRule.newBuilder().setId("id1").build();

    StreamObserver<UpdateCustomSignatureRuleResponse> responseObserver = mock(StreamObserver.class);
    Runnable runnable =
        () ->
            configService.updateCustomSignatureRule(
                UpdateCustomSignatureRuleRequest.getDefaultInstance(), responseObserver);

    when(rulesValidator.validate((UpdateCustomSignatureRuleRequest) any()))
        .thenReturn(Status.INVALID_ARGUMENT);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(responseObserver, times(1))
        .onError(
            argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));

    reset(responseObserver);
    when(rulesValidator.validate((UpdateCustomSignatureRuleRequest) any())).thenReturn(Status.OK);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(responseObserver, times(1))
        .onError(argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INTERNAL));

    reset(responseObserver);
    when(rulesManager.updateCustomSignatureRule(any(), any())).thenReturn(Optional.of(rule));
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(responseObserver, times(1))
        .onNext(UpdateCustomSignatureRuleResponse.newBuilder().setRule(rule).build());
    verify(responseObserver, times(1)).onCompleted();
  }

  @Test
  void testDeleteRule() throws InvalidProtocolBufferException {
    DeleteCustomSignatureRuleRequest deleteCustomSignatureRuleRequest =
        DeleteCustomSignatureRuleRequest.newBuilder().setId("id").build();
    StreamObserver<DeleteCustomSignatureRuleResponse> responseObserver = mock(StreamObserver.class);
    Runnable runnable =
        () ->
            configService.deleteCustomSignatureRule(
                deleteCustomSignatureRuleRequest, responseObserver);

    when(rulesValidator.validate((DeleteCustomSignatureRuleRequest) any()))
        .thenReturn(Status.INVALID_ARGUMENT);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(responseObserver, times(1))
        .onError(
            argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT));

    reset(responseObserver);
    when(rulesValidator.validate((DeleteCustomSignatureRuleRequest) any())).thenReturn(Status.OK);
    when(rulesManager.deleteCustomSignatureRule(any(), eq("id")))
        .thenReturn(Optional.of(CustomSignatureRule.newBuilder().setId("id").build()));
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(responseObserver, times(1))
        .onNext(DeleteCustomSignatureRuleResponse.getDefaultInstance());
    verify(responseObserver, times(1)).onCompleted();
  }

  @Test
  public void testGetCustomSignatureModsecRules() {
    when(rulesManager.getCustomSignatureRules(any(), any()))
        .thenReturn(List.of(CustomSignatureRule.newBuilder().build()));
    when(modsecRulesManager.getModsecRules(any(), any(), any()))
        .thenThrow(new UnsupportedOperationException())
        .thenReturn(GetCustomSignatureModsecRulesResponse.newBuilder().build());

    StreamObserver<GetCustomSignatureModsecRulesResponse> responseObserver =
        mock(StreamObserver.class);
    Runnable runnable =
        () ->
            configService.getCustomSignatureModsecRules(
                GetCustomSignatureModsecRulesRequest.getDefaultInstance(), responseObserver);

    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);
    verify(responseObserver, times(1))
        .onError(argThat(err -> Status.fromThrowable(err).getCode() == Status.Code.INTERNAL));
    verify(responseObserver, times(1))
        .onNext(GetCustomSignatureModsecRulesResponse.newBuilder().build());
    verify(responseObserver, times(1)).onCompleted();
  }
}
