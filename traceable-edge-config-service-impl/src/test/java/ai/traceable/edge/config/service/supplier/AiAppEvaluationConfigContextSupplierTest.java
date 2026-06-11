package ai.traceable.edge.config.service.supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.aiapp.protection.config.service.v1.AiAppConfigServiceGrpc;
import ai.traceable.aiapp.protection.config.service.v1.GetAiAppEvaluationConfigContextRequest;
import ai.traceable.aiapp.protection.config.service.v1.GetAiAppEvaluationConfigContextResponse;
import ai.traceable.aiapp.protection.config.service.v1.RuleEvaluationPoint;
import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.edge.config.service.config.TraceableEdgeConfig;
import ai.traceable.edge.config.service.v1.AgentCapabilities;
import ai.traceable.edge.config.service.v1.ConfigRequestElement;
import ai.traceable.edge.config.service.v1.ConfigResponseElement;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import java.util.concurrent.Callable;
import org.hypertrace.config.objectstore.ClientConfig;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AiAppEvaluationConfigContextSupplierTest {
  private static final String TEST_ENV = "edge-waf-automation";

  @Mock private RequestContext requestContext;
  @Mock private ConfigRequestElement requestElement;
  @Mock private TraceableEdgeConfig config;
  @Mock private FeatureCachingClient featureCachingClient;
  @Mock private AiAppConfigServiceGrpc.AiAppConfigServiceBlockingStub stub;
  @Mock private AiAppConfigServiceGrpc.AiAppConfigServiceBlockingStub stubWithDeadline;

  private AiAppEvaluationConfigContextSupplier supplier;

  @BeforeEach
  void setUp() {
    lenient()
        .when(featureCachingClient.isProtectionEngineAiAppProtectionEnabledForTenant(any()))
        .thenReturn(true);
    lenient()
        .when(config.getAgentPollingFrequency("AiFirewallConfigContext"))
        .thenReturn(Duration.newBuilder().setSeconds(600).build());
    supplier =
        new AiAppEvaluationConfigContextSupplier(
            new UuidGenerator(), config, stub, featureCachingClient);
  }

  private void stubGrpcClient() throws Exception {
    when(config.getClientConfig()).thenReturn(ClientConfig.DEFAULT);
    when(stub.withDeadlineAfter(anyLong(), any())).thenReturn(stubWithDeadline);
    doAnswer(invocation -> invocation.getArgument(0, Callable.class).call())
        .when(requestContext)
        .call(any());
  }

  @Test
  void testGetConfigs_featureFlagDisabled_returnsEmptyPayload() {
    when(featureCachingClient.isProtectionEngineAiAppProtectionEnabledForTenant(any()))
        .thenReturn(false);
    AgentCapabilities agentCapabilities = AgentCapabilities.getDefaultInstance();

    ConfigResponseElement response =
        supplier.getConfigs(requestContext, TEST_ENV, requestElement, agentCapabilities);

    assertEquals("AiFirewallConfigContext", response.getConfigType());
    assertTrue(response.getConfigPayloads().getConfigBytesList().isEmpty());
  }

  @Test
  void testGetConfigs_withEnvironment_passesEnvironmentScopeInRequest() throws Exception {
    stubGrpcClient();
    ByteString configContext = ByteString.copyFromUtf8("test-config");
    when(stubWithDeadline.getAiAppEvaluationConfigContext(any()))
        .thenReturn(
            GetAiAppEvaluationConfigContextResponse.newBuilder()
                .setAiAppEvaluationConfigContext(configContext)
                .build());
    AgentCapabilities agentCapabilities = AgentCapabilities.getDefaultInstance();

    ConfigResponseElement response =
        supplier.getConfigs(requestContext, TEST_ENV, requestElement, agentCapabilities);

    ArgumentCaptor<GetAiAppEvaluationConfigContextRequest> requestCaptor =
        ArgumentCaptor.forClass(GetAiAppEvaluationConfigContextRequest.class);
    verify(stubWithDeadline).getAiAppEvaluationConfigContext(requestCaptor.capture());
    GetAiAppEvaluationConfigContextRequest request = requestCaptor.getValue();
    assertEquals(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE, request.getRuleEvaluationPoint());
    assertTrue(request.getRuleScope().hasEnvironmentScope());
    assertEquals(TEST_ENV, request.getRuleScope().getEnvironmentScope().getEnvironmentIds(0));
    assertEquals(configContext, response.getConfigPayloads().getConfigBytes(0));
  }

  @Test
  void testGetConfigs_withBlankEnvironment_setsTenantScope() throws Exception {
    stubGrpcClient();
    when(stubWithDeadline.getAiAppEvaluationConfigContext(any()))
        .thenReturn(GetAiAppEvaluationConfigContextResponse.getDefaultInstance());
    AgentCapabilities agentCapabilities = AgentCapabilities.getDefaultInstance();

    supplier.getConfigs(requestContext, "  ", requestElement, agentCapabilities);

    ArgumentCaptor<GetAiAppEvaluationConfigContextRequest> requestCaptor =
        ArgumentCaptor.forClass(GetAiAppEvaluationConfigContextRequest.class);
    verify(stubWithDeadline).getAiAppEvaluationConfigContext(requestCaptor.capture());
    assertTrue(requestCaptor.getValue().getRuleScope().hasTenantScope());
  }
}
