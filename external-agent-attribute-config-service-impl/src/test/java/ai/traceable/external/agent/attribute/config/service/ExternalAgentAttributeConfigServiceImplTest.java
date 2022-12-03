package ai.traceable.external.agent.attribute.config.service;

import static java.util.Collections.*;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.external.agent.attribute.config.service.translator.ExternalAgentAttributeRuleTranslator;
import ai.traceable.external.agent.attribute.config.service.v1.ExternalAgentAttributeConfigServiceGrpc;
import ai.traceable.external.agent.attribute.config.service.v1.ExternalAgentAttributeConfigServiceGrpc.ExternalAgentAttributeConfigServiceBlockingStub;
import ai.traceable.external.agent.attribute.config.service.v1.GetAgentAttributeRulesRequest;
import ai.traceable.userattribution.config.service.v1.GetUserAttributionRulesRequest;
import ai.traceable.userattribution.config.service.v1.GetUserAttributionRulesResponse;
import ai.traceable.userattribution.config.service.v1.UserAttributionConfigServiceGrpc;
import ai.traceable.userattribution.config.service.v1.UserAttributionConfigServiceGrpc.UserAttributionConfigServiceImplBase;
import io.grpc.stub.StreamObserver;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ExternalAgentAttributeConfigServiceImplTest {
  MockGenericConfigService mockGenericConfigService;
  @Mock UuidGenerator mockUuidGenerator;
  @Mock FeatureCachingClient mockFeatureClient;
  @Mock ExternalAgentAttributeRuleTranslator mockRuleTranslator;
  ExternalAgentAttributeConfigServiceBlockingStub stub;

  @BeforeEach
  void setUp() {
    mockGenericConfigService = new MockGenericConfigService();
    mockGenericConfigService
        .addService(new MockUserAttributionService())
        .addService(
            new ExternalAgentAttributeConfigServiceImpl(
                UserAttributionConfigServiceGrpc.newBlockingStub(
                    this.mockGenericConfigService.channel()),
                mockRuleTranslator,
                new ExternalAgentAttributeRuleResponseBuilder(mockUuidGenerator),
                mockFeatureClient))
        .start();
    stub =
        ExternalAgentAttributeConfigServiceGrpc.newBlockingStub(
                this.mockGenericConfigService.channel())
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @AfterEach
  void afterEach() {
    mockGenericConfigService.shutdown();
  }

  @Test
  void returnsDisabledResponseIfFeatureDisabled() {
    when(this.mockFeatureClient.isUserAttributionV2Enabled(any())).thenReturn(false);
    assertFalse(
        this.stub
            .getAgentAttributeRules(GetAgentAttributeRulesRequest.getDefaultInstance())
            .getEnabled());
  }

  @Test
  void returnsEnabledResponseIfFeatureEnabled() {
    when(this.mockFeatureClient.isUserAttributionV2Enabled(any())).thenReturn(true);
    when(this.mockUuidGenerator.generateId(emptyList())).thenReturn("test-hash");
    assertTrue(
        this.stub
            .getAgentAttributeRules(GetAgentAttributeRulesRequest.getDefaultInstance())
            .getEnabled());
  }

  private static class MockUserAttributionService extends UserAttributionConfigServiceImplBase {
    @Override
    public void getUserAttributionRules(
        GetUserAttributionRulesRequest request,
        StreamObserver<GetUserAttributionRulesResponse> responseObserver) {
      responseObserver.onNext(GetUserAttributionRulesResponse.getDefaultInstance());
      responseObserver.onCompleted();
    }
  }
}
