package ai.traceable.external.agent.attribute.config.service;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import ai.traceable.auth.detection.config.service.v1.AuthDetectionConfigServiceGrpc;
import ai.traceable.auth.detection.config.service.v1.AuthDetectionConfigServiceGrpc.AuthDetectionConfigServiceImplBase;
import ai.traceable.auth.detection.config.service.v1.GetAuthDetectionRulesRequest;
import ai.traceable.auth.detection.config.service.v1.GetAuthDetectionRulesResponse;
import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.config.utils.SemanticVersioningComparator;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.external.agent.attribute.config.service.translator.ExternalAgentAttributeRuleTranslator;
import ai.traceable.external.agent.attribute.config.service.v1.ExternalAgentAttributeConfigServiceGrpc;
import ai.traceable.external.agent.attribute.config.service.v1.ExternalAgentAttributeConfigServiceGrpc.ExternalAgentAttributeConfigServiceBlockingStub;
import ai.traceable.external.agent.attribute.config.service.v1.GetAgentAttributeRulesRequest;
import ai.traceable.jwt.extraction.config.service.v1.GetJwtExtractionRulesRequest;
import ai.traceable.jwt.extraction.config.service.v1.GetJwtExtractionRulesResponse;
import ai.traceable.jwt.extraction.config.service.v1.JwtExtractionConfigServiceGrpc;
import ai.traceable.jwt.extraction.config.service.v1.JwtExtractionConfigServiceGrpc.JwtExtractionConfigServiceImplBase;
import ai.traceable.sessionidentification.config.service.v1.GetSessionIdentificationRulesRequest;
import ai.traceable.sessionidentification.config.service.v1.GetSessionIdentificationRulesResponse;
import ai.traceable.sessionidentification.config.service.v1.SessionIdentificationConfigServiceGrpc;
import ai.traceable.span.processing.config.service.v1.GetServiceNamingRulesRequest;
import ai.traceable.span.processing.config.service.v1.GetServiceNamingRulesResponse;
import ai.traceable.span.processing.config.service.v1.SpanProcessingConfigServiceGrpc;
import ai.traceable.span.processing.config.service.v1.SpanProcessingConfigServiceGrpc.SpanProcessingConfigServiceImplBase;
import ai.traceable.userattribution.config.service.v1.GetUserAttributionRulesRequest;
import ai.traceable.userattribution.config.service.v1.GetUserAttributionRulesResponse;
import ai.traceable.userattribution.config.service.v1.UserAttributionConfigServiceGrpc;
import ai.traceable.userattribution.config.service.v1.UserAttributionConfigServiceGrpc.UserAttributionConfigServiceImplBase;
import io.grpc.stub.StreamObserver;
import org.hypertrace.config.objectstore.ClientConfig;
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
        .addService(new MockAuthDetectionConfigService())
        .addService(new MockJwtExtractionConfigService())
        .addService(new MockSpanProcessingConfigService())
        .addService(new MockSessionIdentificationService())
        .addService(
            new ExternalAgentAttributeConfigServiceImpl(
                UserAttributionConfigServiceGrpc.newBlockingStub(
                    this.mockGenericConfigService.channel()),
                AuthDetectionConfigServiceGrpc.newBlockingStub(
                    this.mockGenericConfigService.channel()),
                JwtExtractionConfigServiceGrpc.newBlockingStub(
                    this.mockGenericConfigService.channel()),
                SpanProcessingConfigServiceGrpc.newBlockingStub(
                    this.mockGenericConfigService.channel()),
                SessionIdentificationConfigServiceGrpc.newBlockingStub(
                    this.mockGenericConfigService.channel()),
                mockRuleTranslator,
                new ExternalAgentAttributeRuleResponseBuilder(mockUuidGenerator),
                mockFeatureClient,
                new SemanticVersioningComparator(),
                ClientConfig.DEFAULT))
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

  private static class MockSessionIdentificationService
      extends SessionIdentificationConfigServiceGrpc.SessionIdentificationConfigServiceImplBase {
    @Override
    public void getSessionIdentificationRules(
        GetSessionIdentificationRulesRequest request,
        StreamObserver<GetSessionIdentificationRulesResponse> responseObserver) {
      responseObserver.onNext(GetSessionIdentificationRulesResponse.getDefaultInstance());
      responseObserver.onCompleted();
    }
  }

  private static class MockAuthDetectionConfigService extends AuthDetectionConfigServiceImplBase {
    @Override
    public void getAuthDetectionRules(
        GetAuthDetectionRulesRequest request,
        StreamObserver<GetAuthDetectionRulesResponse> responseObserver) {
      responseObserver.onNext(GetAuthDetectionRulesResponse.getDefaultInstance());
      responseObserver.onCompleted();
    }
  }

  private static class MockJwtExtractionConfigService extends JwtExtractionConfigServiceImplBase {
    @Override
    public void getJwtExtractionRules(
        GetJwtExtractionRulesRequest request,
        StreamObserver<GetJwtExtractionRulesResponse> responseObserver) {
      responseObserver.onNext(GetJwtExtractionRulesResponse.getDefaultInstance());
      responseObserver.onCompleted();
    }
  }

  private static class MockSpanProcessingConfigService extends SpanProcessingConfigServiceImplBase {
    @Override
    public void getServiceNamingRules(
        GetServiceNamingRulesRequest request,
        StreamObserver<GetServiceNamingRulesResponse> responseObserver) {
      responseObserver.onNext(GetServiceNamingRulesResponse.getDefaultInstance());
      responseObserver.onCompleted();
    }
  }
}
