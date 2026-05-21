package ai.traceable.aiapp.protection.config.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.aiapp.protection.config.service.converter.AnomalyToAiAppRuleConverter;
import ai.traceable.aiapp.protection.config.service.converter.customsignature.AiAppToCustomSignatureConverter;
import ai.traceable.aiapp.protection.config.service.converter.customsignature.CustomSignatureToAiAppConverter;
import ai.traceable.aiapp.protection.config.service.converter.ratelimit.AiAppToRateLimitingConverter;
import ai.traceable.aiapp.protection.config.service.converter.ratelimit.RateLimitingToAiAppConverter;
import ai.traceable.aiapp.protection.config.service.firewall.AiAppEvaluationConfigContextManager;
import ai.traceable.aiapp.protection.config.service.v1.AiAppCustomRule;
import ai.traceable.aiapp.protection.config.service.v1.AiAppCustomRuleData;
import ai.traceable.aiapp.protection.config.service.v1.AiAppCustomRuleToDelete;
import ai.traceable.aiapp.protection.config.service.v1.AiAppCustomRuleType;
import ai.traceable.aiapp.protection.config.service.v1.AiAppRule;
import ai.traceable.aiapp.protection.config.service.v1.AiAppRuleUpdate;
import ai.traceable.aiapp.protection.config.service.v1.AiInputExplosionRuleData;
import ai.traceable.aiapp.protection.config.service.v1.AiSensitiveDataProtectionRuleData;
import ai.traceable.aiapp.protection.config.service.v1.CreateAiAppCustomRuleRequest;
import ai.traceable.aiapp.protection.config.service.v1.CreateAiAppCustomRuleResponse;
import ai.traceable.aiapp.protection.config.service.v1.DeleteAiAppRulesRequest;
import ai.traceable.aiapp.protection.config.service.v1.DeleteAiAppRulesResponse;
import ai.traceable.aiapp.protection.config.service.v1.GetAiAppRulesRequest;
import ai.traceable.aiapp.protection.config.service.v1.GetAiAppRulesResponse;
import ai.traceable.aiapp.protection.config.service.v1.ModelGovernanceRuleData;
import ai.traceable.aiapp.protection.config.service.v1.PiiDetectedInPromptRuleData;
import ai.traceable.aiapp.protection.config.service.v1.ResetToDefault;
import ai.traceable.aiapp.protection.config.service.v1.RuleStatusChange;
import ai.traceable.aiapp.protection.config.service.v1.UpdateAiAppRulesRequest;
import ai.traceable.aiapp.protection.config.service.v1.UpdateAiAppRulesResponse;
import ai.traceable.aiapp.protection.config.service.v1.UpsertAiAppCustomRuleRequest;
import ai.traceable.aiapp.protection.config.service.v1.UpsertAiAppCustomRuleResponse;
import ai.traceable.aiapp.protection.config.service.validator.AiAppProtectionConfigServiceValidator;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.detector.DeleteScopedAnomalyDetectionConfigResponse;
import ai.traceable.anomaly.config.service.v1.detector.DetectorConfigServiceGrpc;
import ai.traceable.anomaly.config.service.v1.detector.GetAllUnresolvedScopedAnomalyDetectionConfigsResponse;
import ai.traceable.anomaly.config.service.v1.detector.GetScopedAnomalyDetectionConfigResponse;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.UpdateScopedAnomalyDetectionConfigResponse;
import ai.traceable.anomaly.config.service.v1.global.AnomalyGlobalConfigServiceGrpc;
import ai.traceable.anomaly.config.service.v1.global.GetAnomalyRuleInfosResponse;
import ai.traceable.customsignature.config.service.v1.CreateCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.CreateCustomSignatureRuleResponse;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.DeleteCustomSignatureRuleResponse;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureRulesResponse;
import ai.traceable.customsignature.config.service.v1.RuleDefinition;
import ai.traceable.customsignature.config.service.v1.UpdateCustomSignatureRuleResponse;
import ai.traceable.ratelimiting.config.service.v2.CreateRateLimitingRuleRequest;
import ai.traceable.ratelimiting.config.service.v2.CreateRateLimitingRuleResponse;
import ai.traceable.ratelimiting.config.service.v2.DeleteRateLimitingRuleResponse;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRulesResponse;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingConfigServiceGrpc;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRule;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRuleData;
import ai.traceable.ratelimiting.config.service.v2.UpdateRateLimitingRuleResponse;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.Arrays;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class AiAppProtectionConfigServiceImplTest {

  @Mock private AiAppProtectionConfigServiceValidator validator;
  @Mock private AnomalyToAiAppRuleConverter anomalyToAiAppRuleConverter;
  @Mock private AiAppToCustomSignatureConverter aiAppToCustomSignatureConverter;
  @Mock private CustomSignatureToAiAppConverter customSignatureToAiAppConverter;
  @Mock private RateLimitingToAiAppConverter rateLimitingToAiAppConverter;
  @Mock private AiAppToRateLimitingConverter aiAppToRateLimitingConverter;

  @Mock
  private AnomalyGlobalConfigServiceGrpc.AnomalyGlobalConfigServiceBlockingStub
      anomalyGlobalConfigStub;

  @Mock private DetectorConfigServiceGrpc.DetectorConfigServiceBlockingStub detectorConfigStub;

  @Mock
  private RateLimitingConfigServiceGrpc.RateLimitingConfigServiceBlockingStub
      rateLimitingConfigStub;

  @Mock
  private CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub
      customSignatureConfigStub;

  @Mock private AiAppEvaluationConfigContextManager aiAppEvaluationConfigContextManager;
  @Mock private StreamObserver<GetAiAppRulesResponse> getResponseObserver;
  @Mock private StreamObserver<UpdateAiAppRulesResponse> updateResponseObserver;
  @Mock private StreamObserver<DeleteAiAppRulesResponse> deleteResponseObserver;
  @Mock private StreamObserver<CreateAiAppCustomRuleResponse> createResponseObserver;
  @Mock private StreamObserver<UpsertAiAppCustomRuleResponse> upsertResponseObserver;
  private RequestContext requestContext = RequestContext.forTenantId("tenantId");

  private AiAppProtectionConfigServiceImpl service;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    service =
        new AiAppProtectionConfigServiceImpl(
            aiAppEvaluationConfigContextManager,
            validator,
            anomalyToAiAppRuleConverter,
            aiAppToCustomSignatureConverter,
            customSignatureToAiAppConverter,
            rateLimitingToAiAppConverter,
            aiAppToRateLimitingConverter,
            anomalyGlobalConfigStub,
            detectorConfigStub,
            rateLimitingConfigStub,
            customSignatureConfigStub);
  }

  @Nested
  @DisplayName("GetAiAppRules Tests")
  class GetAiAppRulesTests {

    @Test
    @DisplayName("Should return rules successfully with valid request")
    void shouldReturnRulesSuccessfullyWithValidRequest() {
      // Given
      GetAiAppRulesRequest request = createValidGetRequest();
      when(validator.validateGetAiAppRulesRequest(request)).thenReturn(Status.OK);

      // Mock anomaly service responses
      GetAnomalyRuleInfosResponse anomalyRuleInfosResponse = createMockAnomalyRuleInfosResponse();
      when(anomalyGlobalConfigStub.getAnomalyRuleInfos(any())).thenReturn(anomalyRuleInfosResponse);

      GetScopedAnomalyDetectionConfigResponse scopedConfigResponse =
          createMockScopedConfigResponse();
      when(detectorConfigStub.getScopedAnomalyDetectionConfig(any()))
          .thenReturn(scopedConfigResponse);

      GetAllUnresolvedScopedAnomalyDetectionConfigsResponse unresolvedConfigsResponse =
          createMockUnresolvedConfigsResponse();
      when(detectorConfigStub.getAllUnresolvedScopedAnomalyDetectionConfigs(any()))
          .thenReturn(unresolvedConfigsResponse);

      // Mock rate limiting service response
      GetRateLimitingRulesResponse rateLimitingResponse = createMockRateLimitingResponse();
      when(rateLimitingConfigStub.getRateLimitingRules(any())).thenReturn(rateLimitingResponse);

      // Mock custom signature service response
      GetCustomSignatureRulesResponse customSignatureResponse = createMockCustomSignatureResponse();
      when(customSignatureConfigStub.getCustomSignatureRules(any()))
          .thenReturn(customSignatureResponse);

      // Mock converter calls for custom signature and rate limiting rules
      when(customSignatureToAiAppConverter.convertFromCustomSignatureRule(any()))
          .thenReturn(AiAppCustomRule.newBuilder().build());
      when(rateLimitingToAiAppConverter.convertFromRateLimitingRule(any()))
          .thenReturn(AiAppCustomRule.newBuilder().build());

      // Mock converter response
      List<AiAppRule> mockRules = createMockAiAppRules();
      when(anomalyToAiAppRuleConverter.convertToAiAppRules(
              any(), any(), any(), any(), any(), any()))
          .thenReturn(mockRules);

      // When
      requestContext.run(() -> service.getAiAppRules(request, getResponseObserver));

      // Then
      verify(getResponseObserver)
          .onNext(argThat(response -> response.getAiAppRulesList().size() == mockRules.size()));
      verify(getResponseObserver).onCompleted();
      verify(getResponseObserver, never()).onError(any());
    }

    @Test
    @DisplayName("Should handle validation error")
    void shouldHandleValidationError() {
      // Given
      GetAiAppRulesRequest request = createValidGetRequest();
      Status validationError = Status.INVALID_ARGUMENT.withDescription("Invalid request");
      when(validator.validateGetAiAppRulesRequest(request)).thenReturn(validationError);

      // When
      requestContext.run(() -> service.getAiAppRules(request, getResponseObserver));

      // Then
      verify(getResponseObserver)
          .onError(
              argThat(
                  throwable ->
                      throwable instanceof StatusRuntimeException
                          && ((StatusRuntimeException) throwable).getStatus().getCode()
                              == Status.Code.INVALID_ARGUMENT
                          && ((StatusRuntimeException) throwable)
                              .getStatus()
                              .getDescription()
                              .equals("Invalid request")));
      verify(getResponseObserver, never()).onNext(any());
      verify(getResponseObserver, never()).onCompleted();
    }

    @Test
    @DisplayName("Should handle anomaly service error")
    void shouldHandleAnomalyServiceError() {
      // Given
      GetAiAppRulesRequest request = createValidGetRequest();
      when(validator.validateGetAiAppRulesRequest(request)).thenReturn(Status.OK);

      // Mock the anomaly service to throw an exception
      when(anomalyGlobalConfigStub.getAnomalyRuleInfos(any()))
          .thenThrow(new RuntimeException("Anomaly service error"));

      // Mock other services to ensure we reach the anomaly service call
      when(detectorConfigStub.getScopedAnomalyDetectionConfig(any()))
          .thenReturn(createMockScopedConfigResponse());
      when(detectorConfigStub.getAllUnresolvedScopedAnomalyDetectionConfigs(any()))
          .thenReturn(createMockUnresolvedConfigsResponse());
      when(customSignatureConfigStub.getCustomSignatureRules(any()))
          .thenReturn(createMockCustomSignatureResponse());
      when(rateLimitingConfigStub.getRateLimitingRules(any()))
          .thenReturn(createMockRateLimitingResponse());

      // Mock converter calls for custom signature and rate limiting rules
      when(customSignatureToAiAppConverter.convertFromCustomSignatureRule(any()))
          .thenReturn(AiAppCustomRule.newBuilder().build());
      when(rateLimitingToAiAppConverter.convertFromRateLimitingRule(any()))
          .thenReturn(AiAppCustomRule.newBuilder().build());

      // When
      requestContext.run(() -> service.getAiAppRules(request, getResponseObserver));

      // Then
      verify(getResponseObserver)
          .onError(
              argThat(
                  throwable ->
                      throwable instanceof StatusRuntimeException
                          && ((StatusRuntimeException) throwable).getStatus().getCode()
                              == Status.Code.INTERNAL));
      verify(getResponseObserver, never()).onNext(any());
      verify(getResponseObserver, never()).onCompleted();
    }
  }

  @Nested
  @DisplayName("UpdateAiAppRules Tests")
  class UpdateAiAppRulesTests {

    @Test
    @DisplayName("Should update anomaly rules successfully with valid request")
    void shouldUpdateAnomalyRulesSuccessfullyWithValidRequest() {
      // Given
      UpdateAiAppRulesRequest request = createValidUpdateRequest();
      when(validator.validateUpdateAiAppRulesRequest(request)).thenReturn(Status.OK);

      UpdateScopedAnomalyDetectionConfigResponse updateResponse =
          UpdateScopedAnomalyDetectionConfigResponse.newBuilder().build();
      when(detectorConfigStub.updateScopedAnomalyDetectionConfig(any())).thenReturn(updateResponse);

      // Mock the getUpdatedAiAppRules call
      List<AiAppRule> updatedRules = createMockAiAppRules();
      when(anomalyToAiAppRuleConverter.convertToAiAppRules(
              any(), any(), any(), any(), any(), any()))
          .thenReturn(updatedRules);

      GetAnomalyRuleInfosResponse anomalyRuleInfosResponse = createMockAnomalyRuleInfosResponse();
      when(anomalyGlobalConfigStub.getAnomalyRuleInfos(any())).thenReturn(anomalyRuleInfosResponse);

      GetScopedAnomalyDetectionConfigResponse scopedConfigResponse =
          createMockScopedConfigResponse();
      when(detectorConfigStub.getScopedAnomalyDetectionConfig(any()))
          .thenReturn(scopedConfigResponse);

      GetAllUnresolvedScopedAnomalyDetectionConfigsResponse unresolvedConfigsResponse =
          createMockUnresolvedConfigsResponse();
      when(detectorConfigStub.getAllUnresolvedScopedAnomalyDetectionConfigs(any()))
          .thenReturn(unresolvedConfigsResponse);

      GetRateLimitingRulesResponse rateLimitingResponse = createMockRateLimitingResponse();
      when(rateLimitingConfigStub.getRateLimitingRules(any())).thenReturn(rateLimitingResponse);

      GetCustomSignatureRulesResponse customSignatureResponse = createMockCustomSignatureResponse();
      when(customSignatureConfigStub.getCustomSignatureRules(any()))
          .thenReturn(customSignatureResponse);

      // Mock converter calls for custom signature and rate limiting rules
      when(customSignatureToAiAppConverter.convertFromCustomSignatureRule(any()))
          .thenReturn(AiAppCustomRule.newBuilder().build());
      when(rateLimitingToAiAppConverter.convertFromRateLimitingRule(any()))
          .thenReturn(AiAppCustomRule.newBuilder().build());

      // When
      requestContext.run(() -> service.updateAiAppRules(request, updateResponseObserver));

      // Then
      verify(detectorConfigStub).updateScopedAnomalyDetectionConfig(any());
      verify(updateResponseObserver).onCompleted();
      verify(updateResponseObserver, never()).onError(any());
    }

    @Test
    @DisplayName("Should handle validation error")
    void shouldHandleValidationError() {
      // Given
      UpdateAiAppRulesRequest request = createValidUpdateRequest();
      Status validationError = Status.INVALID_ARGUMENT.withDescription("Invalid update request");
      when(validator.validateUpdateAiAppRulesRequest(request)).thenReturn(validationError);

      // When
      requestContext.run(() -> service.updateAiAppRules(request, updateResponseObserver));

      // Then
      verify(updateResponseObserver)
          .onError(
              argThat(
                  throwable ->
                      throwable instanceof StatusRuntimeException
                          && ((StatusRuntimeException) throwable).getStatus().getCode()
                              == Status.Code.INVALID_ARGUMENT
                          && ((StatusRuntimeException) throwable)
                              .getStatus()
                              .getDescription()
                              .equals("Invalid update request")));
      verify(detectorConfigStub, never()).updateScopedAnomalyDetectionConfig(any());
    }
  }

  @Nested
  @DisplayName("DeleteAiAppRules Tests")
  class DeleteAiAppRulesTests {

    @Test
    @DisplayName("Should delete rules successfully with reset to default")
    void shouldDeleteRulesSuccessfullyWithResetToDefault() {
      // Given
      DeleteAiAppRulesRequest request = createValidDeleteRequestWithResetToDefault();
      when(validator.validateDeleteAiAppRulesRequest(request)).thenReturn(Status.OK);

      DeleteScopedAnomalyDetectionConfigResponse deleteResponse =
          DeleteScopedAnomalyDetectionConfigResponse.newBuilder().build();
      when(detectorConfigStub.deleteScopedAnomalyDetectionConfig(any())).thenReturn(deleteResponse);

      // When
      requestContext.run(() -> service.deleteAiAppRules(request, deleteResponseObserver));

      // Then
      verify(detectorConfigStub).deleteScopedAnomalyDetectionConfig(any());
      verify(deleteResponseObserver).onNext(any(DeleteAiAppRulesResponse.class));
      verify(deleteResponseObserver).onCompleted();
      verify(deleteResponseObserver, never()).onError(any());
    }

    @Test
    @DisplayName("Should delete custom rule successfully")
    void shouldDeleteCustomRuleSuccessfully() {
      // Given
      DeleteAiAppRulesRequest request = createValidDeleteRequestWithCustomRule();
      when(validator.validateDeleteAiAppRulesRequest(request)).thenReturn(Status.OK);

      DeleteCustomSignatureRuleResponse deleteResponse =
          DeleteCustomSignatureRuleResponse.newBuilder().build();
      when(customSignatureConfigStub.deleteCustomSignatureRule(any())).thenReturn(deleteResponse);

      // When
      requestContext.run(() -> service.deleteAiAppRules(request, deleteResponseObserver));

      // Then
      verify(customSignatureConfigStub).deleteCustomSignatureRule(any());
      verify(deleteResponseObserver).onNext(any(DeleteAiAppRulesResponse.class));
      verify(deleteResponseObserver).onCompleted();
    }

    @Test
    @DisplayName("Should delete rate limiting rule successfully")
    void shouldDeleteRateLimitingRuleSuccessfully() {
      // Given
      DeleteAiAppRulesRequest request = createValidDeleteRequestWithRateLimitingRule();
      when(validator.validateDeleteAiAppRulesRequest(request)).thenReturn(Status.OK);

      DeleteRateLimitingRuleResponse deleteResponse =
          DeleteRateLimitingRuleResponse.newBuilder().build();
      when(rateLimitingConfigStub.deleteRateLimitingRule(any())).thenReturn(deleteResponse);

      // When
      requestContext.run(() -> service.deleteAiAppRules(request, deleteResponseObserver));

      // Then
      verify(rateLimitingConfigStub).deleteRateLimitingRule(any());
      verify(deleteResponseObserver).onNext(any(DeleteAiAppRulesResponse.class));
      verify(deleteResponseObserver).onCompleted();
    }

    @Test
    @DisplayName("Should delete AI sensitive data protection rule successfully")
    void shouldDeleteAiSensitiveDataProtectionRuleSuccessfully() {
      // Given
      DeleteAiAppRulesRequest request = createValidDeleteRequestForAiSensitiveDataProtection();
      when(validator.validateDeleteAiAppRulesRequest(request)).thenReturn(Status.OK);

      DeleteRateLimitingRuleResponse deleteResponse =
          DeleteRateLimitingRuleResponse.newBuilder().build();
      when(rateLimitingConfigStub.deleteRateLimitingRule(any())).thenReturn(deleteResponse);

      // When
      requestContext.run(() -> service.deleteAiAppRules(request, deleteResponseObserver));

      // Then
      verify(rateLimitingConfigStub).deleteRateLimitingRule(any());
      verify(customSignatureConfigStub, never()).deleteCustomSignatureRule(any());
      verify(deleteResponseObserver).onNext(any(DeleteAiAppRulesResponse.class));
      verify(deleteResponseObserver).onCompleted();
    }

    @Test
    @DisplayName("Should handle validation error")
    void shouldHandleValidationError() {
      // Given
      DeleteAiAppRulesRequest request = createValidDeleteRequestWithResetToDefault();
      Status validationError = Status.INVALID_ARGUMENT.withDescription("Invalid delete request");
      when(validator.validateDeleteAiAppRulesRequest(request)).thenReturn(validationError);

      // When
      requestContext.run(() -> service.deleteAiAppRules(request, deleteResponseObserver));

      // Then
      verify(deleteResponseObserver)
          .onError(
              argThat(
                  throwable ->
                      throwable instanceof StatusRuntimeException
                          && ((StatusRuntimeException) throwable).getStatus().getCode()
                              == Status.Code.INVALID_ARGUMENT
                          && ((StatusRuntimeException) throwable)
                              .getStatus()
                              .getDescription()
                              .equals("Invalid delete request")));
    }
  }

  @Nested
  @DisplayName("CreateAiAppCustomRule Tests")
  class CreateAiAppCustomRuleTests {

    @Test
    @DisplayName("Should create rate limiting rule successfully")
    void shouldCreateRateLimitingRuleSuccessfully() {
      // Given
      CreateAiAppCustomRuleRequest request = createValidCreateRequestForRateLimiting();
      when(validator.validateCreateAiAppCustomRuleRequest(request)).thenReturn(Status.OK);

      // Mock converter method calls
      CreateRateLimitingRuleRequest mockCreateRequest =
          CreateRateLimitingRuleRequest.newBuilder()
              .setData(RateLimitingRuleData.newBuilder().build())
              .build();
      when(aiAppToRateLimitingConverter.convertToCreateRateLimitingRuleRequest(any()))
          .thenReturn(mockCreateRequest);

      RateLimitingRule mockCreatedRule =
          RateLimitingRule.newBuilder()
              .setId("rate-limit-rule-123")
              .setData(RateLimitingRuleData.newBuilder().build())
              .build();
      CreateRateLimitingRuleResponse createResponse =
          CreateRateLimitingRuleResponse.newBuilder().setRule(mockCreatedRule).build();
      when(rateLimitingConfigStub.createRateLimitingRule(any())).thenReturn(createResponse);

      AiAppCustomRule mockAiAppCustomRule =
          AiAppCustomRule.newBuilder()
              .setRuleId("rate-limit-rule-123")
              .setRuleData(AiAppCustomRuleData.newBuilder().build())
              .build();
      when(rateLimitingToAiAppConverter.convertFromRateLimitingRule(any()))
          .thenReturn(mockAiAppCustomRule);

      // When
      requestContext.run(() -> service.createAiAppCustomRule(request, createResponseObserver));

      // Then
      verify(aiAppToRateLimitingConverter).convertToCreateRateLimitingRuleRequest(any());
      verify(rateLimitingConfigStub).createRateLimitingRule(any());
      verify(rateLimitingToAiAppConverter).convertFromRateLimitingRule(any());
      verify(createResponseObserver).onNext(any(CreateAiAppCustomRuleResponse.class));
      verify(createResponseObserver).onCompleted();
    }

    @Test
    @DisplayName("Should create AI sensitive data protection rule successfully")
    void shouldCreateAiSensitiveDataProtectionRuleSuccessfully() {
      // Given
      CreateAiAppCustomRuleRequest request = createValidCreateRequestForAiSensitiveDataProtection();
      when(validator.validateCreateAiAppCustomRuleRequest(request)).thenReturn(Status.OK);

      // Mock converter method calls
      CreateRateLimitingRuleRequest mockCreateRequest =
          CreateRateLimitingRuleRequest.newBuilder()
              .setData(RateLimitingRuleData.newBuilder().build())
              .build();
      when(aiAppToRateLimitingConverter.convertToCreateRateLimitingRuleRequest(any()))
          .thenReturn(mockCreateRequest);

      RateLimitingRule mockCreatedRule =
          RateLimitingRule.newBuilder()
              .setId("sdp-rule-123")
              .setData(RateLimitingRuleData.newBuilder().build())
              .build();
      CreateRateLimitingRuleResponse createResponse =
          CreateRateLimitingRuleResponse.newBuilder().setRule(mockCreatedRule).build();
      when(rateLimitingConfigStub.createRateLimitingRule(any())).thenReturn(createResponse);

      AiAppCustomRule mockAiAppCustomRule =
          AiAppCustomRule.newBuilder()
              .setRuleId("sdp-rule-123")
              .setRuleData(AiAppCustomRuleData.newBuilder().build())
              .build();
      when(rateLimitingToAiAppConverter.convertFromRateLimitingRule(any()))
          .thenReturn(mockAiAppCustomRule);

      // When
      requestContext.run(() -> service.createAiAppCustomRule(request, createResponseObserver));

      // Then
      verify(aiAppToRateLimitingConverter).convertToCreateRateLimitingRuleRequest(any());
      verify(rateLimitingConfigStub).createRateLimitingRule(any());
      verify(rateLimitingToAiAppConverter).convertFromRateLimitingRule(any());
      verify(aiAppToCustomSignatureConverter, never())
          .convertToCreateCustomSignatureRuleRequest(any());
      verify(customSignatureConfigStub, never()).createCustomSignatureRule(any());
      verify(createResponseObserver).onNext(any(CreateAiAppCustomRuleResponse.class));
      verify(createResponseObserver).onCompleted();
    }

    @Test
    @DisplayName("Should create custom signature rule successfully")
    void shouldCreateCustomSignatureRuleSuccessfully() {
      // Given
      CreateAiAppCustomRuleRequest request = createValidCreateRequestForCustomSignature();
      when(validator.validateCreateAiAppCustomRuleRequest(request)).thenReturn(Status.OK);

      // Mock converter method calls
      CreateCustomSignatureRuleRequest mockCreateRequest =
          CreateCustomSignatureRuleRequest.newBuilder()
              .setDefinition(RuleDefinition.newBuilder().build())
              .build();
      when(aiAppToCustomSignatureConverter.convertToCreateCustomSignatureRuleRequest(any()))
          .thenReturn(mockCreateRequest);

      CustomSignatureRule mockCreatedRule =
          CustomSignatureRule.newBuilder()
              .setId("custom-sig-rule-123")
              .setDefinition(RuleDefinition.newBuilder().build())
              .build();
      CreateCustomSignatureRuleResponse createResponse =
          CreateCustomSignatureRuleResponse.newBuilder().setRule(mockCreatedRule).build();
      when(customSignatureConfigStub.createCustomSignatureRule(any())).thenReturn(createResponse);

      AiAppCustomRule mockAiAppCustomRule =
          AiAppCustomRule.newBuilder()
              .setRuleId("custom-sig-rule-123")
              .setRuleData(AiAppCustomRuleData.newBuilder().build())
              .build();
      when(customSignatureToAiAppConverter.convertFromCustomSignatureRule(any()))
          .thenReturn(mockAiAppCustomRule);

      // When
      requestContext.run(() -> service.createAiAppCustomRule(request, createResponseObserver));

      // Then
      verify(aiAppToCustomSignatureConverter).convertToCreateCustomSignatureRuleRequest(any());
      verify(customSignatureConfigStub).createCustomSignatureRule(any());
      verify(customSignatureToAiAppConverter).convertFromCustomSignatureRule(any());
      verify(createResponseObserver).onNext(any(CreateAiAppCustomRuleResponse.class));
      verify(createResponseObserver).onCompleted();
    }

    @Test
    @DisplayName("Should handle validation error")
    void shouldHandleValidationError() {
      // Given
      CreateAiAppCustomRuleRequest request = createValidCreateRequest();
      Status validationError = Status.INVALID_ARGUMENT.withDescription("Invalid create request");
      when(validator.validateCreateAiAppCustomRuleRequest(request)).thenReturn(validationError);

      // When
      requestContext.run(() -> service.createAiAppCustomRule(request, createResponseObserver));

      // Then
      verify(createResponseObserver)
          .onError(
              argThat(
                  throwable ->
                      throwable instanceof StatusRuntimeException
                          && ((StatusRuntimeException) throwable).getStatus().getCode()
                              == Status.Code.INVALID_ARGUMENT
                          && ((StatusRuntimeException) throwable)
                              .getStatus()
                              .getDescription()
                              .equals("Invalid create request")));
      verify(aiAppToRateLimitingConverter, never()).convertToCreateRateLimitingRuleRequest(any());
      verify(aiAppToCustomSignatureConverter, never())
          .convertToCreateCustomSignatureRuleRequest(any());
    }
  }

  @Nested
  @DisplayName("UpsertAiAppCustomRule Tests")
  class UpsertAiAppCustomRuleTests {

    @Test
    @DisplayName("Should upsert custom rule successfully")
    void shouldUpsertCustomRuleSuccessfully() {
      // Given
      UpsertAiAppCustomRuleRequest request = createValidUpsertRequest();
      when(validator.validateUpsertAiAppCustomRuleRequest(request)).thenReturn(Status.OK);

      // Mock converter method calls
      CustomSignatureRule mockCustomSignatureRule =
          CustomSignatureRule.newBuilder()
              .setId("rule-123")
              .setDefinition(RuleDefinition.newBuilder().build())
              .build();
      when(aiAppToCustomSignatureConverter.convertToCustomSignatureRule(any()))
          .thenReturn(mockCustomSignatureRule);

      UpdateCustomSignatureRuleResponse upsertResponse =
          UpdateCustomSignatureRuleResponse.newBuilder().setRule(mockCustomSignatureRule).build();
      when(customSignatureConfigStub.updateCustomSignatureRule(any())).thenReturn(upsertResponse);

      AiAppCustomRule mockAiAppCustomRule =
          AiAppCustomRule.newBuilder()
              .setRuleId("rule-123")
              .setRuleData(AiAppCustomRuleData.newBuilder().build())
              .build();
      when(customSignatureToAiAppConverter.convertFromCustomSignatureRule(any()))
          .thenReturn(mockAiAppCustomRule);

      // When
      requestContext.run(() -> service.upsertAiAppCustomRule(request, upsertResponseObserver));

      // Then
      verify(aiAppToCustomSignatureConverter).convertToCustomSignatureRule(any());
      verify(customSignatureConfigStub).updateCustomSignatureRule(any());
      verify(customSignatureToAiAppConverter).convertFromCustomSignatureRule(any());
      verify(upsertResponseObserver).onNext(any(UpsertAiAppCustomRuleResponse.class));
      verify(upsertResponseObserver).onCompleted();
    }

    @Test
    @DisplayName("Should upsert AI sensitive data protection rule successfully")
    void shouldUpsertAiSensitiveDataProtectionRuleSuccessfully() {
      // Given
      UpsertAiAppCustomRuleRequest request = createValidUpsertRequestForAiSensitiveDataProtection();
      when(validator.validateUpsertAiAppCustomRuleRequest(request)).thenReturn(Status.OK);

      // Mock converter method calls
      RateLimitingRuleData mockRateLimitingRuleData = RateLimitingRuleData.newBuilder().build();
      when(aiAppToRateLimitingConverter.convertToRateLimitingRule(any()))
          .thenReturn(mockRateLimitingRuleData);

      RateLimitingRule mockUpdatedRule =
          RateLimitingRule.newBuilder()
              .setId("rule-123")
              .setData(RateLimitingRuleData.newBuilder().build())
              .build();
      UpdateRateLimitingRuleResponse updateResponse =
          UpdateRateLimitingRuleResponse.newBuilder().setRule(mockUpdatedRule).build();
      when(rateLimitingConfigStub.updateRateLimitingRule(any())).thenReturn(updateResponse);

      AiAppCustomRule mockAiAppCustomRule =
          AiAppCustomRule.newBuilder()
              .setRuleId("rule-123")
              .setRuleData(AiAppCustomRuleData.newBuilder().build())
              .build();
      when(rateLimitingToAiAppConverter.convertFromRateLimitingRule(any()))
          .thenReturn(mockAiAppCustomRule);

      // When
      requestContext.run(() -> service.upsertAiAppCustomRule(request, upsertResponseObserver));

      // Then
      verify(aiAppToRateLimitingConverter).convertToRateLimitingRule(any());
      verify(rateLimitingConfigStub).updateRateLimitingRule(any());
      verify(rateLimitingToAiAppConverter).convertFromRateLimitingRule(any());
      verify(aiAppToCustomSignatureConverter, never()).convertToCustomSignatureRule(any());
      verify(customSignatureConfigStub, never()).updateCustomSignatureRule(any());
      verify(upsertResponseObserver).onNext(any(UpsertAiAppCustomRuleResponse.class));
      verify(upsertResponseObserver).onCompleted();
    }

    @Test
    @DisplayName("Should handle validation error")
    void shouldHandleValidationError() {
      // Given
      UpsertAiAppCustomRuleRequest request = createValidUpsertRequest();
      Status validationError = Status.INVALID_ARGUMENT.withDescription("Invalid upsert request");
      when(validator.validateUpsertAiAppCustomRuleRequest(request)).thenReturn(validationError);

      // When
      requestContext.run(() -> service.upsertAiAppCustomRule(request, upsertResponseObserver));

      // Then
      verify(upsertResponseObserver)
          .onError(
              argThat(
                  throwable ->
                      throwable instanceof StatusRuntimeException
                          && ((StatusRuntimeException) throwable).getStatus().getCode()
                              == Status.Code.INVALID_ARGUMENT
                          && ((StatusRuntimeException) throwable)
                              .getStatus()
                              .getDescription()
                              .equals("Invalid upsert request")));
      verify(aiAppToCustomSignatureConverter, never()).convertToCustomSignatureRule(any());
      verify(customSignatureConfigStub, never()).updateCustomSignatureRule(any());
    }
  }

  // Helper methods to create test data
  private GetAiAppRulesRequest createValidGetRequest() {
    return GetAiAppRulesRequest.newBuilder()
        .setRuleScope(
            ai.traceable.aiapp.protection.config.service.v1.RuleScope.newBuilder()
                .setEnvironmentScope(
                    ai.traceable.aiapp.protection.config.service.v1.EnvironmentScope.newBuilder()
                        .addEnvironmentIds("env-1")
                        .build())
                .build())
        .build();
  }

  private CreateAiAppCustomRuleRequest createValidCreateRequestForRateLimiting() {
    return CreateAiAppCustomRuleRequest.newBuilder()
        .setAiAppCustomRuleData(
            AiAppCustomRuleData.newBuilder()
                .setRuleName("Test Rate Limiting Rule")
                .setPiiDetectedInPromptRuleData(PiiDetectedInPromptRuleData.newBuilder().build())
                .build())
        .build();
  }

  private CreateAiAppCustomRuleRequest createValidCreateRequestForCustomSignature() {
    return CreateAiAppCustomRuleRequest.newBuilder()
        .setAiAppCustomRuleData(
            AiAppCustomRuleData.newBuilder()
                .setRuleName("Test Custom Signature Rule")
                .setModelGovernanceRuleData(ModelGovernanceRuleData.newBuilder().build())
                .build())
        .build();
  }

  private CreateAiAppCustomRuleRequest createValidCreateRequestForAiSensitiveDataProtection() {
    return CreateAiAppCustomRuleRequest.newBuilder()
        .setAiAppCustomRuleData(
            AiAppCustomRuleData.newBuilder()
                .setRuleName("Test AI Sensitive Data Protection Rule")
                .setAiSensitiveDataProtectionRuleData(
                    AiSensitiveDataProtectionRuleData.newBuilder().build())
                .build())
        .build();
  }

  private UpdateAiAppRulesRequest createValidUpdateRequest() {
    return UpdateAiAppRulesRequest.newBuilder()
        .setRuleScope(
            ai.traceable.aiapp.protection.config.service.v1.RuleScope.newBuilder()
                .setEnvironmentScope(
                    ai.traceable.aiapp.protection.config.service.v1.EnvironmentScope.newBuilder()
                        .addEnvironmentIds("env-1")
                        .build())
                .build())
        .addAiAppRuleUpdates(
            AiAppRuleUpdate.newBuilder()
                .setRuleId("rule-123")
                .setRuleStatusChange(RuleStatusChange.newBuilder().setDisabled(true).build())
                .build())
        .build();
  }

  private DeleteAiAppRulesRequest createValidDeleteRequestWithResetToDefault() {
    return DeleteAiAppRulesRequest.newBuilder()
        .setResetToDefault(
            ResetToDefault.newBuilder()
                .addRuleIds("rule-1")
                .setRuleScope(
                    ai.traceable.aiapp.protection.config.service.v1.RuleScope.newBuilder()
                        .setEnvironmentScope(
                            ai.traceable.aiapp.protection.config.service.v1.EnvironmentScope
                                .newBuilder()
                                .addEnvironmentIds("env-1")
                                .build())
                        .build())
                .build())
        .build();
  }

  private DeleteAiAppRulesRequest createValidDeleteRequestWithCustomRule() {
    return DeleteAiAppRulesRequest.newBuilder()
        .setCustomRuleToDelete(
            AiAppCustomRuleToDelete.newBuilder()
                .setRuleId("custom-rule-123")
                .setCustomRuleType(AiAppCustomRuleType.AI_APP_CUSTOM_RULE_TYPE_MODEL_GOVERNANCE)
                .build())
        .build();
  }

  private DeleteAiAppRulesRequest createValidDeleteRequestWithRateLimitingRule() {
    return DeleteAiAppRulesRequest.newBuilder()
        .setCustomRuleToDelete(
            AiAppCustomRuleToDelete.newBuilder()
                .setRuleId("rate-limit-rule-123")
                .setCustomRuleType(
                    AiAppCustomRuleType.AI_APP_CUSTOM_RULE_TYPE_PII_DETECTED_IN_PROMPT)
                .build())
        .build();
  }

  private DeleteAiAppRulesRequest createValidDeleteRequestForAiSensitiveDataProtection() {
    return DeleteAiAppRulesRequest.newBuilder()
        .setCustomRuleToDelete(
            AiAppCustomRuleToDelete.newBuilder()
                .setRuleId("sdp-rule-123")
                .setCustomRuleType(
                    AiAppCustomRuleType.AI_APP_CUSTOM_RULE_TYPE_SENSITIVE_DATA_PROTECTION)
                .build())
        .build();
  }

  private CreateAiAppCustomRuleRequest createValidCreateRequest() {
    return CreateAiAppCustomRuleRequest.newBuilder()
        .setAiAppCustomRuleData(
            AiAppCustomRuleData.newBuilder()
                .setRuleName("Test Rule")
                .setPiiDetectedInPromptRuleData(PiiDetectedInPromptRuleData.newBuilder().build())
                .build())
        .build();
  }

  private UpsertAiAppCustomRuleRequest createValidUpsertRequest() {
    return UpsertAiAppCustomRuleRequest.newBuilder()
        .setAiAppCustomRule(
            AiAppCustomRule.newBuilder()
                .setRuleId("rule-123")
                .setRuleData(
                    AiAppCustomRuleData.newBuilder()
                        .setRuleName("Test Rule")
                        .setAiInputExplosionRuleData(AiInputExplosionRuleData.getDefaultInstance())
                        .build())
                .build())
        .build();
  }

  private UpsertAiAppCustomRuleRequest createValidUpsertRequestForAiSensitiveDataProtection() {
    return UpsertAiAppCustomRuleRequest.newBuilder()
        .setAiAppCustomRule(
            AiAppCustomRule.newBuilder()
                .setRuleId("rule-123")
                .setRuleData(
                    AiAppCustomRuleData.newBuilder()
                        .setRuleName("Test AI Sensitive Data Protection Rule")
                        .setAiSensitiveDataProtectionRuleData(
                            AiSensitiveDataProtectionRuleData.newBuilder().build())
                        .build())
                .build())
        .build();
  }

  private GetAnomalyRuleInfosResponse createMockAnomalyRuleInfosResponse() {
    return GetAnomalyRuleInfosResponse.newBuilder()
        .addRuleInfos(
            AnomalyRuleInfo.newBuilder()
                .setRuleId("anomaly-rule-1")
                .setRuleName("Test Anomaly Rule")
                .build())
        .build();
  }

  private GetScopedAnomalyDetectionConfigResponse createMockScopedConfigResponse() {
    return GetScopedAnomalyDetectionConfigResponse.newBuilder()
        .setScopedAnomalyDetectionConfig(ScopedAnomalyDetectionConfig.newBuilder().build())
        .build();
  }

  private GetAllUnresolvedScopedAnomalyDetectionConfigsResponse
      createMockUnresolvedConfigsResponse() {
    return GetAllUnresolvedScopedAnomalyDetectionConfigsResponse.newBuilder().build();
  }

  private GetRateLimitingRulesResponse createMockRateLimitingResponse() {
    return GetRateLimitingRulesResponse.newBuilder().build();
  }

  private GetCustomSignatureRulesResponse createMockCustomSignatureResponse() {
    return GetCustomSignatureRulesResponse.newBuilder().build();
  }

  private List<AiAppRule> createMockAiAppRules() {
    return Arrays.asList(
        AiAppRule.newBuilder().setRuleId("rule-1").setRuleName("Test Rule 1").build(),
        AiAppRule.newBuilder().setRuleId("rule-2").setRuleName("Test Rule 2").build());
  }
}
