package ai.traceable.risk.config.service.v2.contributors.validator;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;

import ai.traceable.risk.config.service.v2.EntityType;
import ai.traceable.risk.config.service.v2.GetRiskContributorConfigsRequest;
import ai.traceable.risk.config.service.v2.ResetRiskContributorConfigsRequest;
import ai.traceable.risk.config.service.v2.RiskConfigScope;
import ai.traceable.risk.config.service.v2.RiskConfigServiceRequestValidator;
import ai.traceable.risk.config.service.v2.RiskContributorConfigs;
import ai.traceable.risk.config.service.v2.RiskElementConfigUpdateDetails;
import ai.traceable.risk.config.service.v2.RiskElementConfigUpdates;
import ai.traceable.risk.config.service.v2.RiskElementScoring;
import ai.traceable.risk.config.service.v2.RiskFactor;
import ai.traceable.risk.config.service.v2.RiskFactorCategory;
import ai.traceable.risk.config.service.v2.RiskFactorConfig;
import ai.traceable.risk.config.service.v2.RiskFactorConfigUpdateDetails;
import ai.traceable.risk.config.service.v2.RiskFactorInfo;
import ai.traceable.risk.config.service.v2.UpdateRiskContributorConfigsRequest;
import ai.traceable.risk.config.service.v2.factors.validator.RiskFactorConfigsValidatorImpl;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RiskContributorConfigsValidatorImplTest {

  @Mock private RiskFactorConfigsValidatorImpl mockFactorConfigsValidator;
  @Mock private RiskConfigServiceRequestValidator mockRequestValidator;

  @InjectMocks private RiskContributorConfigsValidatorImpl riskContributorConfigsValidator;

  @Test
  void testValidateGetRiskContributorConfigsRequestWithInvalidRequestContext() {
    final RequestContext requestContext = new RequestContext();
    final GetRiskContributorConfigsRequest request = buildGetRequestWithValidScope();
    doThrow(StatusRuntimeException.class)
        .when(mockRequestValidator)
        .validateContextAndScope(requestContext, request.getRiskConfigScope());
    assertThrows(
        StatusRuntimeException.class,
        () ->
            riskContributorConfigsValidator.validateGetRiskContributorConfigsRequest(
                requestContext, request));
  }

  @Test
  void testValidateGetRiskContributorConfigsRequestWithInvalidScope() {
    final RequestContext requestContext = RequestContext.forTenantId("tenant");
    final GetRiskContributorConfigsRequest request = buildGetRequestWithInvalidScope();
    doThrow(StatusRuntimeException.class)
        .when(mockRequestValidator)
        .validateContextAndScope(requestContext, request.getRiskConfigScope());
    assertThrows(
        StatusRuntimeException.class,
        () ->
            riskContributorConfigsValidator.validateGetRiskContributorConfigsRequest(
                requestContext, request));
  }

  @Test
  void testValidateGetRiskContributorConfigsRequestWithValidArgs() {
    final RequestContext requestContext = RequestContext.forTenantId("tenant");
    final GetRiskContributorConfigsRequest request = buildGetRequestWithValidScope();
    doNothing()
        .when(mockRequestValidator)
        .validateContextAndScope(requestContext, request.getRiskConfigScope());
    assertDoesNotThrow(
        () ->
            riskContributorConfigsValidator.validateGetRiskContributorConfigsRequest(
                requestContext, request));
  }

  @Test
  void testValidateUpdateRiskContributorConfigsRequestWithEmptyUpdates() {
    final RequestContext requestContext = RequestContext.forTenantId("tenant");
    final UpdateRiskContributorConfigsRequest request = buildUpdateRequestWithEmptyUpdates();
    doNothing()
        .when(mockRequestValidator)
        .validateContextAndScope(requestContext, request.getRiskConfigScope());
    assertThrows(
        StatusRuntimeException.class,
        () ->
            riskContributorConfigsValidator.validateUpdateRiskContributorConfigsRequest(
                requestContext, request));
  }

  @Test
  void testValidateUpdateRiskContributorConfigsRequestWithEmptyCategory() {
    final RequestContext requestContext = RequestContext.forTenantId("tenant");
    final UpdateRiskContributorConfigsRequest request = buildUpdateRequestWithEmptyCategory();
    doNothing()
        .when(mockRequestValidator)
        .validateContextAndScope(requestContext, request.getRiskConfigScope());
    doThrow(StatusRuntimeException.class)
        .when(mockFactorConfigsValidator)
        .validateRiskFactorConfigUpdateDetails(request.getRiskFactorConfigUpdateDetailsList());
    assertThrows(
        StatusRuntimeException.class,
        () ->
            riskContributorConfigsValidator.validateUpdateRiskContributorConfigsRequest(
                requestContext, request));
  }

  @Test
  void testValidateUpdateRiskContributorConfigsRequestWithEmptyUpdateCase() {
    final RequestContext requestContext = RequestContext.forTenantId("tenant");
    final UpdateRiskContributorConfigsRequest request = buildUpdateRequestWithEmptyUpdateCase();
    doNothing()
        .when(mockRequestValidator)
        .validateContextAndScope(requestContext, request.getRiskConfigScope());
    doThrow(StatusRuntimeException.class)
        .when(mockFactorConfigsValidator)
        .validateRiskFactorConfigUpdateDetails(request.getRiskFactorConfigUpdateDetailsList());
    assertThrows(
        StatusRuntimeException.class,
        () ->
            riskContributorConfigsValidator.validateUpdateRiskContributorConfigsRequest(
                requestContext, request));
  }

  @Test
  void testValidateUpdateRiskContributorConfigsRequestWithDisabledUpdate() {
    final RequestContext requestContext = RequestContext.forTenantId("tenant");
    final UpdateRiskContributorConfigsRequest request = buildUpdateRequestWithDisabledUpdate();
    doNothing()
        .when(mockRequestValidator)
        .validateContextAndScope(requestContext, request.getRiskConfigScope());
    doNothing()
        .when(mockFactorConfigsValidator)
        .validateRiskFactorConfigUpdateDetails(request.getRiskFactorConfigUpdateDetailsList());
    assertDoesNotThrow(
        () ->
            riskContributorConfigsValidator.validateUpdateRiskContributorConfigsRequest(
                requestContext, request));
  }

  @Test
  void testValidateUpdateRiskContributorConfigsRequestWithEmptyElementConfigUpdateList() {
    final RequestContext requestContext = RequestContext.forTenantId("tenant");
    final UpdateRiskContributorConfigsRequest request =
        buildUpdateRequestWithWithEmptyElementConfigUpdateList();
    doNothing()
        .when(mockRequestValidator)
        .validateContextAndScope(requestContext, request.getRiskConfigScope());
    doThrow(StatusRuntimeException.class)
        .when(mockFactorConfigsValidator)
        .validateRiskFactorConfigUpdateDetails(request.getRiskFactorConfigUpdateDetailsList());
    assertThrows(
        StatusRuntimeException.class,
        () ->
            riskContributorConfigsValidator.validateUpdateRiskContributorConfigsRequest(
                requestContext, request));
  }

  @Test
  void testValidateUpdateRiskContributorConfigsRequestWithInvalidElementId() {
    final RequestContext requestContext = RequestContext.forTenantId("tenant");
    final UpdateRiskContributorConfigsRequest request =
        buildUpdateRequestWithWithInvalidElementId();
    doNothing()
        .when(mockRequestValidator)
        .validateContextAndScope(requestContext, request.getRiskConfigScope());
    doThrow(StatusRuntimeException.class)
        .when(mockFactorConfigsValidator)
        .validateRiskFactorConfigUpdateDetails(request.getRiskFactorConfigUpdateDetailsList());
    assertThrows(
        StatusRuntimeException.class,
        () ->
            riskContributorConfigsValidator.validateUpdateRiskContributorConfigsRequest(
                requestContext, request));
  }

  @Test
  void testValidateUpdateRiskContributorConfigsRequestWithNoElementScoring() {
    final RequestContext requestContext = RequestContext.forTenantId("tenant");
    final UpdateRiskContributorConfigsRequest request =
        buildUpdateRequestWithWithNoElementScoring();
    doNothing()
        .when(mockRequestValidator)
        .validateContextAndScope(requestContext, request.getRiskConfigScope());
    doThrow(StatusRuntimeException.class)
        .when(mockFactorConfigsValidator)
        .validateRiskFactorConfigUpdateDetails(request.getRiskFactorConfigUpdateDetailsList());
    assertThrows(
        StatusRuntimeException.class,
        () ->
            riskContributorConfigsValidator.validateUpdateRiskContributorConfigsRequest(
                requestContext, request));
  }

  @Test
  void testValidateUpdateRiskContributorConfigsRequestWithNoScore() {
    final RequestContext requestContext = RequestContext.forTenantId("tenant");
    final UpdateRiskContributorConfigsRequest request = buildUpdateRequestWithWithNoScore();
    doNothing()
        .when(mockRequestValidator)
        .validateContextAndScope(requestContext, request.getRiskConfigScope());
    doThrow(StatusRuntimeException.class)
        .when(mockFactorConfigsValidator)
        .validateRiskFactorConfigUpdateDetails(request.getRiskFactorConfigUpdateDetailsList());
    assertThrows(
        StatusRuntimeException.class,
        () ->
            riskContributorConfigsValidator.validateUpdateRiskContributorConfigsRequest(
                requestContext, request));
  }

  @Test
  void testValidateUpdateRiskContributorConfigsRequestWithInvalidScore() {
    final RequestContext requestContext = RequestContext.forTenantId("tenant");
    final UpdateRiskContributorConfigsRequest request = buildUpdateRequestWithWithInvalidScore();
    doNothing()
        .when(mockRequestValidator)
        .validateContextAndScope(requestContext, request.getRiskConfigScope());
    doThrow(StatusRuntimeException.class)
        .when(mockFactorConfigsValidator)
        .validateRiskFactorConfigUpdateDetails(request.getRiskFactorConfigUpdateDetailsList());
    assertThrows(
        StatusRuntimeException.class,
        () ->
            riskContributorConfigsValidator.validateUpdateRiskContributorConfigsRequest(
                requestContext, request));
  }

  @Test
  void testValidateUpdateRiskContributorConfigsRequestWithValidScore() {
    final RequestContext requestContext = RequestContext.forTenantId("tenant");
    final UpdateRiskContributorConfigsRequest request = buildUpdateRequestWithWithValidScore();
    doNothing()
        .when(mockRequestValidator)
        .validateContextAndScope(requestContext, request.getRiskConfigScope());
    doNothing()
        .when(mockFactorConfigsValidator)
        .validateRiskFactorConfigUpdateDetails(request.getRiskFactorConfigUpdateDetailsList());
    assertDoesNotThrow(
        () ->
            riskContributorConfigsValidator.validateUpdateRiskContributorConfigsRequest(
                requestContext, request));
  }

  @Test
  void testValidateResetRiskContributorConfigsRequestWithEmptyFactorCategories() {
    final RequestContext requestContext = RequestContext.forTenantId("tenant");
    final ResetRiskContributorConfigsRequest request = buildResetRequestWthEmptyFactorCategories();
    doNothing()
        .when(mockRequestValidator)
        .validateContextAndScope(requestContext, request.getRiskConfigScope());
    assertThrows(
        StatusRuntimeException.class,
        () ->
            riskContributorConfigsValidator.validateResetRiskContributorConfigsRequest(
                requestContext, request));
  }

  @Test
  void testValidateResetRiskContributorConfigsRequestWithInvalidFactorCategory() {
    final RequestContext requestContext = RequestContext.forTenantId("tenant");
    final ResetRiskContributorConfigsRequest request = buildResetRequestWthInvalidFactorCategory();
    doNothing()
        .when(mockRequestValidator)
        .validateContextAndScope(requestContext, request.getRiskConfigScope());
    doReturn(Status.INVALID_ARGUMENT)
        .when(mockRequestValidator)
        .validateFactorCategory(request.getRiskFactorCategories(0));
    assertThrows(
        StatusRuntimeException.class,
        () ->
            riskContributorConfigsValidator.validateResetRiskContributorConfigsRequest(
                requestContext, request));
  }

  @Test
  void testValidateResetRiskContributorConfigsRequestWithValidFactorCategory() {
    final RequestContext requestContext = RequestContext.forTenantId("tenant");
    final ResetRiskContributorConfigsRequest request = buildResetRequestWthValidFactorCategory();
    doNothing()
        .when(mockRequestValidator)
        .validateContextAndScope(requestContext, request.getRiskConfigScope());
    doReturn(Status.OK)
        .when(mockRequestValidator)
        .validateFactorCategory(request.getRiskFactorCategories(0));
    assertDoesNotThrow(
        () ->
            riskContributorConfigsValidator.validateResetRiskContributorConfigsRequest(
                requestContext, request));
  }

  @Test
  void testValidateRiskContributorConfigsWithNullConfig() {
    assertEquals(
        Status.NOT_FOUND.getCode(),
        riskContributorConfigsValidator.validateRiskContributorConfigs(null).getCode());
  }

  @Test
  void testValidateRiskContributorConfigsWithEmptyFactors() {
    assertEquals(
        Status.NOT_FOUND.getCode(),
        riskContributorConfigsValidator
            .validateRiskContributorConfigs(RiskContributorConfigs.getDefaultInstance())
            .getCode());
  }

  @Test
  void testValidateRiskContributorConfigsWithInvalidFactorConfig() {
    doReturn(Status.INVALID_ARGUMENT)
        .when(mockFactorConfigsValidator)
        .validateRiskFactorConfig(any(RiskFactorConfig.class));
    assertEquals(
        Status.INVALID_ARGUMENT.getCode(),
        riskContributorConfigsValidator
            .validateRiskContributorConfigs(
                RiskContributorConfigs.newBuilder()
                    .addRiskFactors(
                        RiskFactor.newBuilder()
                            .setRiskFactorConfig(RiskFactorConfig.getDefaultInstance()))
                    .build())
            .getCode());
  }

  @Test
  void testValidateRiskContributorConfigsWithInvalidFactorInfo() {
    doReturn(Status.OK)
        .when(mockFactorConfigsValidator)
        .validateRiskFactorConfig(any(RiskFactorConfig.class));
    doReturn(Status.INVALID_ARGUMENT)
        .when(mockFactorConfigsValidator)
        .validateRiskFactorInfo(any(RiskFactorInfo.class));
    assertEquals(
        Status.INVALID_ARGUMENT.getCode(),
        riskContributorConfigsValidator
            .validateRiskContributorConfigs(
                RiskContributorConfigs.newBuilder()
                    .addRiskFactors(
                        RiskFactor.newBuilder()
                            .setRiskFactorConfig(RiskFactorConfig.getDefaultInstance())
                            .setRiskFactorInfo(RiskFactorInfo.getDefaultInstance()))
                    .build())
            .getCode());
  }

  @Test
  void testValidateRiskContributorConfigsWithValidConfig() {
    doReturn(Status.OK)
        .when(mockFactorConfigsValidator)
        .validateRiskFactorConfig(any(RiskFactorConfig.class));
    doReturn(Status.OK)
        .when(mockFactorConfigsValidator)
        .validateRiskFactorInfo(any(RiskFactorInfo.class));
    assertEquals(
        Status.OK.getCode(),
        riskContributorConfigsValidator
            .validateRiskContributorConfigs(
                RiskContributorConfigs.newBuilder()
                    .addRiskFactors(
                        RiskFactor.newBuilder()
                            .setRiskFactorConfig(RiskFactorConfig.getDefaultInstance())
                            .setRiskFactorInfo(RiskFactorInfo.getDefaultInstance()))
                    .build())
            .getCode());
  }

  @Test
  void testValidateUpdateRequestRejectsUnsupportedCategoryForMcpTool() {
    // Given
    RequestContext requestContext = RequestContext.forTenantId("tenant");
    RiskConfigScope mcpToolScope =
        RiskConfigScope.newBuilder().setEntityType(EntityType.ENTITY_TYPE_MCP_TOOL).build();
    UpdateRiskContributorConfigsRequest request =
        UpdateRiskContributorConfigsRequest.newBuilder()
            .setRiskConfigScope(mcpToolScope)
            .addRiskFactorConfigUpdateDetails(
                RiskFactorConfigUpdateDetails.newBuilder()
                    .setRiskFactorCategory(RiskFactorCategory.RISK_FACTOR_CATEGORY_BLAST_RADIUS)
                    .setDisabled(true))
            .build();
    doNothing()
        .when(mockRequestValidator)
        .validateContextAndScope(requestContext, request.getRiskConfigScope());
    doNothing()
        .when(mockFactorConfigsValidator)
        .validateRiskFactorConfigUpdateDetails(request.getRiskFactorConfigUpdateDetailsList());
    // When
    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                riskContributorConfigsValidator.validateUpdateRiskContributorConfigsRequest(
                    requestContext, request));
    // Then
    assertEquals(Status.INVALID_ARGUMENT.getCode(), exception.getStatus().getCode());
  }

  @Test
  void testValidateResetRequestRejectsUnsupportedCategoryForMcpTool() {
    // Given
    RequestContext requestContext = RequestContext.forTenantId("tenant");
    RiskConfigScope mcpToolScope =
        RiskConfigScope.newBuilder().setEntityType(EntityType.ENTITY_TYPE_MCP_TOOL).build();
    ResetRiskContributorConfigsRequest request =
        ResetRiskContributorConfigsRequest.newBuilder()
            .setRiskConfigScope(mcpToolScope)
            .addRiskFactorCategories(
                RiskFactorCategory.RISK_FACTOR_CATEGORY_EASE_OF_RESOURCE_DISCOVERY)
            .build();
    doNothing()
        .when(mockRequestValidator)
        .validateContextAndScope(requestContext, request.getRiskConfigScope());
    doReturn(Status.OK)
        .when(mockRequestValidator)
        .validateFactorCategory(request.getRiskFactorCategories(0));
    // When
    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                riskContributorConfigsValidator.validateResetRiskContributorConfigsRequest(
                    requestContext, request));
    // Then
    assertEquals(Status.INVALID_ARGUMENT.getCode(), exception.getStatus().getCode());
  }

  private GetRiskContributorConfigsRequest buildGetRequestWithInvalidScope() {
    return GetRiskContributorConfigsRequest.getDefaultInstance();
  }

  private GetRiskContributorConfigsRequest buildGetRequestWithValidScope() {
    return GetRiskContributorConfigsRequest.newBuilder()
        .setRiskConfigScope(RiskConfigScope.getDefaultInstance())
        .build();
  }

  private UpdateRiskContributorConfigsRequest buildUpdateRequestWithEmptyUpdates() {
    return UpdateRiskContributorConfigsRequest.newBuilder()
        .setRiskConfigScope(RiskConfigScope.getDefaultInstance())
        .build();
  }

  private UpdateRiskContributorConfigsRequest buildUpdateRequestWithEmptyCategory() {
    return UpdateRiskContributorConfigsRequest.newBuilder()
        .setRiskConfigScope(RiskConfigScope.getDefaultInstance())
        .addRiskFactorConfigUpdateDetails(RiskFactorConfigUpdateDetails.getDefaultInstance())
        .build();
  }

  private UpdateRiskContributorConfigsRequest buildUpdateRequestWithEmptyUpdateCase() {
    return UpdateRiskContributorConfigsRequest.newBuilder()
        .setRiskConfigScope(RiskConfigScope.getDefaultInstance())
        .addRiskFactorConfigUpdateDetails(
            RiskFactorConfigUpdateDetails.newBuilder()
                .setRiskFactorCategory(
                    RiskFactorCategory.RISK_FACTOR_CATEGORY_SENSITIVE_DATA_EXPOSURE))
        .build();
  }

  private UpdateRiskContributorConfigsRequest buildUpdateRequestWithDisabledUpdate() {
    return UpdateRiskContributorConfigsRequest.newBuilder()
        .setRiskConfigScope(RiskConfigScope.getDefaultInstance())
        .addRiskFactorConfigUpdateDetails(
            RiskFactorConfigUpdateDetails.newBuilder()
                .setRiskFactorCategory(
                    RiskFactorCategory.RISK_FACTOR_CATEGORY_SENSITIVE_DATA_EXPOSURE)
                .setDisabled(true))
        .build();
  }

  private UpdateRiskContributorConfigsRequest
      buildUpdateRequestWithWithEmptyElementConfigUpdateList() {
    return UpdateRiskContributorConfigsRequest.newBuilder()
        .setRiskConfigScope(RiskConfigScope.getDefaultInstance())
        .addRiskFactorConfigUpdateDetails(
            RiskFactorConfigUpdateDetails.newBuilder()
                .setRiskFactorCategory(
                    RiskFactorCategory.RISK_FACTOR_CATEGORY_SENSITIVE_DATA_EXPOSURE)
                .setDisabled(true)
                .setRiskElementConfigUpdates(RiskElementConfigUpdates.getDefaultInstance()))
        .build();
  }

  private UpdateRiskContributorConfigsRequest buildUpdateRequestWithWithInvalidElementId() {
    return UpdateRiskContributorConfigsRequest.newBuilder()
        .setRiskConfigScope(RiskConfigScope.getDefaultInstance())
        .addRiskFactorConfigUpdateDetails(
            RiskFactorConfigUpdateDetails.newBuilder()
                .setRiskFactorCategory(
                    RiskFactorCategory.RISK_FACTOR_CATEGORY_SENSITIVE_DATA_EXPOSURE)
                .setDisabled(true)
                .setRiskElementConfigUpdates(
                    RiskElementConfigUpdates.newBuilder()
                        .addRiskElementConfigUpdateDetails(
                            RiskElementConfigUpdateDetails.newBuilder().setId(""))))
        .build();
  }

  private UpdateRiskContributorConfigsRequest buildUpdateRequestWithWithNoElementScoring() {
    return UpdateRiskContributorConfigsRequest.newBuilder()
        .setRiskConfigScope(RiskConfigScope.getDefaultInstance())
        .addRiskFactorConfigUpdateDetails(
            RiskFactorConfigUpdateDetails.newBuilder()
                .setRiskFactorCategory(
                    RiskFactorCategory.RISK_FACTOR_CATEGORY_SENSITIVE_DATA_EXPOSURE)
                .setDisabled(true)
                .setRiskElementConfigUpdates(
                    RiskElementConfigUpdates.newBuilder()
                        .addRiskElementConfigUpdateDetails(
                            RiskElementConfigUpdateDetails.newBuilder()
                                .setId("responseSensitivityCritical"))))
        .build();
  }

  private UpdateRiskContributorConfigsRequest buildUpdateRequestWithWithNoScore() {
    return UpdateRiskContributorConfigsRequest.newBuilder()
        .setRiskConfigScope(RiskConfigScope.getDefaultInstance())
        .addRiskFactorConfigUpdateDetails(
            RiskFactorConfigUpdateDetails.newBuilder()
                .setRiskFactorCategory(
                    RiskFactorCategory.RISK_FACTOR_CATEGORY_SENSITIVE_DATA_EXPOSURE)
                .setDisabled(true)
                .setRiskElementConfigUpdates(
                    RiskElementConfigUpdates.newBuilder()
                        .addRiskElementConfigUpdateDetails(
                            RiskElementConfigUpdateDetails.newBuilder()
                                .setId("responseSensitivityCritical")
                                .setRiskElementScoring(RiskElementScoring.getDefaultInstance()))))
        .build();
  }

  private UpdateRiskContributorConfigsRequest buildUpdateRequestWithWithInvalidScore() {
    return UpdateRiskContributorConfigsRequest.newBuilder()
        .setRiskConfigScope(RiskConfigScope.getDefaultInstance())
        .addRiskFactorConfigUpdateDetails(
            RiskFactorConfigUpdateDetails.newBuilder()
                .setRiskFactorCategory(
                    RiskFactorCategory.RISK_FACTOR_CATEGORY_SENSITIVE_DATA_EXPOSURE)
                .setDisabled(true)
                .setRiskElementConfigUpdates(
                    RiskElementConfigUpdates.newBuilder()
                        .addRiskElementConfigUpdateDetails(
                            RiskElementConfigUpdateDetails.newBuilder()
                                .setId("responseSensitivityCritical")
                                .setRiskElementScoring(
                                    RiskElementScoring.newBuilder().setScore(12)))))
        .build();
  }

  private UpdateRiskContributorConfigsRequest buildUpdateRequestWithWithValidScore() {
    return UpdateRiskContributorConfigsRequest.newBuilder()
        .setRiskConfigScope(RiskConfigScope.getDefaultInstance())
        .addRiskFactorConfigUpdateDetails(
            RiskFactorConfigUpdateDetails.newBuilder()
                .setRiskFactorCategory(
                    RiskFactorCategory.RISK_FACTOR_CATEGORY_SENSITIVE_DATA_EXPOSURE)
                .setDisabled(true)
                .setRiskElementConfigUpdates(
                    RiskElementConfigUpdates.newBuilder()
                        .addRiskElementConfigUpdateDetails(
                            RiskElementConfigUpdateDetails.newBuilder()
                                .setId("responseSensitivityCritical")
                                .setRiskElementScoring(
                                    RiskElementScoring.newBuilder().setScore(8)))))
        .build();
  }

  private ResetRiskContributorConfigsRequest buildResetRequestWthEmptyFactorCategories() {
    return ResetRiskContributorConfigsRequest.newBuilder()
        .setRiskConfigScope(RiskConfigScope.getDefaultInstance())
        .build();
  }

  private ResetRiskContributorConfigsRequest buildResetRequestWthInvalidFactorCategory() {
    return ResetRiskContributorConfigsRequest.newBuilder()
        .setRiskConfigScope(RiskConfigScope.getDefaultInstance())
        .addRiskFactorCategories(RiskFactorCategory.RISK_FACTOR_CATEGORY_UNSPECIFIED)
        .build();
  }

  private ResetRiskContributorConfigsRequest buildResetRequestWthValidFactorCategory() {
    return ResetRiskContributorConfigsRequest.newBuilder()
        .setRiskConfigScope(RiskConfigScope.getDefaultInstance())
        .addRiskFactorCategories(RiskFactorCategory.RISK_FACTOR_CATEGORY_SENSITIVE_DATA_EXPOSURE)
        .build();
  }
}
