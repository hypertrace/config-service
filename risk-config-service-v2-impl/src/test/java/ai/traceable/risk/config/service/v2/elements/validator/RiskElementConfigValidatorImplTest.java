package ai.traceable.risk.config.service.v2.elements.validator;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;

import ai.traceable.risk.config.service.v2.AuthCategoryPredicate;
import ai.traceable.risk.config.service.v2.AuthCategoryValue;
import ai.traceable.risk.config.service.v2.EnumOperator;
import ai.traceable.risk.config.service.v2.IntOperator;
import ai.traceable.risk.config.service.v2.IntPredicate;
import ai.traceable.risk.config.service.v2.PathParamTypePredicate;
import ai.traceable.risk.config.service.v2.PathParamTypeValue;
import ai.traceable.risk.config.service.v2.ResponseSensitivityPredicate;
import ai.traceable.risk.config.service.v2.ResponseSensitivityValue;
import ai.traceable.risk.config.service.v2.RiskConfigServiceRequestValidator;
import ai.traceable.risk.config.service.v2.RiskElementConfig;
import ai.traceable.risk.config.service.v2.RiskElementConfigUpdateDetails;
import ai.traceable.risk.config.service.v2.RiskElementConfigUpdates;
import ai.traceable.risk.config.service.v2.RiskElementPredicate;
import ai.traceable.risk.config.service.v2.RiskElementScoring;
import ai.traceable.risk.config.service.v2.StringOperator;
import ai.traceable.risk.config.service.v2.StringPredicate;
import ai.traceable.risk.config.service.v2.VulnerabilitySeverityPredicate;
import ai.traceable.risk.config.service.v2.VulnerabilitySeverityValue;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RiskElementConfigValidatorImplTest {

  @Mock private RiskConfigServiceRequestValidator mockRequestValidator;

  @InjectMocks private RiskElementConfigValidatorImpl elementConfigValidator;

  @Test
  void testValidateRiskElementUpdateDetailsWithEmptyUpdateList() {
    final RiskElementConfigUpdates updates = RiskElementConfigUpdates.getDefaultInstance();
    assertThrows(
        StatusRuntimeException.class,
        () -> elementConfigValidator.validateRiskElementUpdateDetails(updates));
  }

  @Test
  void testValidateRiskElementUpdateDetailsWithEmptyId() {
    final RiskElementConfigUpdates updates =
        RiskElementConfigUpdates.newBuilder()
            .addRiskElementConfigUpdateDetails(RiskElementConfigUpdateDetails.getDefaultInstance())
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () -> elementConfigValidator.validateRiskElementUpdateDetails(updates));
  }

  @Test
  void testValidateRiskElementUpdateDetailsWithNoElementScoring() {
    doReturn(Status.OK).when(mockRequestValidator).validateElementId(any());
    final RiskElementConfigUpdates updates =
        RiskElementConfigUpdates.newBuilder()
            .addRiskElementConfigUpdateDetails(
                RiskElementConfigUpdateDetails.newBuilder().setId("responseSensitivityCritical"))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () -> elementConfigValidator.validateRiskElementUpdateDetails(updates));
  }

  @Test
  void testValidateRiskElementUpdateDetailsWithNoScore() {
    doReturn(Status.OK).when(mockRequestValidator).validateElementId(any());
    final RiskElementConfigUpdates updates =
        RiskElementConfigUpdates.newBuilder()
            .addRiskElementConfigUpdateDetails(
                RiskElementConfigUpdateDetails.newBuilder()
                    .setId("responseSensitivityCritical")
                    .setRiskElementScoring(RiskElementScoring.getDefaultInstance()))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () -> elementConfigValidator.validateRiskElementUpdateDetails(updates));
  }

  @Test
  void testValidateRiskElementUpdateDetailsWithInvalidScore() {
    doReturn(Status.OK).when(mockRequestValidator).validateElementId(any());
    doReturn(Status.OUT_OF_RANGE).when(mockRequestValidator).validateScoreValue(any(Integer.class));
    final RiskElementConfigUpdates updates =
        RiskElementConfigUpdates.newBuilder()
            .addRiskElementConfigUpdateDetails(
                RiskElementConfigUpdateDetails.newBuilder()
                    .setId("responseSensitivityCritical")
                    .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(20)))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () -> elementConfigValidator.validateRiskElementUpdateDetails(updates));
  }

  @Test
  void testValidateRiskElementUpdateDetailsWithValidScore() {
    doReturn(Status.OK).when(mockRequestValidator).validateElementId(any());
    doReturn(Status.OK).when(mockRequestValidator).validateScoreValue(any(Integer.class));
    final RiskElementConfigUpdates updates =
        RiskElementConfigUpdates.newBuilder()
            .addRiskElementConfigUpdateDetails(
                RiskElementConfigUpdateDetails.newBuilder()
                    .setId("responseSensitivityCritical")
                    .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(8)))
            .build();
    assertDoesNotThrow(() -> elementConfigValidator.validateRiskElementUpdateDetails(updates));
  }

  @Test
  void testValidateRiskElementConfigWithInvalidId() {
    doReturn(Status.INVALID_ARGUMENT).when(mockRequestValidator).validateElementId(any());
    final RiskElementConfig config = RiskElementConfig.newBuilder().setId("").build();
    assertEquals(
        Status.INVALID_ARGUMENT.getCode(),
        elementConfigValidator.validateRiskElementConfig(config).getCode());
  }

  @Test
  void testValidateRiskElementConfigWithNoElementScoring() {
    doReturn(Status.OK).when(mockRequestValidator).validateElementId(any());
    final RiskElementConfig config =
        RiskElementConfig.newBuilder().setId("responseSensitivityCritical").build();
    assertEquals(
        Status.NOT_FOUND.getCode(),
        elementConfigValidator.validateRiskElementConfig(config).getCode());
  }

  @Test
  void testValidateRiskElementConfigWithInvalidScore() {
    doReturn(Status.OK).when(mockRequestValidator).validateElementId(any());
    doReturn(Status.OUT_OF_RANGE).when(mockRequestValidator).validateScoreValue(any(Integer.class));
    final RiskElementConfig config =
        RiskElementConfig.newBuilder()
            .setId("responseSensitivityCritical")
            .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(20))
            .build();
    assertEquals(
        Status.OUT_OF_RANGE.getCode(),
        elementConfigValidator.validateRiskElementConfig(config).getCode());
  }

  @Test
  void testValidateRiskElementConfigWithNoPredicate() {
    doReturn(Status.OK).when(mockRequestValidator).validateElementId(any());
    doReturn(Status.OK).when(mockRequestValidator).validateScoreValue(any(Integer.class));
    final RiskElementConfig config =
        RiskElementConfig.newBuilder()
            .setId("responseSensitivityCritical")
            .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(9))
            .build();
    assertEquals(
        Status.NOT_FOUND.getCode(),
        elementConfigValidator.validateRiskElementConfig(config).getCode());
  }

  @Test
  void testValidateRiskElementConfigWithDefaultPredicate() {
    doReturn(Status.OK).when(mockRequestValidator).validateElementId(any());
    doReturn(Status.OK).when(mockRequestValidator).validateScoreValue(any(Integer.class));
    final RiskElementConfig config =
        RiskElementConfig.newBuilder()
            .setId("responseSensitivityCritical")
            .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(9))
            .setRiskElementPredicate(RiskElementPredicate.getDefaultInstance())
            .build();
    assertEquals(
        Status.NOT_FOUND.getCode(),
        elementConfigValidator.validateRiskElementConfig(config).getCode());
  }

  @Test
  void testValidateRiskElementConfigStringPredicateWithInvalidOperator() {
    doReturn(Status.OK).when(mockRequestValidator).validateElementId(any());
    doReturn(Status.OK).when(mockRequestValidator).validateScoreValue(any(Integer.class));
    final RiskElementConfig config =
        RiskElementConfig.newBuilder()
            .setId("critical")
            .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(9))
            .setRiskElementPredicate(
                RiskElementPredicate.newBuilder()
                    .setLabelId(
                        StringPredicate.newBuilder()
                            .setOperator(StringOperator.STRING_OPERATOR_UNSPECIFIED)))
            .build();
    assertEquals(
        Status.INVALID_ARGUMENT.getCode(),
        elementConfigValidator.validateRiskElementConfig(config).getCode());
  }

  @Test
  void testValidateRiskElementConfigStringPredicateWithInvalidValue() {
    doReturn(Status.OK).when(mockRequestValidator).validateElementId(any());
    doReturn(Status.OK).when(mockRequestValidator).validateScoreValue(any(Integer.class));
    final RiskElementConfig config =
        RiskElementConfig.newBuilder()
            .setId("critical")
            .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(9))
            .setRiskElementPredicate(
                RiskElementPredicate.newBuilder()
                    .setLabelId(
                        StringPredicate.newBuilder()
                            .setOperator(StringOperator.STRING_OPERATOR_EQUALS)
                            .setValue("")))
            .build();
    assertEquals(
        Status.INVALID_ARGUMENT.getCode(),
        elementConfigValidator.validateRiskElementConfig(config).getCode());
  }

  @Test
  void testValidateRiskElementConfigWithValidStringPredicate() {
    doReturn(Status.OK).when(mockRequestValidator).validateElementId(any());
    doReturn(Status.OK).when(mockRequestValidator).validateScoreValue(any(Integer.class));
    final RiskElementConfig config =
        RiskElementConfig.newBuilder()
            .setId("critical")
            .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(9))
            .setRiskElementPredicate(
                RiskElementPredicate.newBuilder()
                    .setLabelId(
                        StringPredicate.newBuilder()
                            .setOperator(StringOperator.STRING_OPERATOR_EQUALS)
                            .setValue("Critical")))
            .build();
    assertEquals(
        Status.OK.getCode(), elementConfigValidator.validateRiskElementConfig(config).getCode());
  }

  @Test
  void testValidateRiskElementConfigIntPredicateWithInvalidOperator() {
    doReturn(Status.OK).when(mockRequestValidator).validateElementId(any());
    doReturn(Status.OK).when(mockRequestValidator).validateScoreValue(any(Integer.class));
    final RiskElementConfig config =
        RiskElementConfig.newBuilder()
            .setId("thirdPartyAPIsCountGreaterThan2")
            .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(5))
            .setRiskElementPredicate(
                RiskElementPredicate.newBuilder()
                    .setThirdPartyApis(
                        IntPredicate.newBuilder()
                            .setOperator(IntOperator.INT_OPERATOR_UNSPECIFIED)))
            .build();
    assertEquals(
        Status.INVALID_ARGUMENT.getCode(),
        elementConfigValidator.validateRiskElementConfig(config).getCode());
  }

  @Test
  void testValidateRiskElementConfigWithValidIntPredicate() {
    doReturn(Status.OK).when(mockRequestValidator).validateElementId(any());
    doReturn(Status.OK).when(mockRequestValidator).validateScoreValue(any(Integer.class));
    final RiskElementConfig config =
        RiskElementConfig.newBuilder()
            .setId("thirdPartyAPIsCountGreaterThan2")
            .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(5))
            .setRiskElementPredicate(
                RiskElementPredicate.newBuilder()
                    .setThirdPartyApis(
                        IntPredicate.newBuilder()
                            .setOperator(IntOperator.INT_OPERATOR_GREATER_THAN)
                            .setValue(2)))
            .build();
    assertEquals(
        Status.OK.getCode(), elementConfigValidator.validateRiskElementConfig(config).getCode());
  }

  @Test
  void testValidateRiskElementConfigWithInvalidEnumOperator() {
    doReturn(Status.OK).when(mockRequestValidator).validateElementId(any());
    doReturn(Status.OK).when(mockRequestValidator).validateScoreValue(any(Integer.class));
    final RiskElementConfig config =
        RiskElementConfig.newBuilder()
            .setId("pathParamTypeNone")
            .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(5))
            .setRiskElementPredicate(
                RiskElementPredicate.newBuilder()
                    .setPathParamType(
                        PathParamTypePredicate.newBuilder()
                            .setOperator(EnumOperator.ENUM_OPERATOR_UNSPECIFIED)))
            .build();
    assertEquals(
        Status.INVALID_ARGUMENT.getCode(),
        elementConfigValidator.validateRiskElementConfig(config).getCode());
  }

  @Test
  void testValidateRiskElementConfigWithInvalidPathParamValue() {
    doReturn(Status.OK).when(mockRequestValidator).validateElementId(any());
    doReturn(Status.OK).when(mockRequestValidator).validateScoreValue(any(Integer.class));
    final RiskElementConfig config =
        RiskElementConfig.newBuilder()
            .setId("pathParamTypeNone")
            .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(5))
            .setRiskElementPredicate(
                RiskElementPredicate.newBuilder()
                    .setPathParamType(
                        PathParamTypePredicate.newBuilder()
                            .setOperator(EnumOperator.ENUM_OPERATOR_EQUALS)
                            .setValue(PathParamTypeValue.PATH_PARAM_TYPE_VALUE_UNSPECIFIED)))
            .build();
    assertEquals(
        Status.INVALID_ARGUMENT.getCode(),
        elementConfigValidator.validateRiskElementConfig(config).getCode());
  }

  @Test
  void testValidateRiskElementConfigWithValidPathParamValue() {
    doReturn(Status.OK).when(mockRequestValidator).validateElementId(any());
    doReturn(Status.OK).when(mockRequestValidator).validateScoreValue(any(Integer.class));
    final RiskElementConfig config =
        RiskElementConfig.newBuilder()
            .setId("pathParamTypeNone")
            .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(5))
            .setRiskElementPredicate(
                RiskElementPredicate.newBuilder()
                    .setPathParamType(
                        PathParamTypePredicate.newBuilder()
                            .setOperator(EnumOperator.ENUM_OPERATOR_EQUALS)
                            .setValue(PathParamTypeValue.PATH_PARAM_TYPE_VALUE_NONE)))
            .build();
    assertEquals(
        Status.OK.getCode(), elementConfigValidator.validateRiskElementConfig(config).getCode());
  }

  @Test
  void testValidateRiskElementConfigWithInvalidResponseSensitivityValue() {
    doReturn(Status.OK).when(mockRequestValidator).validateElementId(any());
    doReturn(Status.OK).when(mockRequestValidator).validateScoreValue(any(Integer.class));
    final RiskElementConfig config =
        RiskElementConfig.newBuilder()
            .setId("responseSensitivityCritical")
            .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(5))
            .setRiskElementPredicate(
                RiskElementPredicate.newBuilder()
                    .setResponseSensitivity(
                        ResponseSensitivityPredicate.newBuilder()
                            .setOperator(EnumOperator.ENUM_OPERATOR_EQUALS)
                            .setValue(
                                ResponseSensitivityValue.RESPONSE_SENSITIVITY_VALUE_UNSPECIFIED)))
            .build();
    assertEquals(
        Status.INVALID_ARGUMENT.getCode(),
        elementConfigValidator.validateRiskElementConfig(config).getCode());
  }

  @Test
  void testValidateRiskElementConfigWithValidResponseSensitivityValue() {
    doReturn(Status.OK).when(mockRequestValidator).validateElementId(any());
    doReturn(Status.OK).when(mockRequestValidator).validateScoreValue(any(Integer.class));
    final RiskElementConfig config =
        RiskElementConfig.newBuilder()
            .setId("responseSensitivityCritical")
            .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(5))
            .setRiskElementPredicate(
                RiskElementPredicate.newBuilder()
                    .setResponseSensitivity(
                        ResponseSensitivityPredicate.newBuilder()
                            .setOperator(EnumOperator.ENUM_OPERATOR_EQUALS)
                            .setValue(
                                ResponseSensitivityValue.RESPONSE_SENSITIVITY_VALUE_CRITICAL)))
            .build();
    assertEquals(
        Status.OK.getCode(), elementConfigValidator.validateRiskElementConfig(config).getCode());
  }

  @Test
  void testValidateRiskElementConfigWithInvalidVulnerabilitySeverityValue() {
    doReturn(Status.OK).when(mockRequestValidator).validateElementId(any());
    doReturn(Status.OK).when(mockRequestValidator).validateScoreValue(any(Integer.class));
    final RiskElementConfig config =
        RiskElementConfig.newBuilder()
            .setId("vulnerabilitySeverityCritical")
            .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(5))
            .setRiskElementPredicate(
                RiskElementPredicate.newBuilder()
                    .setVulnerabilitySeverity(
                        VulnerabilitySeverityPredicate.newBuilder()
                            .setOperator(EnumOperator.ENUM_OPERATOR_EQUALS)
                            .setValue(
                                VulnerabilitySeverityValue
                                    .VULNERABILITY_SEVERITY_VALUE_UNSPECIFIED)))
            .build();
    assertEquals(
        Status.INVALID_ARGUMENT.getCode(),
        elementConfigValidator.validateRiskElementConfig(config).getCode());
  }

  @Test
  void testValidateRiskElementConfigWithValidRVulnerabilitySeverityValue() {
    doReturn(Status.OK).when(mockRequestValidator).validateElementId(any());
    doReturn(Status.OK).when(mockRequestValidator).validateScoreValue(any(Integer.class));
    final RiskElementConfig config =
        RiskElementConfig.newBuilder()
            .setId("vulnerabilitySeverityCritical")
            .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(5))
            .setRiskElementPredicate(
                RiskElementPredicate.newBuilder()
                    .setVulnerabilitySeverity(
                        VulnerabilitySeverityPredicate.newBuilder()
                            .setOperator(EnumOperator.ENUM_OPERATOR_EQUALS)
                            .setValue(
                                VulnerabilitySeverityValue.VULNERABILITY_SEVERITY_VALUE_CRITICAL)))
            .build();
    assertEquals(
        Status.OK.getCode(), elementConfigValidator.validateRiskElementConfig(config).getCode());
  }

  @Test
  void testValidateRiskElementConfigWithInvalidAuthCategoryValue() {
    doReturn(Status.OK).when(mockRequestValidator).validateElementId(any());
    doReturn(Status.OK).when(mockRequestValidator).validateScoreValue(any(Integer.class));
    final RiskElementConfig config =
        RiskElementConfig.newBuilder()
            .setId("externalEncryptedAuthCategoryNone")
            .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(5))
            .setRiskElementPredicate(
                RiskElementPredicate.newBuilder()
                    .setExternalEncryptedAuthCategory(
                        AuthCategoryPredicate.newBuilder()
                            .setOperator(EnumOperator.ENUM_OPERATOR_EQUALS)
                            .setValue(AuthCategoryValue.AUTH_CATEGORY_VALUE_UNSPECIFIED)))
            .build();
    assertEquals(
        Status.INVALID_ARGUMENT.getCode(),
        elementConfigValidator.validateRiskElementConfig(config).getCode());
  }

  @Test
  void testValidateRiskElementConfigWithValidAuthCategoryValue() {
    doReturn(Status.OK).when(mockRequestValidator).validateElementId(any());
    doReturn(Status.OK).when(mockRequestValidator).validateScoreValue(any(Integer.class));
    final RiskElementConfig config =
        RiskElementConfig.newBuilder()
            .setId("externalEncryptedAuthCategoryNone")
            .setRiskElementScoring(RiskElementScoring.newBuilder().setScore(5))
            .setRiskElementPredicate(
                RiskElementPredicate.newBuilder()
                    .setExternalEncryptedAuthCategory(
                        AuthCategoryPredicate.newBuilder()
                            .setOperator(EnumOperator.ENUM_OPERATOR_EQUALS)
                            .setValue(AuthCategoryValue.AUTH_CATEGORY_VALUE_NONE)))
            .build();
    assertEquals(
        Status.OK.getCode(), elementConfigValidator.validateRiskElementConfig(config).getCode());
  }
}
