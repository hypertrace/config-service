package ai.traceable.anomaly.config.service.modsec.rules;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.anomaly.config.service.common.AnomalyConfigValidator;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleType;
import ai.traceable.anomaly.config.service.v1.modsec.GetModsecCrsRulesRequest;
import io.grpc.Status;
import io.grpc.Status.Code;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ModsecValidatorImplTest {
  private ModsecValidatorImpl validator;

  @BeforeEach
  void setUp() {
    this.validator = new ModsecValidatorImpl(new AnomalyConfigValidator());
  }

  @Test
  @DisplayName("Should return OK status when a given valid get modsec rule request")
  void validateOk() {
    GetModsecCrsRulesRequest request =
        GetModsecCrsRulesRequest.newBuilder()
            .addAllSubRuleTypes(
                List.of(
                    AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE,
                    AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_REGULAR))
            .build();
    Status status = validator.validate(request);
    assertEquals(Status.Code.OK, status.getCode());
  }

  @Test
  @DisplayName(
      "Should return INVALID_ARGUMENT status when a given an invalid get modsec rule request")
  void validateError() {
    GetModsecCrsRulesRequest request =
        GetModsecCrsRulesRequest.newBuilder()
            .addAllSubRuleTypes(
                List.of(
                    AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_UNSPECIFIED,
                    AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_REGULAR))
            .build();
    Status status = validator.validate(request);
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());
  }
}
