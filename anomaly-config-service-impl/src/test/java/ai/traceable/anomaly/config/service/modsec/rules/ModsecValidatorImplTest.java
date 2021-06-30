package ai.traceable.anomaly.config.service.modsec.rules;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.anomaly.config.service.v1.modsec.GetModsecCrsRulesRequest;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecCrsRulesType;
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
    this.validator = new ModsecValidatorImpl();
  }

  @Test
  @DisplayName("Should return OK status when a given valid get modsec rule request")
  void validateOk() {
    GetModsecCrsRulesRequest request =
        GetModsecCrsRulesRequest.newBuilder()
            .addAllModsecCrsRulesTypes(
                List.of(
                    ModsecCrsRulesType.MODSEC_CRS_RULES_TYPE_SAFE,
                    ModsecCrsRulesType.MODSEC_CRS_RULES_TYPE_REGULAR))
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
            .addAllModsecCrsRulesTypes(
                List.of(
                    ModsecCrsRulesType.MODSEC_CRS_RULES_TYPE_UNSPECIFIED,
                    ModsecCrsRulesType.MODSEC_CRS_RULES_TYPE_REGULAR))
            .build();
    Status status = validator.validate(request);
    assertEquals(Code.INVALID_ARGUMENT, status.getCode());
  }
}
