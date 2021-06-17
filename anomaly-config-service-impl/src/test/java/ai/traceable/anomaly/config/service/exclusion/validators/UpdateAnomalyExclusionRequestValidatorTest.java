package ai.traceable.anomaly.config.service.exclusion.validators;

import ai.traceable.anomaly.config.service.v1.exclusion.UpdateAnomalyExclusionRuleRequest;
import io.grpc.Status;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class UpdateAnomalyExclusionRequestValidatorTest {
  private UpdateAnomalyExclusionRequestValidator validator =
      new UpdateAnomalyExclusionRequestValidator();

  @Test
  void test() {
    UpdateAnomalyExclusionRuleRequest validRequest =
        UpdateAnomalyExclusionRuleRequest.newBuilder().setRuleId("a").setName("rule_name").build();
    Assertions.assertEquals(Status.OK, validator.validate(validRequest));

    UpdateAnomalyExclusionRuleRequest invalidRequest =
        UpdateAnomalyExclusionRuleRequest.newBuilder().setRuleId("").build();
    Assertions.assertEquals(
        Status.INVALID_ARGUMENT.getCode(), validator.validate(invalidRequest).getCode());
    Assertions.assertEquals(
        "Rule Id required to update the rule", validator.validate(invalidRequest).getDescription());
  }
}
