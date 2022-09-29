package ai.traceable.anomaly.config.service.override.common.validator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.anomaly.config.service.v1.override.DetectionOverrideEnvironmentScope;
import ai.traceable.anomaly.config.service.v1.override.DetectionOverrideRuleScope;
import io.grpc.Status;
import java.util.List;
import org.junit.jupiter.api.Test;

public class DetectionOverrideScopeValidatorTest {

  private final DetectionOverrideScopeValidator scopeValidator =
      new DetectionOverrideScopeValidator();

  @Test
  public void testValidateRuleScope() {
    DetectionOverrideRuleScope scope = DetectionOverrideRuleScope.getDefaultInstance();
    assertEquals(Status.OK, scopeValidator.validateRuleScope(scope));

    scope =
        DetectionOverrideRuleScope.newBuilder()
            .setEnvironmentScope(DetectionOverrideEnvironmentScope.getDefaultInstance())
            .build();
    assertEquals(
        Status.INVALID_ARGUMENT.getCode(), scopeValidator.validateRuleScope(scope).getCode());

    scope =
        DetectionOverrideRuleScope.newBuilder()
            .setEnvironmentScope(
                DetectionOverrideEnvironmentScope.newBuilder()
                    .addAllEnvironmentIds(List.of("dev", "prod")))
            .build();
    assertEquals(Status.OK, scopeValidator.validateRuleScope(scope));
  }
}
