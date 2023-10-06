package ai.traceable.modsecurity.rule.secrule.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class ModsecOperatorExpressionTest {
  @Test
  public void test_toString() {
    assertEquals(
        "\"@rx x-(real|forward)\"",
        new ModsecOperatorExpression(ModsecOperator.MATCHES_REGEX, false, "x-(real|forward)")
            .toString());
    assertEquals(
        "\"!@contains x-forward\"",
        new ModsecOperatorExpression(ModsecOperator.CONTAINS, true, "x-forward").toString());
  }
}
