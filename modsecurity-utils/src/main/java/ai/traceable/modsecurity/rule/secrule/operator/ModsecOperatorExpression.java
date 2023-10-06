package ai.traceable.modsecurity.rule.secrule.operator;

import static ai.traceable.modsecurity.rule.secrule.ModsecRuleConstants.AT_PREFIX;
import static ai.traceable.modsecurity.rule.secrule.ModsecRuleConstants.DOUBLE_QUOTES;
import static ai.traceable.modsecurity.rule.secrule.ModsecRuleConstants.NOT_PREFIX;
import static ai.traceable.modsecurity.rule.secrule.ModsecRuleConstants.SPACE_DELIMITER;

import ai.traceable.config.utils.RegexValidator;
import io.grpc.Status;

public class ModsecOperatorExpression {
  private final ModsecOperator operator;
  private final boolean negate;
  private final String value;

  public ModsecOperatorExpression(ModsecOperator operator, boolean negate, String value) {
    this.operator = operator;
    this.negate = negate;
    this.value = value;
    validate();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(DOUBLE_QUOTES);
    if (negate) {
      sb.append(NOT_PREFIX);
    }
    sb.append(AT_PREFIX + operator.toString() + SPACE_DELIMITER + value);
    sb.append(DOUBLE_QUOTES);
    return sb.toString();
  }

  private void validate() {
    if (operator.equals(ModsecOperator.MATCHES_REGEX)) {
      Status status = RegexValidator.validate(value);
      if (!status.isOk()) {
        throw status.asRuntimeException();
      }
    }
  }
}
