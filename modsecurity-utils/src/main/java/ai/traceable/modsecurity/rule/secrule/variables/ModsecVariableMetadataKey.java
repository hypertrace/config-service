package ai.traceable.modsecurity.rule.secrule.variables;

import static ai.traceable.modsecurity.rule.secrule.ModsecRuleConstants.FRONT_SLASH;
import static ai.traceable.modsecurity.rule.secrule.ModsecRuleConstants.PIPE;

import ai.traceable.config.utils.RegexValidator;
import io.grpc.Status;
import lombok.Getter;

@Getter
public class ModsecVariableMetadataKey {
  private final ModsecVariableKeyOperator keyOperator;
  private final String key;

  public ModsecVariableMetadataKey(ModsecVariableKeyOperator keyOperator, String key) {
    this.keyOperator = keyOperator;
    this.key = key;
    validate();
  }

  @Override
  public String toString() {
    switch (keyOperator) {
      case MATCHES_REGEX:
        return FRONT_SLASH + key + FRONT_SLASH;
      default:
        return key;
    }
  }

  private void validate() {
    if (keyOperator.equals(ModsecVariableKeyOperator.MATCHES_REGEX)) {
      if (key.contains(PIPE)) {
        // Ref: https://github.com/SpiderLabs/ModSecurity/issues/1591#issuecomment-337262698
        throw new IllegalArgumentException(
            String.format("Pipe is not allowed in Modsec Variable Operator Regex: %s", key));
      }
      Status status = RegexValidator.validate(key);
      if (!status.isOk()) {
        throw status.asRuntimeException();
      }
    }
  }

  public enum ModsecVariableKeyOperator {
    EQUALS,
    MATCHES_REGEX;
  }
}
