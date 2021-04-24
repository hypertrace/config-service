package ai.traceable.customsignature.config.service.modsec.registry;

public enum ModsecOperators {
  EQUALS("@streq"),
  NOT_EQUAL("!@streq"),
  MATCHES_REGEX("@rx"),
  NOT_MATCH_REGEX("!@rx"),
  GREATER_THAN("@gt"),
  LESS_THAN("@lt"),
  CONTAINS("@contains"),
  NOT_CONTAIN("!@contains"),
  ;

  private final String operator;

  ModsecOperators(String operator) {
    this.operator = operator;
  }

  public String getOperator() {
    return operator;
  }
}
