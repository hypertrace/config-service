package ai.traceable.modsecurity.rule.secrule.operator;

public enum ModsecOperator {
  EQUALS("eq"),
  STRING_EQUALS("streq"),
  MATCHES_REGEX("rx"),
  GREATER_THAN("gt"),
  LESS_THAN("lt"),
  CONTAINS("contains");

  private final String operator;

  ModsecOperator(String operator) {
    this.operator = operator;
  }

  @Override
  public String toString() {
    return operator;
  }
}
