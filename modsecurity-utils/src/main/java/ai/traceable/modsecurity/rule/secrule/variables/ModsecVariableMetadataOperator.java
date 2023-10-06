package ai.traceable.modsecurity.rule.secrule.variables;

public enum ModsecVariableMetadataOperator {
  NOT("!", true),
  COUNT("&", false);

  private final String operator;
  private final boolean needsMetadataKey;

  ModsecVariableMetadataOperator(String operator, boolean needsMetadataKey) {
    this.operator = operator;
    this.needsMetadataKey = needsMetadataKey;
  }

  public boolean needsMetadataKey() {
    return needsMetadataKey;
  }

  @Override
  public String toString() {
    return operator;
  }
}
