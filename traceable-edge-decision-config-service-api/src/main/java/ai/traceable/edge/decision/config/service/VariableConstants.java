package ai.traceable.edge.decision.config.service;

public enum VariableConstants {
  USER_ATTRIBUTION_VARIABLE_NAME("TRACEABLE_USER_ID");

  private final String value;

  VariableConstants(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }
}
