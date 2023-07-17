package ai.traceable.external.data.classification.config.service.session;

public class SessionIdentificationConstants {
  private static final String SESSION_NEW_PREFIX_KEY = "traceableai.session.new";
  private static final String SESSION_PREFIX_KEY = "traceableai.session";
  private static final String DOT = ".";
  private static final String ID = "id";

  public String buildKeyForSessionId(String ruleId, int ruleIndex) {
    return SESSION_PREFIX_KEY + DOT + ruleId + DOT + ruleIndex + DOT + ID;
  }

  public String buildKeyForNewSessionId(String ruleId, int ruleIndex) {
    return SESSION_NEW_PREFIX_KEY + DOT + ruleId + DOT + ruleIndex + DOT + ID;
  }
}
