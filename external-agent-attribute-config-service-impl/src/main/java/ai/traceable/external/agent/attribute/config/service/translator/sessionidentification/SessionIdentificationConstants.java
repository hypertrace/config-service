package ai.traceable.external.agent.attribute.config.service.translator.sessionidentification;

public class SessionIdentificationConstants {
  public static final String SESSION_NEW_PREFIX_KEY = "traceableai.session.new";
  public static final String SESSION_PREFIX_KEY = "traceableai.session";
  public static final String SESSION_ATTRIBUTE_REGEX = "traceableai.session.*";
  private static final String DOT = ".";
  private static final String EXPIRATION_VALUE_KEY = "expiration.value";
  private static final String JWT_ATTR_VALUE_KEY = "jwt.attribute.value";
  private static final String ID = "id";

  public String buildKeyForSessionId(String ruleId, int ruleIndex) {
    return SESSION_PREFIX_KEY + DOT + ruleId + DOT + ruleIndex + DOT + ID;
  }

  public String buildKeyForNewSessionId(String ruleId, int ruleIndex) {
    return SESSION_NEW_PREFIX_KEY + DOT + ruleId + DOT + ruleIndex + DOT + ID;
  }

  public String buildKeyForExpirationValue(String ruleId, int ruleIndex, String keyPrefix) {
    return keyPrefix + DOT + ruleId + DOT + ruleIndex + DOT + EXPIRATION_VALUE_KEY;
  }

  public String buildKeyForJwtAttrValue(
      String ruleId, int ruleIndex, String keyPrefix, String jwtAttr) {
    return keyPrefix + DOT + ruleId + DOT + ruleIndex + DOT + jwtAttr + DOT + JWT_ATTR_VALUE_KEY;
  }
}
