package ai.traceable.external.agent.attribute.config.service.translator.sessionidentification;

import static ai.traceable.external.agent.attribute.config.service.translator.AgentAttributeConstants.JWT_EXTRACTION_RULE_ATTRIBUTE_KEY_PREFIX;

import ai.traceable.sessionidentification.config.service.v1.CustomAttributeRule;

public class SessionIdentificationConstants {
  public static final String SESSION_NEW_PREFIX_KEY = "traceableai.session.new";
  public static final String SESSION_PREFIX_KEY = "traceableai.session";
  public static final String SESSION_ATTRIBUTE_REGEX = "traceableai.session.*";
  private static final String DOT = ".";
  private static final String EXPIRATION_VALUE_KEY = "expiration.value";
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
      String ruleId,
      int ruleIndex,
      CustomAttributeRule.JwtAttributeExtraction jwtAttributeExtraction) {

    switch (jwtAttributeExtraction.getSourceCase()) {
      case HEADER_KEY:
        return JWT_EXTRACTION_RULE_ATTRIBUTE_KEY_PREFIX
            + ".header"
            + DOT
            + ruleId
            + DOT
            + ruleIndex
            + DOT
            + jwtAttributeExtraction.getHeaderKey();
      case PAYLOAD_CLAIM_NAME:
        return JWT_EXTRACTION_RULE_ATTRIBUTE_KEY_PREFIX
            + ".payload"
            + DOT
            + ruleId
            + DOT
            + ruleIndex
            + DOT
            + jwtAttributeExtraction.getPayloadClaimName();
      default:
    }
    return JWT_EXTRACTION_RULE_ATTRIBUTE_KEY_PREFIX + ruleId + DOT + ruleIndex;
  }
}
