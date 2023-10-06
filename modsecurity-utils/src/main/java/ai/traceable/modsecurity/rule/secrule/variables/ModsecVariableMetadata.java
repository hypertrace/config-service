package ai.traceable.modsecurity.rule.secrule.variables;

public enum ModsecVariableMetadata {
  REQUEST_URI_RAW,
  REQUEST_METHOD,
  REQUEST_HEADERS_NAMES,
  REQUEST_HEADERS,
  ARGS_NAMES,
  ARGS,
  ARGS_GET_NAMES,
  ARGS_GET,
  ARGS_POST_NAMES,
  ARGS_POST,
  REQUEST_BODY,
  RESPONSE_STATUS,
  RESPONSE_HEADERS,
  RESPONSE_HEADERS_NAMES,
  RESPONSE_BODY,
  REQUEST_COOKIES,
  REQUEST_COOKIES_NAMES,
  MATCHED_VARS,
  MATCHED_VARS_NAMES;

  private static final String RESPONSE_PHASE_STRING = "RESPONSE";

  public boolean needsResponsePhase() {
    return name().contains(RESPONSE_PHASE_STRING);
  }

  @Override
  public String toString() {
    return name();
  }
}
