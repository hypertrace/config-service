package ai.traceable.external.agent.attribute.config.service.translator;

import java.util.List;

class AgentAttributeConstants {

  static final List<String> REQUEST_BODY_KEYS = List.of("http.request.body", "rpc.request.body");
  static final List<String> RESPONSE_BODY_KEYS = List.of("http.response.body", "rpc.response.body");
  static final String COOKIE_HEADER_KEY = "http.request.header.cookie";
  static final List<String> AUTH_HEADER_KEYS =
      List.of("http.request.header.authorization", "rpc.request.metadata.authorization");
  static final List<String> REQUEST_HEADER_KEY_FORMAT_STRINGS =
      List.of("http.request.header.%s", "rpc.request.metadata.%s");
  static final String END_USER_ID_ATTRIBUTE_KEY = "enduser.id";
  static final String END_USER_ID_RULE_ATTRIBUTE_KEY = "enduser.id.rule";
  static final String END_USER_ROLE_ATTRIBUTE_KEY = "enduser.role";
  static final String END_USER_ROLE_RULE_ATTRIBUTE_KEY = "enduser.role.rule";
  static final String AUTH_TYPES_ATTRIBUTE_KEY = "traceableai.auth.types";
  static final String AUTH_TYPES_RULE_ATTRIBUTE_KEY = "traceableai.auth.rules";
  static final String BASIC_AUTH_TYPE = "Basic";
  static final List<String> URL_KEYS = List.of("http.url", "http.target");
}
