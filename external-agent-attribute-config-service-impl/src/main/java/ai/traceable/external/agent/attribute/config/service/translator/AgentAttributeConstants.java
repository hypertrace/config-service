package ai.traceable.external.agent.attribute.config.service.translator;

import java.util.List;

public class AgentAttributeConstants {
  public static final String REQUEST_BODY_KEY_HTTP = "http.request.body";
  public static final List<String> REQUEST_BODY_KEYS =
      List.of(REQUEST_BODY_KEY_HTTP, "rpc.request.body");
  public static final List<String> RESPONSE_BODY_KEYS =
      List.of("http.response.body", "rpc.response.body");
  public static final String REQUEST_COOKIE_HEADER_KEY = "http.request.header.cookie";
  public static final String RESPONSE_COOKIE_HEADER_KEY = "http.response.header.set-cookie";
  public static final List<String> AUTH_HEADER_KEYS =
      List.of("http.request.header.authorization", "rpc.request.metadata.authorization");
  public static final List<String> REQUEST_HEADER_KEY_FORMAT_STRINGS =
      List.of("http.request.header.%s", "rpc.request.metadata.%s");
  public static final List<String> REQUEST_HEADER_KEY_ESCAPED_FORMAT_STRINGS =
      List.of("http\\.request\\.header\\.%s", "rpc\\.request\\.metadata\\.%s");
  public static final List<String> RESPONSE_HEADER_KEY_FORMAT_STRINGS =
      List.of("http.response.header.%s", "rpc.response.metadata.%s");
  public static final List<String> RESPONSE_HEADER_KEY_ESCAPED_FORMAT_STRINGS =
      List.of("http\\.response\\.header\\.%s", "rpc\\.response\\.metadata\\.%s");
  public static final String END_USER_ID_ATTRIBUTE_KEY = "enduser.id";
  public static final String END_USER_ID_RULE_ATTRIBUTE_KEY = "enduser.id.rule";
  public static final String END_USER_ROLE_ATTRIBUTE_KEY = "enduser.role";
  public static final String END_USER_ROLE_RULE_ATTRIBUTE_KEY = "enduser.role.rule";
  public static final String AUTH_TYPES_ATTRIBUTE_KEY = "traceableai.auth.types";
  public static final String AUTH_TYPES_RULE_ATTRIBUTE_KEY = "traceableai.auth.rules";
  public static final String JWT_EXTRACTION_RULE_ATTRIBUTE_KEY_PREFIX = "traceableai.jwt";
  // The attributes used to hold parts or all of the URL continue to evolve in OTEL and our agents.
  // Here we split into two use cases - one based on usages that require the path, and one that
  // requires the query. Attributes that hold the combined value are exposed in both.
  public static final List<String> URL_OR_PATH_ATTRIBUTE_KEYS =
      List.of("http.url", "http.target", "http.path", "url.full", "url.path");
  public static final List<String> URL_OR_QUERY_ATTRIBUTE_KEYS =
      List.of("http.url", "http.target", "http.path", "url.full", "url.query");
}
