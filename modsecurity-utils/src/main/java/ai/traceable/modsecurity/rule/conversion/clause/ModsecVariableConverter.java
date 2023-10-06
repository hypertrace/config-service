package ai.traceable.modsecurity.rule.conversion.clause;

import ai.traceable.modsecurity.rule.api.v1.RequestKeyValueMatchMetadata;
import ai.traceable.modsecurity.rule.api.v1.RequestValueMatchMetadata;
import ai.traceable.modsecurity.rule.api.v1.ResponseKeyValueMatchMetadata;
import ai.traceable.modsecurity.rule.api.v1.ResponseValueMatchMetadata;
import ai.traceable.modsecurity.rule.secrule.variables.ModsecVariable;
import ai.traceable.modsecurity.rule.secrule.variables.ModsecVariableMetadata;
import ai.traceable.modsecurity.rule.secrule.variables.ModsecVariableMetadataKey;
import java.util.Collections;
import java.util.List;

public class ModsecVariableConverter {
  private static final String HOST_HEADER = "Host";
  private static final String X_FORWARDED_HOST_HEADER = "x-forwarded-host";
  private static final String FORWARDED_HEADER = "forwarded";
  private static final String USER_AGENT_HEADER = "User-Agent";

  List<ModsecVariable> getModsecVariables(RequestValueMatchMetadata metadata) {
    switch (metadata) {
      case REQUEST_VALUE_MATCH_METADATA_URL:
        return Collections.singletonList(
            new ModsecVariable(ModsecVariableMetadata.REQUEST_URI_RAW));
      case REQUEST_VALUE_MATCH_METADATA_HOST:
        return List.of(
            createModsecVariable(ModsecVariableMetadata.REQUEST_HEADERS, HOST_HEADER),
            createModsecVariable(ModsecVariableMetadata.REQUEST_HEADERS, X_FORWARDED_HOST_HEADER),
            createModsecVariable(ModsecVariableMetadata.REQUEST_HEADERS, FORWARDED_HEADER));
      case REQUEST_VALUE_MATCH_METADATA_HTTP_METHOD:
        return Collections.singletonList(new ModsecVariable(ModsecVariableMetadata.REQUEST_METHOD));
      case REQUEST_VALUE_MATCH_METADATA_USER_AGENT:
        return Collections.singletonList(
            createModsecVariable(ModsecVariableMetadata.REQUEST_HEADERS, USER_AGENT_HEADER));
      case REQUEST_VALUE_MATCH_METADATA_BODY:
        return Collections.singletonList(new ModsecVariable(ModsecVariableMetadata.REQUEST_BODY));
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported RequestValueMatchMetadata: %s", metadata));
    }
  }

  List<ModsecVariable> getModsecVariables(ResponseValueMatchMetadata metadata) {
    switch (metadata) {
      case RESPONSE_VALUE_MATCH_METADATA_STATUS_CODE:
        return Collections.singletonList(
            new ModsecVariable(ModsecVariableMetadata.RESPONSE_STATUS));
      case RESPONSE_VALUE_MATCH_METADATA_BODY:
        return Collections.singletonList(new ModsecVariable(ModsecVariableMetadata.RESPONSE_BODY));
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported ResponseValueMatchMetadata: %s", metadata));
    }
  }

  ModsecVariableMetadata getModsecKeyVariableMetadata(RequestKeyValueMatchMetadata metadata) {
    switch (metadata) {
      case REQUEST_KEY_VALUE_MATCH_METADATA_HEADER:
        return ModsecVariableMetadata.REQUEST_HEADERS_NAMES;
      case REQUEST_KEY_VALUE_MATCH_METADATA_PARAMETER:
        return ModsecVariableMetadata.ARGS_NAMES;
      case REQUEST_KEY_VALUE_MATCH_METADATA_QUERY_PARAMETER:
        return ModsecVariableMetadata.ARGS_GET_NAMES;
      case REQUEST_KEY_VALUE_MATCH_METADATA_BODY_PARAMETER:
        return ModsecVariableMetadata.ARGS_POST_NAMES;
      case REQUEST_KEY_VALUE_MATCH_METADATA_COOKIE:
        return ModsecVariableMetadata.REQUEST_COOKIES_NAMES;
      default:
        throw new IllegalArgumentException(
            String.format("Unknown RequestKeyValueMatchMetadata: %s", metadata));
    }
  }

  ModsecVariableMetadata getModsecKeyVariableMetadata(ResponseKeyValueMatchMetadata metadata) {
    switch (metadata) {
      case RESPONSE_KEY_VALUE_MATCH_METADATA_HEADER:
        return ModsecVariableMetadata.RESPONSE_HEADERS_NAMES;
      default:
        throw new IllegalArgumentException(
            String.format("Unknown ResponseKeyValueMatchMetadata: %s", metadata));
    }
  }

  ModsecVariableMetadata getModsecValueVariableMetadata(RequestKeyValueMatchMetadata metadata) {
    switch (metadata) {
      case REQUEST_KEY_VALUE_MATCH_METADATA_HEADER:
        return ModsecVariableMetadata.REQUEST_HEADERS;
      case REQUEST_KEY_VALUE_MATCH_METADATA_PARAMETER:
        return ModsecVariableMetadata.ARGS;
      case REQUEST_KEY_VALUE_MATCH_METADATA_QUERY_PARAMETER:
        return ModsecVariableMetadata.ARGS_GET;
      case REQUEST_KEY_VALUE_MATCH_METADATA_BODY_PARAMETER:
        return ModsecVariableMetadata.ARGS_POST;
      case REQUEST_KEY_VALUE_MATCH_METADATA_COOKIE:
        return ModsecVariableMetadata.REQUEST_COOKIES;
      default:
        throw new IllegalArgumentException(
            String.format("Unknown RequestKeyValueMatchMetadata: %s", metadata));
    }
  }

  ModsecVariableMetadata getModsecValueVariableMetadata(ResponseKeyValueMatchMetadata metadata) {
    switch (metadata) {
      case RESPONSE_KEY_VALUE_MATCH_METADATA_HEADER:
        return ModsecVariableMetadata.RESPONSE_HEADERS;
      default:
        throw new IllegalArgumentException(
            String.format("Unknown ResponseKeyValueMatchMetadata: %s", metadata));
    }
  }

  private ModsecVariable createModsecVariable(ModsecVariableMetadata metadata, String key) {
    return new ModsecVariable(
        metadata,
        new ModsecVariableMetadataKey(
            ModsecVariableMetadataKey.ModsecVariableKeyOperator.EQUALS, key));
  }
}
