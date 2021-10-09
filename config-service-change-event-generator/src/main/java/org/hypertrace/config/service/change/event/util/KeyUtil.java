package org.hypertrace.config.service.change.event.util;

import java.util.Optional;
import org.hypertrace.config.change.event.v1.ConfigChangeEventKey;

public class KeyUtil {

  private KeyUtil() {}

  public static final ConfigChangeEventKey getKey(
      String tenantId, String configType, Optional<String> contextOptional) {
    ConfigChangeEventKey.Builder builder = ConfigChangeEventKey.newBuilder();
    builder.setTenantId(tenantId);
    builder.setConfigType(configType);
    contextOptional.ifPresent(builder::setContext);
    return builder.build();
  }
}
