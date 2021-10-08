package org.hypertrace.config.service.change.event.util;

import javax.annotation.Nullable;
import org.hypertrace.config.change.event.v1.ConfigChangeEventKey;

public class KeyUtil {

  private KeyUtil() {}

  public static final ConfigChangeEventKey getKey(
      String tenantId, String configType, @Nullable String context) {
    ConfigChangeEventKey.Builder builder = ConfigChangeEventKey.newBuilder();
    builder.setTenantId(tenantId);
    builder.setConfigType(configType);
    if (context != null) {
      builder.setContext(context);
    }
    return builder.build();
  }
}
