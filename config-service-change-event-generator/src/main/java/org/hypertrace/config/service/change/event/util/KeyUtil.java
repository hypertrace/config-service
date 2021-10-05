package org.hypertrace.config.service.change.event.util;

import org.hypertrace.config.change.event.v1.ConfigChangeEventKey;

public class KeyUtil {

  private KeyUtil() {}

  public static final ConfigChangeEventKey getKey(
      String tenantId, String resourceName, String resourceNamespace, String context) {
    ConfigChangeEventKey.Builder builder = ConfigChangeEventKey.newBuilder();
    builder.setTenantId(tenantId);
    builder.setResourceName(resourceName);
    builder.setResourceNamespace(resourceNamespace);
    if (context != null) {
      builder.setContext(context);
    }
    return builder.build();
  }
}
