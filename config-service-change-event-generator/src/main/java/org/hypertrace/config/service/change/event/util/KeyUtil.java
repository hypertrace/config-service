package org.hypertrace.config.service.change.event.util;

import org.hypertrace.config.change.event.v1.ConfigChangeEventKey;

public class KeyUtil {

  private KeyUtil() {}

  public static final ConfigChangeEventKey getKey(
      String tenantId, String resourceName, String resourceNamespace, String context) {
    return new ConfigChangeEventKey(tenantId, resourceName, resourceNamespace, context);
  }
}
