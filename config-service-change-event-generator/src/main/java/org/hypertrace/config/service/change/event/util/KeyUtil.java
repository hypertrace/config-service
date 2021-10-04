package org.hypertrace.config.service.change.event.util;

public class KeyUtil {

  private KeyUtil() {}

  public static final String getKey(
      String tenantId, String resourceName, String resourceNamespace, String context) {
    return String.format("%s:%s:%s:%s", tenantId, resourceName, resourceNamespace, context);
  }
}
