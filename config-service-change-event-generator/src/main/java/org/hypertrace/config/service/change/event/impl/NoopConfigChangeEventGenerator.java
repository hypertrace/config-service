package org.hypertrace.config.service.change.event.impl;

import com.google.protobuf.Value;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;

/** No-op implementation of Config change event generator interface. */
public class NoopConfigChangeEventGenerator implements ConfigChangeEventGenerator {

  NoopConfigChangeEventGenerator() {}

  @Override
  public void sendCreateNotification(
      String tenantId,
      String resourceName,
      String resourceNamespace,
      String context,
      Value config) {
    // No-op
  }

  @Override
  public void sendDeleteNotification(
      String tenantId,
      String resourceName,
      String resourceNamespace,
      String context,
      Value config) {
    // No-op
  }

  @Override
  public void sendUpdateNotification(
      String tenantId,
      String resourceName,
      String resourceNamespace,
      String context,
      Value prevConfig,
      Value latestConfig) {
    // No-op
  }
}
