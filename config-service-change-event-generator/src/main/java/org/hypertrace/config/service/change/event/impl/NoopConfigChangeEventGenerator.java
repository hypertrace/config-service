package org.hypertrace.config.service.change.event.impl;

import com.google.protobuf.Value;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.core.grpcutils.context.RequestContext;

/** No-op implementation of Config change event generator interface. */
public class NoopConfigChangeEventGenerator implements ConfigChangeEventGenerator {

  NoopConfigChangeEventGenerator() {}

  @Override
  public void sendCreateNotification(
      RequestContext requestContext, String configType, String context, Value config) {
    // No-op
  }

  @Override
  public void sendDeleteNotification(
      RequestContext requestContext, String configType, String context, Value config) {
    // No-op
  }

  @Override
  public void sendUpdateNotification(
      RequestContext requestContext,
      String configType,
      String context,
      Value prevConfig,
      Value latestConfig) {
    // No-op
  }
}
