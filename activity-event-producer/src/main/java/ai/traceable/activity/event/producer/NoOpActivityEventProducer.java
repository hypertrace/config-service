package ai.traceable.activity.event.producer;

import ai.traceable.activity.event.SecurityConfigurationChange;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class NoOpActivityEventProducer implements ActivityEventProducer {
  @Override
  public void publishSecurityConfigurationChangeEvent(
      RequestContext requestContext, SecurityConfigurationChange securityConfigurationChange) {}

  @Override
  public void close() {}
}
