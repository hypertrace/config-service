package ai.traceable.activity.event.producer;

import ai.traceable.activity.event.SecurityConfigurationChange;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface ActivityEventProducer {

  void publishSecurityConfigurationChangeEvent(
      RequestContext requestContext, SecurityConfigurationChange securityConfigurationChange);

  void close();
}
