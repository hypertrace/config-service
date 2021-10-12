package ai.traceable.activity.event.producer;

import ai.traceable.activity.event.ActivityEvent;
import ai.traceable.activity.event.ActivityType;
import ai.traceable.activity.event.InitiatorType;
import ai.traceable.activity.event.SecurityConfigurationChange;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.eventstore.EventProducer;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class ActivityEventProducerImpl implements ActivityEventProducer {

  private final EventProducer<Void, ActivityEvent> activityEventProducer;

  public ActivityEventProducerImpl(EventProducer<Void, ActivityEvent> activityEventProducer) {
    this.activityEventProducer = activityEventProducer;
  }

  @Override
  public void publishSecurityConfigurationChangeEvent(
      RequestContext requestContext, SecurityConfigurationChange securityConfigurationChange) {
    try {
      ActivityEvent.Builder activityEventBuilder =
          ActivityEvent.newBuilder()
              .setTenantId(getTenantId(requestContext))
              .setActivityType(ActivityType.SECURITY_CONFIGURATION_CHANGE)
              .setEventTimeMillis(System.currentTimeMillis())
              .setInitiatorType(InitiatorType.USER)
              .setSecurityConfigurationChange(securityConfigurationChange);
      requestContext.getUserId().ifPresent(activityEventBuilder::setInitiatorId);
      requestContext.getName().ifPresent(activityEventBuilder::setInitiatorName);
      activityEventProducer.send(null, activityEventBuilder.build());
    } catch (Exception e) {
      log.error("Failed to publish security configuration change event", e);
    }
  }

  @Override
  public void close() {
    activityEventProducer.close();
  }

  private String getTenantId(RequestContext requestContext) {
    return requestContext
        .getTenantId()
        .orElseThrow(() -> new IllegalArgumentException("Tenant Id is missing in request"));
  }
}
