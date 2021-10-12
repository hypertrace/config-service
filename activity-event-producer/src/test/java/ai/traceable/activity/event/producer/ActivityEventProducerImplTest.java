package ai.traceable.activity.event.producer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.activity.event.ActivityEvent;
import ai.traceable.activity.event.ActivityType;
import ai.traceable.activity.event.InitiatorType;
import ai.traceable.activity.event.SecurityConfigurationChange;
import java.util.Optional;
import org.hypertrace.core.eventstore.EventProducer;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ActivityEventProducerImplTest {

  @Mock private EventProducer<Void, ActivityEvent> eventProducer;
  @Captor private ArgumentCaptor<ActivityEvent> activityEventArgumentCaptor;

  private ActivityEventProducer activityEventProducer;

  @BeforeEach
  void setUp() {
    activityEventProducer = new ActivityEventProducerImpl(eventProducer);
  }

  @Test
  void publishSecurityConfigurationChangeEvent() {
    RequestContext requestContext = mock(RequestContext.class);
    when(requestContext.getTenantId()).thenReturn(Optional.of("tenant1"));
    when(requestContext.getUserId()).thenReturn(Optional.of("user1"));
    when(requestContext.getName()).thenReturn(Optional.of("John Doe"));
    SecurityConfigurationChange securityConfigurationChange =
        mock(SecurityConfigurationChange.class);
    activityEventProducer.publishSecurityConfigurationChangeEvent(
        requestContext, securityConfigurationChange);

    verify(eventProducer, times(1)).send(isNull(), activityEventArgumentCaptor.capture());
    ActivityEvent publishedActivityEvent = activityEventArgumentCaptor.getValue();
    assertEquals("tenant1", publishedActivityEvent.getTenantId());
    assertEquals(
        ActivityType.SECURITY_CONFIGURATION_CHANGE, publishedActivityEvent.getActivityType());
    assertEquals("user1", publishedActivityEvent.getInitiatorId());
    assertEquals("John Doe", publishedActivityEvent.getInitiatorName());
    assertEquals(InitiatorType.USER, publishedActivityEvent.getInitiatorType());
    assertEquals(
        securityConfigurationChange, publishedActivityEvent.getSecurityConfigurationChange());
  }
}
