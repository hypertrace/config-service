package ai.traceable.activity.event.producer;

import ai.traceable.activity.event.ActivityEvent;
import com.typesafe.config.Config;
import org.hypertrace.core.eventstore.EventProducer;
import org.hypertrace.core.eventstore.EventProducerConfig;
import org.hypertrace.core.eventstore.EventStore;
import org.hypertrace.core.eventstore.EventStoreProvider;

public class ActivityEventProducerFactory {

  private static final String EVENT_STORE = "event.store";
  private static final String EVENT_STORE_TYPE_CONFIG = "type";
  private static final String ACTIVITY_EVENTS_TOPIC = "activity-events";
  private static final String ACTIVITY_EVENTS_PRODUCER_CONFIG = "activity.events.producer";

  private ActivityEventProducerFactory() {}

  public static ActivityEventProducer build(Config config) {
    Config eventStoreConfig = config.getConfig(EVENT_STORE);
    String storeType = eventStoreConfig.getString(EVENT_STORE_TYPE_CONFIG);
    EventStore eventStore = EventStoreProvider.getEventStore(storeType, eventStoreConfig);
    EventProducer<ActivityEvent> eventProducer =
        eventStore.createProducer(
            ACTIVITY_EVENTS_TOPIC,
            new EventProducerConfig(
                storeType, eventStoreConfig.getConfig(ACTIVITY_EVENTS_PRODUCER_CONFIG)));
    return new ActivityEventProducerImpl(eventProducer);
  }
}
