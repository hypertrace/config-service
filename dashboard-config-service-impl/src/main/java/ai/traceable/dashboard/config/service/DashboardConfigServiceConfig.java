package ai.traceable.dashboard.config.service;

import com.typesafe.config.Config;
import lombok.Value;

@Value
public class DashboardConfigServiceConfig {
  private static final String TRACEABLE_URL = "traceable.url";
  private static final String EVENT_STORE_TYPE_CONFIG = "type";
  private static final String TOPIC_NAME = "topic.name";
  private static final String EMAIL_NOTIFICATION_PRODUCER = "email.notification.producer";
  private static final String EVENT_STORE_CONFIG_PATH = "event.store";

  String traceableUrl;
  String storeType;
  Config emailNotificationProducerConfig;
  String emailNotificationTopicName;
  Config unparsedConfig;

  public DashboardConfigServiceConfig(Config config) {
    this.traceableUrl = config.getString(TRACEABLE_URL);
    Config eventStoreConfig = config.getConfig(EVENT_STORE_CONFIG_PATH);
    this.storeType = eventStoreConfig.getString(EVENT_STORE_TYPE_CONFIG);
    this.emailNotificationProducerConfig = eventStoreConfig.getConfig(EMAIL_NOTIFICATION_PRODUCER);
    this.emailNotificationTopicName = this.emailNotificationProducerConfig.getString(TOPIC_NAME);
    this.unparsedConfig = eventStoreConfig;
  }

  public static DashboardConfigServiceConfig from(Config config) {
    return new DashboardConfigServiceConfig(config);
  }
}
