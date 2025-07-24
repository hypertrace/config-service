package ai.traceable.dashboard.config.service;

import ai.traceable.notification.message.api.v2.NotificationMessage;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import java.time.Clock;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.eventstore.EventProducer;
import org.hypertrace.core.eventstore.EventProducerConfig;
import org.hypertrace.core.eventstore.EventStore;
import org.hypertrace.core.eventstore.EventStoreProvider;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

class DashboardConfigServiceModule extends AbstractModule {

  private final Channel channel;
  private final ConfigChangeEventGenerator configChangeEventGenerator;
  private final DashboardConfigServiceConfig dashboardConfigServiceConfig;
  private final Config config;

  DashboardConfigServiceModule(
      Channel channel, Config config, ConfigChangeEventGenerator configChangeEventGenerator) {
    this.channel = channel;
    this.config = config;
    this.configChangeEventGenerator = configChangeEventGenerator;
    this.dashboardConfigServiceConfig = DashboardConfigServiceConfig.from(config);
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(DashboardConfigServiceImpl.class);
    bind(Clock.class).toInstance(Clock.systemUTC());
    bind(ConfigChangeEventGenerator.class).toInstance(configChangeEventGenerator);
    bind(DashboardConfigServiceConfig.class).toInstance(dashboardConfigServiceConfig);
  }

  @Provides
  ConfigServiceGrpc.ConfigServiceBlockingStub provideConfigStub() {
    return ConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Provides
  @Singleton
  public EventProducer<String, NotificationMessage> provideEventProducer() {
    EventStore eventStore =
        EventStoreProvider.getEventStore(
            dashboardConfigServiceConfig.getStoreType(),
            dashboardConfigServiceConfig.getUnparsedConfig());
    return eventStore.createProducer(
        dashboardConfigServiceConfig.getEmailNotificationTopicName(),
        new EventProducerConfig(
            dashboardConfigServiceConfig.getStoreType(),
            dashboardConfigServiceConfig.getEmailNotificationProducerConfig()));
  }
}
