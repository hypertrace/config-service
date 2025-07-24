package ai.traceable.dashboard.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.dashboard.config.service.notification.TemplateLoader;
import ai.traceable.notification.message.api.v2.NotificationMessage;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Provides;
import com.google.inject.Stage;
import com.google.inject.util.Modules;
import com.typesafe.config.Config;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.core.eventstore.EventProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DashboardConfigServiceModuleTest {
  @Mock ConfigChangeEventGenerator mockChangeEventGenerator;
  @Mock Channel mockChannel;
  @Mock Config config;
  @Mock Config eventStoreConfig;
  @Mock Config emailNotificationProducerConfig;

  @BeforeEach
  void setUp() {
    when(config.getString("traceable.url")).thenReturn("http://traceable.ai");
    when(config.getConfig("event.store")).thenReturn(eventStoreConfig);
    when(eventStoreConfig.getString("type")).thenReturn("kafka");
    when(eventStoreConfig.getConfig("email.notification.producer"))
        .thenReturn(emailNotificationProducerConfig);
    when(emailNotificationProducerConfig.getString("topic.name")).thenReturn("email-notifications");
  }

  @Test
  void testResolveBindings() {
    EventProducer<String, NotificationMessage> mockEventProducer = mock(EventProducer.class);

    // Create a mock TemplateLoader without any stubbing
    TemplateLoader mockTemplateLoader = mock(TemplateLoader.class);
    // We don't need to stub any methods since they're not called in this test

    AbstractModule overrideModule =
        new AbstractModule() {
          @Override
          protected void configure() {
            // Bind the mock TemplateLoader
            bind(TemplateLoader.class).toInstance(mockTemplateLoader);
          }

          @Provides
          EventProducer<String, NotificationMessage> provideEventProducer() {
            return mockEventProducer;
          }
        };
    assertDoesNotThrow(
        () ->
            Guice.createInjector(
                    Stage.PRODUCTION,
                    Modules.override(
                            new DashboardConfigServiceModule(
                                mockChannel, config, mockChangeEventGenerator))
                        .with(overrideModule))
                .getAllBindings());
  }
}
