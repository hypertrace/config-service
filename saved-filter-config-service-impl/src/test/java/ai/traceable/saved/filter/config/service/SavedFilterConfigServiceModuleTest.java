package ai.traceable.saved.filter.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Stage;
import com.google.inject.util.Modules;
import com.typesafe.config.Config;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.core.attribute.service.client.AttributeServiceCachedClient;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SavedFilterConfigServiceModuleTest {

  @Mock private AttributeServiceCachedClient attributeServiceCachedClient;

  @Test
  void testResolveBindings() {
    Channel mockChannel = mock(Channel.class);
    Config config = mock(Config.class);
    GrpcChannelRegistry grpcChannelRegistry = mock(GrpcChannelRegistry.class);
    ConfigChangeEventGenerator mockEventGenerator = mock(ConfigChangeEventGenerator.class);
    assertDoesNotThrow(
        () ->
            Guice.createInjector(
                    Stage.PRODUCTION,
                    Modules.override(
                            new SavedFilterConfigServiceModule(
                                mockChannel, mockEventGenerator, config, grpcChannelRegistry))
                        .with(
                            new AbstractModule() {
                              @Override
                              protected void configure() {

                                bind(AttributeServiceCachedClient.class)
                                    .toInstance(attributeServiceCachedClient);
                              }
                            }))
                .getAllBindings());
  }
}
