package ai.traceable.anomaly.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import com.google.inject.Guice;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.junit.jupiter.api.Test;

class AnomalyConfigServiceModuleTest {
  @Test
  public void testResolveBindings() {
    Channel mockChannel = mock(Channel.class);
    Config config =
        ConfigFactory.parseString(
            "anomaly.config.service {\n"
                + "  disabled = true\n"
                + "  internal = false\n"
                + "}\n"
                + "license.metering.service {\n"
                + "  host = \"localhost\"\n"
                + "  port = 51018\n"
                + "  call.timeout.duration = 10s\n"
                + "  cache.expiration.duration = 5m\n"
                + "  cache.max.size = 5000\n"
                + "}");

    assertDoesNotThrow(
        () ->
            Guice.createInjector(
                    new AnomalyConfigServiceModule(
                        new GrpcChannelRegistry(),
                        mockChannel,
                        config,
                        mock(ConfigChangeEventGenerator.class),
                        mock(FeatureCachingClient.class)))
                .getAllBindings());
  }
}
