package ai.traceable.bot.categorized.policy.service.v1;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.junit.jupiter.api.Test;

class CategorizedBotConfigPolicyServiceFactoryTest {

  @Test
  void testBindings() {
    final Channel mockChannel = mock(Channel.class);
    final ConfigChangeEventGenerator mockEventGenerator = mock(ConfigChangeEventGenerator.class);
    final GrpcChannelRegistry grpcChannelRegistry = mock(GrpcChannelRegistry.class);
    final Config mockConfig = mock(Config.class);

    doReturn(mock(ManagedChannel.class))
        .when(grpcChannelRegistry)
        .forPlaintextAddress("localhost", 50061);

    when(mockConfig.getConfig("entity.fetcher.cache"))
        .thenReturn(
            ConfigFactory.parseString(
                "  service.mapping.cache = {\n"
                    + "    maxSize = 1000\n"
                    + "    refreshAfterWriteDuration = 10m\n"
                    + "    expireAfterWriteDuration = 1h\n"
                    + "  }\n"
                    + "  api.mapping.cache = {\n"
                    + "    maxSize = 1000\n"
                    + "    refreshAfterWriteDuration = 10m\n"
                    + "    expireAfterAccessDuration = 1h\n"
                    + "  }\n"));
    when(mockConfig.getConfig("entity.service"))
        .thenReturn(
            ConfigFactory.parseString(
                "config {\n"
                    + "    host = localhost\n"
                    + "    port = 50061\n"
                    + "    timeout = 10s\n"
                    + "  }\n"
                    + "  attributeMap {\n"
                    + "    service.id = \"SERVICE.id\"\n"
                    + "    service.name = \"SERVICE.name\"\n"
                    + "    service.environment = \"SERVICE.environment\"\n"
                    + "    environment.id = \"ENVIRONMENT.id\"\n"
                    + "    environment.name = \"ENVIRONMENT.name\"\n"
                    + "  }"));
    when(mockConfig.getConfig("bot.config.service"))
        .thenReturn(
            ConfigFactory.parseResources("application.conf").getConfig("bot.config.service"));

    assertDoesNotThrow(
        () ->
            CategorizedBotConfigPolicyServiceFactory.build(
                mockChannel, mockEventGenerator, grpcChannelRegistry, mockConfig));
  }
}
