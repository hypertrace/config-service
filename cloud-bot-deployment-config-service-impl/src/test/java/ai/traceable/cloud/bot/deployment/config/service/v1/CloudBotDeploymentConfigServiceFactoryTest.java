package ai.traceable.cloud.bot.deployment.config.service.v1;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

import com.google.inject.Guice;
import com.google.inject.Stage;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.junit.jupiter.api.Test;

class CloudBotDeploymentConfigServiceFactoryTest {

  @Test
  void testModuleBindings() {
    Channel channel = mock(Channel.class);
    ConfigChangeEventGenerator configChangeEventGenerator = mock(ConfigChangeEventGenerator.class);

    assertDoesNotThrow(
        () ->
            Guice.createInjector(
                    Stage.PRODUCTION,
                    new CloudBotDeploymentConfigServiceModule(channel, configChangeEventGenerator))
                .getAllBindings());
  }
}
