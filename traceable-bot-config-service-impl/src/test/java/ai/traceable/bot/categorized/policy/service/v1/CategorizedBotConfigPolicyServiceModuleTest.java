package ai.traceable.bot.categorized.policy.service.v1;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.junit.jupiter.api.Test;

class CategorizedBotConfigPolicyServiceModuleTest {

  @Test
  void testBindings() {
    final Channel mockChannel = mock(Channel.class);
    final ConfigChangeEventGenerator mockEventGenerator = mock(ConfigChangeEventGenerator.class);
    final CategorizedBotConfigPolicyServiceModule categorizedBotConfigPolicyServiceModule =
        new CategorizedBotConfigPolicyServiceModule(mockChannel, mockEventGenerator);
    assertDoesNotThrow(categorizedBotConfigPolicyServiceModule::provideConfigStub);
  }
}
