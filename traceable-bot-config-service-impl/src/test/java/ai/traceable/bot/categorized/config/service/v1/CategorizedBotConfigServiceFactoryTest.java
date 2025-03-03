package ai.traceable.bot.categorized.config.service.v1;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

import io.grpc.Channel;
import org.junit.jupiter.api.Test;

class CategorizedBotConfigServiceFactoryTest {

  @Test
  void testBindings() {
    final Channel mockChannel = mock(Channel.class);
    assertDoesNotThrow(() -> CategorizedBotConfigServiceFactory.build(mockChannel));
  }
}
