package ai.traceable.region.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

import com.google.inject.Guice;
import com.typesafe.config.Config;
import org.junit.jupiter.api.Test;

class RegionConfigServiceModuleTest {
  @Test
  public void testResolveBindings() {
    Config mockConfig = mock(Config.class);

    assertDoesNotThrow(
        () -> Guice.createInjector(new RegionConfigServiceModule(mockConfig)).getAllBindings());
  }
}
