package ai.traceable.data.obfuscation.config.service.rules;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class ConfigManagerModuleTest {
  Channel mockChannel = Mockito.mock(Channel.class);
  ConfigChangeEventGenerator configChangeEventGenerator =
      Mockito.mock(ConfigChangeEventGenerator.class);

  AbstractModule testBindings =
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(Channel.class).toInstance(mockChannel);
          bind(ConfigChangeEventGenerator.class).toInstance(configChangeEventGenerator);
        }
      };

  @Test
  void configure() {
    Injector injector =
        Guice.createInjector(Modules.combine(testBindings, new ConfigManagerModule()));
    ConfigManager configManager = injector.getInstance(ConfigManager.class);
    assertNotNull(configManager);
  }

  @Test
  void providesConfigService() {
    Injector injector =
        Guice.createInjector(Modules.combine(testBindings, new ConfigManagerModule()));
    ConfigServiceGrpc.ConfigServiceBlockingStub stub =
        injector.getInstance(ConfigServiceGrpc.ConfigServiceBlockingStub.class);
    assertNotNull(stub);
  }
}
