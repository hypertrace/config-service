package ai.traceable.genai.config.service.v1;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.genai.config.service.v1.feature.config.GenAiFeatureConfigHandler;
import ai.traceable.genai.config.service.v1.genai.config.DefaultGenAiConfigProvider;
import ai.traceable.genai.config.service.v1.manager.GenAiConfigManager;
import ai.traceable.genai.config.service.v1.manager.GenAiConfigManagerImpl;
import ai.traceable.genai.config.service.v1.store.GenAiConfigStore;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.grpc.BindableService;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import java.util.Set;
import java.util.stream.Collectors;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class GenAiConfigServiceModuleTest {

  @Mock private ManagedChannel channel;
  @Mock private ConfigChangeEventGenerator changeEventGenerator;

  private Injector injector;

  @BeforeEach
  void setUp() {
    Config config =
        ConfigFactory.parseString(
            "gen.ai.config.service.config {\n"
                + "    genAiConfig {\n"
                + "        key = \"value\"\n"
                + "    }\n"
                + "}");

    GenAiConfigServiceModule module =
        new GenAiConfigServiceModule(channel, config, changeEventGenerator);
    injector = Guice.createInjector(module);
  }

  @Test
  void test_bindings_basicDependencies() {
    assertNotNull(injector.getInstance(BindableService.class));
    assertNotNull(injector.getInstance(Config.class));
    assertNotNull(injector.getInstance(ConfigChangeEventGenerator.class));
    assertNotNull(injector.getInstance(Channel.class));
  }

  @Test
  void test_bindings_configServiceBindings() {
    assertNotNull(injector.getInstance(GenAiConfigServiceConfig.class));
    assertEquals(
        "value",
        injector.getInstance(GenAiConfigServiceConfig.class).getGenAiConfig().getString("key"));
  }

  @Test
  void test_bindings_genAiConfigBindings() {
    assertNotNull(injector.getInstance(GenAiConfig.class));
    assertNotNull(injector.getInstance(DefaultGenAiConfigProvider.class));
    assertNotNull(injector.getInstance(GenAiConfigManager.class));
    assertInstanceOf(GenAiConfigManagerImpl.class, injector.getInstance(GenAiConfigManager.class));
  }

  @Test
  void test_bindings_storeBindings() {
    assertNotNull(injector.getInstance(GenAiConfigStore.class));
    assertNotNull(
        injector.getInstance(Key.get(new TypeLiteral<IdentifiedObjectStore<GenAiConfig>>() {})));
    assertInstanceOf(
        GenAiConfigStore.class,
        injector.getInstance(Key.get(new TypeLiteral<IdentifiedObjectStore<GenAiConfig>>() {})));
  }

  @Test
  void test_bindings_featureConfigHandlerModule() {
    Set<GenAiFeatureConfigHandler<?>> handlers = injector.getInstance(new Key<>() {});
    Set<String> featureNames =
        handlers.stream()
            .map(GenAiFeatureConfigHandler::getFeatureName)
            .collect(Collectors.toUnmodifiableSet());
    assertTrue(
        GenAiConfig.getDescriptor().getFields().stream()
            .map(FieldDescriptor::getName)
            .filter(name -> !name.equals("scope"))
            .allMatch(featureNames::contains));
  }

  @Test
  void test_providesGenAiConfigServiceConfig() {
    GenAiConfigServiceConfig serviceConfig = injector.getInstance(GenAiConfigServiceConfig.class);

    assertNotNull(serviceConfig);
    assertNotNull(serviceConfig.getGenAiConfig());
    assertEquals("value", serviceConfig.getGenAiConfig().getString("key"));
  }

  @Test
  void test_providesConfigService() {
    ConfigServiceGrpc.ConfigServiceBlockingStub stub =
        injector.getInstance(ConfigServiceGrpc.ConfigServiceBlockingStub.class);

    assertNotNull(stub);
  }

  @Test
  void test_bindings_singletons() {
    GenAiConfigServiceConfig config1 = injector.getInstance(GenAiConfigServiceConfig.class);
    GenAiConfigServiceConfig config2 = injector.getInstance(GenAiConfigServiceConfig.class);

    assertSame(config1, config2);

    ConfigServiceGrpc.ConfigServiceBlockingStub stub1 =
        injector.getInstance(ConfigServiceGrpc.ConfigServiceBlockingStub.class);
    ConfigServiceGrpc.ConfigServiceBlockingStub stub2 =
        injector.getInstance(ConfigServiceGrpc.ConfigServiceBlockingStub.class);

    assertSame(stub1, stub2);
  }
}
