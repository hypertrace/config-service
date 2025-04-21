package ai.traceable.genai.config.service.v1.feature.config;

import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.genai.config.service.v1.GenAiConfig;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.protobuf.Descriptors.FieldDescriptor;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class GenAiFeatureConfigHandlerModuleTest {

  @Test
  void test_allFeatureBindings() {
    Injector injector = Guice.createInjector(new GenAiFeatureConfigHandlerModule());
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
}
