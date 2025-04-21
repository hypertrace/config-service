package ai.traceable.genai.config.service.v1.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.genai.config.service.v1.EnvironmentScope;
import ai.traceable.genai.config.service.v1.GenAiScope;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GenAiConfigIdGeneratorTest {

  private GenAiConfigIdGenerator idGenerator;

  @BeforeEach
  void setUp() {
    idGenerator = new GenAiConfigIdGenerator(new UuidGenerator());
  }

  @Test
  void test_generateId_withEnvironmentScope() {
    String envId = "test-env";
    GenAiScope scope =
        GenAiScope.newBuilder()
            .setEnvironmentScope(EnvironmentScope.newBuilder().setEnvironmentId(envId).build())
            .build();

    String result = idGenerator.generateId(scope);

    assertNotNull(result);
    assertNotEquals("", result);

    String secondResult = idGenerator.generateId(scope);
    assertEquals(result, secondResult);

    GenAiScope differentScope =
        GenAiScope.newBuilder()
            .setEnvironmentScope(
                EnvironmentScope.newBuilder().setEnvironmentId("different-env").build())
            .build();
    String differentResult = idGenerator.generateId(differentScope);
    assertNotEquals(result, differentResult);
  }

  @Test
  void test_generateId_withDefaultScope() {
    GenAiScope scope = GenAiScope.getDefaultInstance();

    String result = idGenerator.generateId(scope);

    assertNotNull(result);
    assertNotEquals("", result);

    String secondResult = idGenerator.generateId(scope);
    assertEquals(result, secondResult);
  }

  @Test
  void test_generateId_differentScopesGenerateDifferentIds() {
    GenAiScope defaultScope = GenAiScope.getDefaultInstance();
    GenAiScope envScope =
        GenAiScope.newBuilder()
            .setEnvironmentScope(EnvironmentScope.newBuilder().setEnvironmentId("test-env").build())
            .build();

    String defaultResult = idGenerator.generateId(defaultScope);
    String envResult = idGenerator.generateId(envScope);

    assertNotEquals(defaultResult, envResult);
  }
}
