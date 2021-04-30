package ai.traceable.region.config.service.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class UuidGeneratorTest {
  private UuidGenerator uuidGenerator;

  @BeforeEach
  void setup() {
    uuidGenerator = new UuidGenerator();
  }

  @Test
  void shouldGenerateV5Uuid() {
    assertEquals("7f1e661c-627b-5589-8b3c-37756f524553", uuidGenerator.generateId("random-string"));
  }

  @Test
  void shouldGenerateRandomUuid() {
    assertNotNull(uuidGenerator.generateId());
  }
}
