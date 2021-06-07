package ai.traceable.iprange.config.service.utils;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class UuidGeneratorTest {
  private UuidGenerator uuidGenerator;

  @BeforeEach
  void setUp() {
    uuidGenerator = new UuidGenerator();
  }

  @Test
  void testGenerateId() {
    assertNotNull(uuidGenerator.generateId());
  }
}
