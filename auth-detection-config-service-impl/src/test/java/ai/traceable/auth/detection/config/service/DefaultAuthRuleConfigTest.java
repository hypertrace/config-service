package ai.traceable.auth.detection.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class DefaultAuthRuleConfigTest {

  @Test
  void loadsDefaultsSuccessfully() {
    Config config = ConfigFactory.parseResources("default-rules.conf");

    DefaultAuthRuleConfig defaultRuleConfig = new DefaultAuthRuleConfig(config);

    assertEquals(2, defaultRuleConfig.getAll().size());
    assertTrue(defaultRuleConfig.isDefaultConfig("f496d554-4685-4de4-a074-4186ee579a4c"));
    assertFalse(defaultRuleConfig.isDefaultConfig(UUID.randomUUID().toString()));
  }
}
