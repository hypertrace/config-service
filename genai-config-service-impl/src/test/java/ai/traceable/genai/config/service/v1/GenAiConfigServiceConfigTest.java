package ai.traceable.genai.config.service.v1;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.Test;

class GenAiConfigServiceConfigTest {

  @Test
  void test_constructor_withValidConfig() {
    Config config =
        ConfigFactory.parseString(
            "genAiConfig {\n" + "    key1 = \"value1\"\n" + "    key2 = \"value2\"\n" + "}");

    GenAiConfigServiceConfig serviceConfig = new GenAiConfigServiceConfig(config);

    assertNotNull(serviceConfig.getGenAiConfig());
    assertEquals("value1", serviceConfig.getGenAiConfig().getString("key1"));
    assertEquals("value2", serviceConfig.getGenAiConfig().getString("key2"));
  }

  @Test
  void test_constructor_withEmptyConfig() {
    Config config = ConfigFactory.empty();

    GenAiConfigServiceConfig serviceConfig = new GenAiConfigServiceConfig(config);

    assertNotNull(serviceConfig.getGenAiConfig());
    assertTrue(serviceConfig.getGenAiConfig().isEmpty());
  }

  @Test
  void test_constructor_withMissingGenAiConfigPath() {
    Config config = ConfigFactory.parseString("otherConfig {\n" + "    key = \"value\"\n" + "}");

    GenAiConfigServiceConfig serviceConfig = new GenAiConfigServiceConfig(config);

    assertNotNull(serviceConfig.getGenAiConfig());
    assertTrue(serviceConfig.getGenAiConfig().isEmpty());
  }

  @Test
  void test_constructor_withNestedConfig() {
    Config config =
        ConfigFactory.parseString(
            "genAiConfig {\n" + "    nested {\n" + "        key = \"value\"\n" + "    }\n" + "}");

    GenAiConfigServiceConfig serviceConfig = new GenAiConfigServiceConfig(config);

    assertNotNull(serviceConfig.getGenAiConfig());
    assertTrue(serviceConfig.getGenAiConfig().hasPath("nested"));
    assertEquals("value", serviceConfig.getGenAiConfig().getString("nested.key"));
  }

  @Test
  void test_constructor_withMultipleConfigs() {
    Config config =
        ConfigFactory.parseString(
            "genAiConfig {\n"
                + "    key1 = \"value1\"\n"
                + "}\n"
                + "otherConfig {\n"
                + "    key2 = \"value2\"\n"
                + "}");

    GenAiConfigServiceConfig serviceConfig = new GenAiConfigServiceConfig(config);

    assertNotNull(serviceConfig.getGenAiConfig());
    assertEquals("value1", serviceConfig.getGenAiConfig().getString("key1"));
    assertFalse(serviceConfig.getGenAiConfig().hasPath("otherConfig"));
  }
}
