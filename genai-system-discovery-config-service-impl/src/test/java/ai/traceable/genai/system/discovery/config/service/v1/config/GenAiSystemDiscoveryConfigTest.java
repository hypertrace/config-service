package ai.traceable.genai.system.discovery.config.service.v1.config;

import static org.junit.jupiter.api.Assertions.*;

import ai.traceable.genai.system.discovery.config.service.v1.validation.GenAiSystemDiscoveryRulesValidatorImpl;
import org.junit.jupiter.api.Test;

class GenAiSystemDiscoveryConfigTest {
  private final GenAiSystemDiscoveryConfig config =
      new GenAiSystemDiscoveryConfig(new GenAiSystemDiscoveryRulesValidatorImpl());

  @Test
  void test() {
    assertDoesNotThrow(config::getDefaultGenAiSystemDiscoveryRuleMap);
  }
}
