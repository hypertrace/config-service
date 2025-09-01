package ai.traceable.genai.config.service.v1.genai.config;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import ai.traceable.genai.config.service.v1.GenAiConfig;
import ai.traceable.genai.config.service.v1.GenAiConfigServiceConfig;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DefaultGenAiConfigProviderTest {

  @Mock private GenAiConfigServiceConfig configServiceConfig;
  private DefaultGenAiConfigProvider provider;

  @BeforeEach
  void setUp() {
    provider = new DefaultGenAiConfigProvider(configServiceConfig);
  }

  @Test
  void test_get_configServiceConfigExists_prioritiseServiceConfig() {
    Config mockConfig =
        ConfigFactory.parseString(
            "{\n" + "  issuesSummaryFeatureConfig: {\n" + "    enabled: false\n" + "  }\n" + "}");

    when(configServiceConfig.getGenAiConfig()).thenReturn(mockConfig);
    GenAiConfig config = provider.get();

    assertNotNull(config);
    assertTrue(config.hasIssuesSummaryFeatureConfig());
    assertFalse(config.getIssuesSummaryFeatureConfig().getEnabled());
  }

  @Test
  void test_get_configServiceConfigExists_chatbotFeatureConfig() {
    Config mockConfig =
        ConfigFactory.parseString(
            "{\n" + "  chatBotFeatureConfig: {\n" + "    enabled: true\n" + "  }\n" + "}");

    when(configServiceConfig.getGenAiConfig()).thenReturn(mockConfig);
    GenAiConfig config = provider.get();

    assertNotNull(config);
    assertTrue(config.hasChatBotFeatureConfig());
    assertTrue(config.getChatBotFeatureConfig().getEnabled());
  }
}
