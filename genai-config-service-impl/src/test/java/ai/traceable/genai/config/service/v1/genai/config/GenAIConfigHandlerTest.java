package ai.traceable.genai.config.service.v1.genai.config;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.genai.config.service.v1.GenAiConfig;
import ai.traceable.genai.config.service.v1.GenAiFeatureConfigUpdate;
import ai.traceable.genai.config.service.v1.IssuesSummaryFeatureConfig;
import ai.traceable.genai.config.service.v1.ThreatActivitySummaryFeatureConfig;
import ai.traceable.genai.config.service.v1.feature.config.GenAiFeatureConfigHandler;
import ai.traceable.genai.config.service.v1.feature.config.IssuesSummaryFeatureConfigHandler;
import ai.traceable.genai.config.service.v1.feature.config.ThreatActivitySummaryFeatureConfigHandler;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GenAiConfigHandlerTest {

  private GenAiConfigHandler configHandler;
  private GenAiFeatureConfigHandler<IssuesSummaryFeatureConfig> issuesHandler;
  private GenAiFeatureConfigHandler<ThreatActivitySummaryFeatureConfig> threatActivityHandler;

  @BeforeEach
  void setUp() {
    issuesHandler = new IssuesSummaryFeatureConfigHandler();
    threatActivityHandler = new ThreatActivitySummaryFeatureConfigHandler();
    configHandler = new GenAiConfigHandler(Set.of(issuesHandler, threatActivityHandler));
  }

  @Test
  void test_mergeConfigs_whenHighPriorityHasConfig() {
    IssuesSummaryFeatureConfig highPriorityFeatureConfig =
        IssuesSummaryFeatureConfig.newBuilder().setEnabled(true).build();
    IssuesSummaryFeatureConfig lowPriorityFeatureConfig =
        IssuesSummaryFeatureConfig.newBuilder().setEnabled(false).build();
    ThreatActivitySummaryFeatureConfig lowPriorityThreatActivityFeatureConfig =
        ThreatActivitySummaryFeatureConfig.newBuilder().setEnabled(true).build();

    GenAiConfig highPriority =
        GenAiConfig.newBuilder().setIssuesSummaryFeatureConfig(highPriorityFeatureConfig).build();
    GenAiConfig lowPriority =
        GenAiConfig.newBuilder()
            .setIssuesSummaryFeatureConfig(lowPriorityFeatureConfig)
            .setThreatActivitySummaryFeatureConfig(lowPriorityThreatActivityFeatureConfig)
            .build();

    GenAiConfig result = configHandler.mergeConfigs(highPriority, lowPriority);

    assertTrue(result.hasIssuesSummaryFeatureConfig());
    assertTrue(result.getIssuesSummaryFeatureConfig().getEnabled());
    assertTrue(result.hasThreatActivitySummaryFeatureConfig());
    assertTrue(result.getThreatActivitySummaryFeatureConfig().getEnabled());
  }

  @Test
  void test_mergeConfigs_whenOnlyLowPriorityHasConfig() {
    IssuesSummaryFeatureConfig lowPriorityFeatureConfig =
        IssuesSummaryFeatureConfig.newBuilder().setEnabled(true).build();

    GenAiConfig highPriority = GenAiConfig.getDefaultInstance();
    GenAiConfig lowPriority =
        GenAiConfig.newBuilder().setIssuesSummaryFeatureConfig(lowPriorityFeatureConfig).build();

    GenAiConfig result = configHandler.mergeConfigs(highPriority, lowPriority);

    assertTrue(result.hasIssuesSummaryFeatureConfig());
    assertTrue(result.getIssuesSummaryFeatureConfig().getEnabled());
  }

  @Test
  void test_mergeConfigs_whenNeitherHasConfig() {
    GenAiConfig highPriority = GenAiConfig.getDefaultInstance();
    GenAiConfig lowPriority = GenAiConfig.getDefaultInstance();

    GenAiConfig result = configHandler.mergeConfigs(highPriority, lowPriority);

    assertFalse(result.hasIssuesSummaryFeatureConfig());
  }

  @Test
  void test_getGenAiConfigFromUpdate_whenUpdateHasConfig() {
    IssuesSummaryFeatureConfig featureConfig =
        IssuesSummaryFeatureConfig.newBuilder().setEnabled(true).build();
    GenAiFeatureConfigUpdate update =
        GenAiFeatureConfigUpdate.newBuilder().setIssuesSummaryFeatureConfig(featureConfig).build();

    GenAiConfig result = configHandler.getGenAiConfigFromUpdate(update);

    assertTrue(result.hasIssuesSummaryFeatureConfig());
    assertTrue(result.getIssuesSummaryFeatureConfig().getEnabled());
  }

  @Test
  void test_getGenAiConfigFromUpdate_whenUpdateDoesNotHaveConfig() {
    GenAiFeatureConfigUpdate update = GenAiFeatureConfigUpdate.getDefaultInstance();

    GenAiConfig result = configHandler.getGenAiConfigFromUpdate(update);

    assertFalse(result.hasIssuesSummaryFeatureConfig());
  }

  @Test
  void test_mergeConfigs_withMultipleHandlers() {
    GenAiFeatureConfigHandler<IssuesSummaryFeatureConfig> secondHandler =
        new IssuesSummaryFeatureConfigHandler();
    configHandler = new GenAiConfigHandler(Set.of(issuesHandler, secondHandler));

    IssuesSummaryFeatureConfig highPriorityFeatureConfig =
        IssuesSummaryFeatureConfig.newBuilder().setEnabled(true).build();

    GenAiConfig highPriority =
        GenAiConfig.newBuilder().setIssuesSummaryFeatureConfig(highPriorityFeatureConfig).build();
    GenAiConfig lowPriority = GenAiConfig.getDefaultInstance();

    GenAiConfig result = configHandler.mergeConfigs(highPriority, lowPriority);

    assertTrue(result.hasIssuesSummaryFeatureConfig());
    assertTrue(result.getIssuesSummaryFeatureConfig().getEnabled());
  }
}
