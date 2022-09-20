package ai.traceable.anomaly.config.service.aggregator;

import ai.traceable.anomaly.config.service.aggregator.config.AggregationConfigServiceConfig;
import ai.traceable.anomaly.config.service.v1.aggregator.AggregationConfig;
import ai.traceable.anomaly.config.service.v1.aggregator.EventAggregationGlobalConfig;
import com.typesafe.config.ConfigFactory;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AggregationConfigServiceConfigTest {

  @Test
  void testConfig() {
    AggregationConfigServiceConfig config =
        new AggregationConfigServiceConfig(
            ConfigFactory.parseString(
                "apiDefinitionAggregationConfig = {\n"
                    + "     max_events_with_score_per_param_value = 2\n"
                    + "     max_events_without_score_per_param_value = 2\n"
                    + "     max_events_with_score_per_param_across_values = 2\n"
                    + "     max_events_without_score_per_param_across_values = 1\n"
                    + "     max_events_with_score_per_user_across_params = 5\n"
                    + "     max_events_without_score_per_user_across_params = 1\n"
                    + "     max_users_per_param = 10\n"
                    + "  }\n"
                    + "  modsecAggregationConfig = {\n"
                    + "     max_events_with_score_per_param_value = 1\n"
                    + "     max_events_without_score_per_param_value = 10\n"
                    + "     max_events_with_score_per_param_across_values = 10\n"
                    + "     max_events_without_score_per_param_across_values = 100\n"
                    + "     max_events_with_score_per_user_across_params = 15\n"
                    + "     max_events_without_score_per_user_across_params = 1\n"
                    + "     max_users_per_param = 10\n"
                    + "  }\n"
                    + "  sessionAggregationConfig = {\n"
                    + "     max_events_with_score_per_param_value = 1\n"
                    + "     max_events_without_score_per_param_value = 10\n"
                    + "     max_events_with_score_per_param_across_values = 10\n"
                    + "     max_events_without_score_per_param_across_values = 100\n"
                    + "     max_events_with_score_per_user_across_params = 15\n"
                    + "     max_events_without_score_per_user_across_params = 1\n"
                    + "     max_users_per_param = 10\n"
                    + "  }\n"
                    + "  customSignatureAggregationConfig = {\n"
                    + "     max_events_with_score_per_user_across_params = 100\n"
                    + "     max_events_without_score_per_user_across_params = 1000\n"
                    + "  }\n"
                    + "  globalAggregationConfig = {\n"
                    + "     param_name_max_duration = 1d\n"
                    + "     param_value_max_duration = 1d\n"
                    + "     user_max_duration = 1d\n"
                    + "  }"));

    Optional<AggregationConfig> modsecConfig = config.getDefaultModsecAggregationConfig();
    Assertions.assertEquals(1, modsecConfig.get().getMaxEventsWithScorePerParamValue());
    Assertions.assertEquals(10, modsecConfig.get().getMaxEventsWithoutScorePerParamValue());

    Optional<AggregationConfig> apiDefConfig = config.getDefaultApiDefinitionAggregationConfig();
    Assertions.assertEquals(2, apiDefConfig.get().getMaxEventsWithScorePerParamValue());
    Assertions.assertEquals(2, apiDefConfig.get().getMaxEventsWithoutScorePerParamValue());

    Optional<AggregationConfig> sessionConfig = config.getDefaultSessionAggregationConfig();
    Assertions.assertEquals(1, sessionConfig.get().getMaxEventsWithScorePerParamValue());
    Assertions.assertEquals(10, sessionConfig.get().getMaxEventsWithoutScorePerParamValue());

    Optional<AggregationConfig> customSignatureConfig =
        config.getDefaultCustomSignatureAggregationConfig();
    Assertions.assertEquals(
        100, customSignatureConfig.get().getMaxEventsWithScorePerUserAcrossParams());
    Assertions.assertEquals(
        1000, customSignatureConfig.get().getMaxEventsWithoutScorePerUserAcrossParams());

    Optional<EventAggregationGlobalConfig> globalConfig =
        config.getDefaultGlobalAggregationConfig();

    Assertions.assertEquals("1d", globalConfig.get().getParamNameMaxDuration());
    Assertions.assertEquals("1d", globalConfig.get().getParamValueMaxDuration());
  }

  @Test
  public void testWhenSomeConfigsNotPresent() {
    AggregationConfigServiceConfig config =
        new AggregationConfigServiceConfig(
            ConfigFactory.parseString(
                "apiDefinitionAggregationConfig = {\n"
                    + "     max_events_with_score_per_param_value =2\n"
                    + "     max_events_without_score_per_param_value =2\n"
                    + "     max_events_with_score_per_param_across_values = 2\n"
                    + "     max_events_without_score_per_param_across_values = 0\n"
                    + "     max_events_with_score_per_user_across_params = 5\n"
                    + "     max_events_without_score_per_user_across_params = 0\n"
                    + "     max_users_per_param = 10\n"
                    + "  }\n"
                    + "  modsecAggregationConfig = {\n"
                    + "     max_events_with_score_per_param_value = 1\n"
                    + "     max_events_without_score_per_param_value = 10\n"
                    + "     max_events_with_score_per_param_across_values = 10\n"
                    + "     max_events_without_score_per_param_across_values = 100\n"
                    + "     max_events_with_score_per_user_across_params = 15\n"
                    + "     max_events_without_score_per_user_across_params = 2\n"
                    + "     max_users_per_param = 10\n"
                    + "  }"));

    Optional<AggregationConfig> modsecConfig = config.getDefaultModsecAggregationConfig();
    Assertions.assertEquals(1, modsecConfig.get().getMaxEventsWithScorePerParamValue());
    Assertions.assertEquals(10, modsecConfig.get().getMaxEventsWithoutScorePerParamValue());

    Optional<AggregationConfig> apiDefConfig = config.getDefaultApiDefinitionAggregationConfig();
    Assertions.assertEquals(2, apiDefConfig.get().getMaxEventsWithScorePerParamValue());
    Assertions.assertEquals(2, apiDefConfig.get().getMaxEventsWithoutScorePerParamValue());

    Optional<AggregationConfig> sessionConfig = config.getDefaultSessionAggregationConfig();
    Assertions.assertTrue(sessionConfig.isEmpty());
    Optional<EventAggregationGlobalConfig> globalConfig =
        config.getDefaultGlobalAggregationConfig();
    Assertions.assertTrue(globalConfig.isEmpty());
  }
}
