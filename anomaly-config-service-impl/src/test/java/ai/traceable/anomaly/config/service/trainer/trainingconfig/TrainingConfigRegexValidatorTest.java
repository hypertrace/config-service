package ai.traceable.anomaly.config.service.trainer.trainingconfig;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.anomaly.config.service.v1.StringList;
import ai.traceable.anomaly.config.service.v1.trainer.ApiNamingTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.CustomRuleConfig;
import ai.traceable.anomaly.config.service.v1.trainer.CustomRulesListConfig;
import ai.traceable.anomaly.config.service.v1.trainer.EnumerableParamTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.JwtParamsTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.MetadataTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.ObjectBolaTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.ParamSusceptibilityConfig;
import ai.traceable.anomaly.config.service.v1.trainer.RejectFilterConfig;
import ai.traceable.anomaly.config.service.v1.trainer.SessionTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.TrieModelTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.UrlFilterConfig;
import ai.traceable.anomaly.config.service.v1.trainer.UrlPathFilterConfig;
import ai.traceable.anomaly.config.service.v1.trainer.VulnerabilityTrainingConfig;
import io.grpc.Status;
import java.util.List;
import org.junit.jupiter.api.Test;

public class TrainingConfigRegexValidatorTest {
  private final TrainingConfigRegexValidator trainingConfigRegexValidator =
      new TrainingConfigRegexValidator();

  @Test
  void testValidateMetadataTrainingConfigRegex() {
    MetadataTrainingConfig config =
        MetadataTrainingConfig.newBuilder()
            .setJwtParams(
                JwtParamsTrainingConfig.newBuilder()
                    .setIncludeParamRegexes(
                        StringList.newBuilder()
                            .addAllValues(
                                List.of(
                                    "/^[(]{0,1}[0-9]{3}[)]{0,1}[-\\s\\.]{0,1}[0-9]{3}[-\\s\\.]{0,1}[0-9]{4}$/\n"))
                            .build())
                    .build())
            .build();
    Status status = trainingConfigRegexValidator.validateMetadataTrainingConfigRegex(config);
    assertEquals(Status.OK.getCode(), status.getCode());
    config =
        MetadataTrainingConfig.newBuilder()
            .setJwtParams(
                JwtParamsTrainingConfig.newBuilder()
                    .setIncludeParamRegexes(
                        StringList.newBuilder().addAllValues(List.of("[")).build())
                    .build())
            .build();

    status = trainingConfigRegexValidator.validateMetadataTrainingConfigRegex(config);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("Invalid Regex pattern: ["));
  }

  @Test
  void testValidateVulnerabilityTrainingConfigRegex() {
    VulnerabilityTrainingConfig config =
        VulnerabilityTrainingConfig.newBuilder()
            .setEnumerableParam(
                EnumerableParamTrainingConfig.newBuilder().setIncludeParamRegex("[").build())
            .build();
    Status status = trainingConfigRegexValidator.validateVulnerabilityTrainingConfigRegex(config);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("Invalid Regex pattern: ["));

    config =
        VulnerabilityTrainingConfig.newBuilder()
            .setEnumerableParam(
                EnumerableParamTrainingConfig.newBuilder()
                    .setIncludeParamRegex(
                        "/^[(]{0,1}[0-9]{3}[)]{0,1}[-\\s\\.]{0,1}[0-9]{3}[-\\s\\.]{0,1}[0-9]{4}$/\n")
                    .build())
            .build();
    status = trainingConfigRegexValidator.validateVulnerabilityTrainingConfigRegex(config);
    assertEquals(Status.OK.getCode(), status.getCode());
  }

  @Test
  void testValidateSessionTrainingConfigRegex() {
    SessionTrainingConfig config =
        SessionTrainingConfig.newBuilder()
            .setObjectBola(
                ObjectBolaTrainingConfig.newBuilder()
                    .setParamSusceptibilityConfig(
                        ParamSusceptibilityConfig.newBuilder()
                            .setRequestParamValueRegex("[")
                            .build())
                    .build())
            .build();

    Status status = trainingConfigRegexValidator.validateSessionTrainingConfigRegex(config);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("Invalid Regex pattern: ["));

    config =
        SessionTrainingConfig.newBuilder()
            .setObjectBola(
                ObjectBolaTrainingConfig.newBuilder()
                    .setParamSusceptibilityConfig(
                        ParamSusceptibilityConfig.newBuilder()
                            .setRequestParamValueRegex(
                                "/^[(]{0,1}[0-9]{3}[)]{0,1}[-\\s\\.]{0,1}[0-9]{3}[-\\s\\.]{0,1}[0-9]{4}$/\n")
                            .build())
                    .build())
            .build();

    status = trainingConfigRegexValidator.validateSessionTrainingConfigRegex(config);
    assertEquals(Status.OK.getCode(), status.getCode());
  }

  @Test
  void testValidateApiNamingTrainingConfigRegex() {
    ApiNamingTrainingConfig config =
        ApiNamingTrainingConfig.newBuilder()
            .setTrieModelTrainingConfig(
                TrieModelTrainingConfig.newBuilder()
                    .setAllowRegexList(StringList.newBuilder().addAllValues(List.of("[")).build())
                    .build())
            .build();

    Status status = trainingConfigRegexValidator.validateApiNamingTrainingConfigRegex(config);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("Invalid Regex pattern: ["));

    config =
        ApiNamingTrainingConfig.newBuilder()
            .setTrieModelTrainingConfig(
                TrieModelTrainingConfig.newBuilder()
                    .setAllowRegexList(
                        StringList.newBuilder()
                            .addAllValues(
                                List.of(
                                    "/^[(]{0,1}[0-9]{3}[)]{0,1}[-\\s\\.]{0,1}[0-9]{3}[-\\s\\.]{0,1}[0-9]{4}$/\n"))
                            .build())
                    .build())
            .build();

    status = trainingConfigRegexValidator.validateApiNamingTrainingConfigRegex(config);
    assertEquals(Status.OK.getCode(), status.getCode());

    config =
        ApiNamingTrainingConfig.newBuilder()
            .setRejectFilterConfig(
                RejectFilterConfig.newBuilder()
                    .setUrlPathFilterConfig(
                        UrlPathFilterConfig.newBuilder()
                            .setUrlPathRegexPatterns(
                                StringList.newBuilder().addAllValues(List.of("[")).build())
                            .build())
                    .build())
            .build();

    status = trainingConfigRegexValidator.validateApiNamingTrainingConfigRegex(config);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("Invalid Regex pattern: ["));

    config =
        ApiNamingTrainingConfig.newBuilder()
            .setRejectFilterConfig(
                RejectFilterConfig.newBuilder()
                    .setUrlPathFilterConfig(
                        UrlPathFilterConfig.newBuilder()
                            .setUrlPathRegexPatterns(
                                StringList.newBuilder()
                                    .addAllValues(
                                        List.of(
                                            "/^[(]{0,1}[0-9]{3}[)]{0,1}[-\\s\\.]{0,1}[0-9]{3}[-\\s\\.]{0,1}[0-9]{4}$/\n"))
                                    .build())
                            .build())
                    .build())
            .build();
    status = trainingConfigRegexValidator.validateApiNamingTrainingConfigRegex(config);
    assertEquals(Status.OK.getCode(), status.getCode());

    config =
        ApiNamingTrainingConfig.newBuilder()
            .setUrlFilterConfig(
                UrlFilterConfig.newBuilder()
                    .setUrlRejectRegexPatterns(
                        StringList.newBuilder().addAllValues(List.of("[")).build())
                    .build())
            .build();

    status = trainingConfigRegexValidator.validateApiNamingTrainingConfigRegex(config);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("Invalid Regex pattern: ["));
    config =
        ApiNamingTrainingConfig.newBuilder()
            .setUrlFilterConfig(
                UrlFilterConfig.newBuilder()
                    .setUrlRejectRegexPatterns(
                        StringList.newBuilder()
                            .addAllValues(
                                List.of(
                                    "/^[(]{0,1}[0-9]{3}[)]{0,1}[-\\s\\.]{0,1}[0-9]{3}[-\\s\\.]{0,1}[0-9]{4}$/\n"))
                            .build())
                    .build())
            .build();

    status = trainingConfigRegexValidator.validateApiNamingTrainingConfigRegex(config);
    assertEquals(Status.OK.getCode(), status.getCode());

    config =
        ApiNamingTrainingConfig.newBuilder()
            .setCustomRulesListConfig(
                CustomRulesListConfig.newBuilder()
                    .addCustomRulesConfig(CustomRuleConfig.newBuilder().setRegex("[").build())
                    .build())
            .build();
    status = trainingConfigRegexValidator.validateApiNamingTrainingConfigRegex(config);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertTrue(status.getDescription().contains("Invalid Regex pattern: ["));
    config =
        ApiNamingTrainingConfig.newBuilder()
            .setCustomRulesListConfig(
                CustomRulesListConfig.newBuilder()
                    .addCustomRulesConfig(
                        CustomRuleConfig.newBuilder()
                            .setRegex(
                                "/^[(]{0,1}[0-9]{3}[)]{0,1}[-\\s\\.]{0,1}[0-9]{3}[-\\s\\.]{0,1}[0-9]{4}$/\n")
                            .build())
                    .build())
            .build();
    status = trainingConfigRegexValidator.validateApiNamingTrainingConfigRegex(config);
    assertEquals(Status.OK.getCode(), status.getCode());
  }
}
