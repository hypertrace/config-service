package ai.traceable.risk.config.service.factors;

import static ai.traceable.risk.config.service.v1.CustomizationOptions.CUSTOMIZATION_OPTIONS_ELEMENT_ADD_DELETE;
import static ai.traceable.risk.config.service.v1.CustomizationOptions.CUSTOMIZATION_OPTIONS_FACTOR_SCORE_CONTRIBUTION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.risk.config.service.RiskConfigServiceConfig;
import ai.traceable.risk.config.service.factors.processor.utils.RiskContributorConfigUtils;
import ai.traceable.risk.config.service.factors.processor.utils.RiskElementConfigUtils;
import ai.traceable.risk.config.service.factors.processor.utils.RiskFactorConfigUtils;
import ai.traceable.risk.config.service.factors.processor.utils.RiskFactorListUtils;
import ai.traceable.risk.config.service.v1.RiskContributorConfigs;
import ai.traceable.risk.config.service.v1.RiskFactor;
import ai.traceable.risk.config.service.v1.RiskFactorInfo;
import ai.traceable.risk.config.service.v1.RiskFactorScoring;
import ai.traceable.risk.config.service.v1.RiskFactorType;
import com.typesafe.config.ConfigFactory;
import io.grpc.StatusRuntimeException;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

public class DefaultRiskLikelihoodConfigsProviderTest {

  private final RiskContributorConfigUtils configUtils =
      new RiskContributorConfigUtils(
          new RiskFactorListUtils(new RiskFactorConfigUtils(new RiskElementConfigUtils())));

  @Test
  public void testGetNoServiceConfig() {
    DefaultRiskLikelihoodConfigsProvider provider =
        new DefaultRiskLikelihoodConfigsProvider(
            configUtils, new RiskConfigServiceConfig(ConfigFactory.empty()));
    RiskContributorConfigs configs = provider.get();
    assertEquals(3, configs.getRiskFactorsCount());

    Map<String, RiskFactor> factorMap =
        configs.getRiskFactorsList().stream()
            .collect(
                Collectors.toMap(
                    riskFactor -> riskFactor.getRiskFactorConfig().getId(), Function.identity()));
    {
      RiskFactor factor = factorMap.get("customTagsLikelihood");
      assertTrue(factor.getIsDefault());
      assertEquals(
          RiskFactorInfo.newBuilder()
              .setName("Custom Tags")
              .setRiskFactorType(RiskFactorType.RISK_FACTOR_TYPE_CUSTOM_TAGS)
              .build(),
          factor.getRiskFactorInfo());
      assertTrue(
          factor
              .getCustomizationOptionsList()
              .contains(CUSTOMIZATION_OPTIONS_FACTOR_SCORE_CONTRIBUTION));
      assertTrue(
          factor.getCustomizationOptionsList().contains(CUSTOMIZATION_OPTIONS_ELEMENT_ADD_DELETE));
      assertEquals(
          RiskFactorScoring.getDefaultInstance(),
          factor.getRiskFactorConfig().getRiskFactorScoring());
      assertEquals(2, factor.getRiskFactorConfig().getRiskElementConfigsCount());
    }
    {
      RiskFactor factor = factorMap.get("motive");
      assertTrue(factor.getIsDefault());
      assertEquals(
          RiskFactorInfo.newBuilder()
              .setName("Motive")
              .setRiskFactorType(RiskFactorType.RISK_FACTOR_TYPE_MOTIVE)
              .build(),
          factor.getRiskFactorInfo());
      assertEquals(0, factor.getCustomizationOptionsCount());
      assertEquals(
          RiskFactorScoring.getDefaultInstance(),
          factor.getRiskFactorConfig().getRiskFactorScoring());
      assertEquals(1, factor.getRiskFactorConfig().getRiskElementConfigsCount());
    }
    {
      RiskFactor factor = factorMap.get("easeOfAccess");
      assertTrue(factor.getIsDefault());
      assertEquals(
          RiskFactorInfo.newBuilder()
              .setName("Ease Of Access")
              .setRiskFactorType(RiskFactorType.RISK_FACTOR_TYPE_EASE_OF_ACCESS)
              .build(),
          factor.getRiskFactorInfo());
      assertEquals(0, factor.getCustomizationOptionsCount());
      assertEquals(
          RiskFactorScoring.getDefaultInstance(),
          factor.getRiskFactorConfig().getRiskFactorScoring());
      assertEquals(4, factor.getRiskFactorConfig().getRiskElementConfigsCount());
    }
  }

  @Test
  public void testGetWithServiceConfig() {
    DefaultRiskLikelihoodConfigsProvider provider =
        new DefaultRiskLikelihoodConfigsProvider(
            configUtils,
            new RiskConfigServiceConfig(
                ConfigFactory.parseString(
                    "riskLikelihoodConfigs={riskFactors = [\n"
                        + "  {\n"
                        + "    riskFactorInfo = {\n"
                        + "      name = \"Custom Tags\"\n"
                        + "      riskFactorType = RISK_FACTOR_TYPE_CUSTOM_TAGS\n"
                        + "    }\n"
                        + "    riskFactorConfig = {\n"
                        + "      id = \"customTagsLikelihood\"\n"
                        + "      riskElementConfigs = [\n"
                        + "        {\n"
                        + "          id = \"criticalLikelihood\"\n"
                        + "          riskElementInfo = {\n"
                        + "            name = \"Critical\"\n"
                        + "            labelId = {\n"
                        + "              operator = STRING_OPERATOR_EQUALS\n"
                        + "              value = \"Critical\"\n"
                        + "            }\n"
                        + "          }\n"
                        + "          riskElementScoring = {\n"
                        + "            score = 7\n"
                        + "          }\n"
                        + "        }\n"
                        + "      ]\n"
                        + "    }\n"
                        + "    customizationOptions = [CUSTOMIZATION_OPTIONS_FACTOR_SCORE_CONTRIBUTION]\n"
                        + "  },\n"
                        + "  {\n"
                        + "    riskFactorInfo = {\n"
                        + "      name = \"Motive2\"\n"
                        + "      riskFactorType = RISK_FACTOR_TYPE_MOTIVE\n"
                        + "    }\n"
                        + "    riskFactorConfig = {\n"
                        + "      id = \"motive2\"\n"
                        + "      riskElementConfigs = [\n"
                        + "        {\n"
                        + "          id = \"responseHasPii3\"\n"
                        + "          riskElementInfo = {\n"
                        + "            name = \"Response has PII\"\n"
                        + "            responsePiiCount = {\n"
                        + "              operator = INT_OPERATOR_EQUALS\n"
                        + "              value = 0\n"
                        + "            }\n"
                        + "          }\n"
                        + "          riskElementScoring = {\n"
                        + "            score = 5\n"
                        + "          }\n"
                        + "        }\n"
                        + "      ]\n"
                        + "    }\n"
                        + "  },\n"
                        + "  {\n"
                        + "    riskFactorInfo = {\n"
                        + "      name = \"Ease Of Api Access\"\n"
                        + "      riskFactorType = RISK_FACTOR_TYPE_EASE_OF_ACCESS\n"
                        + "    }\n"
                        + "    riskFactorConfig = {\n"
                        + "      id = \"easeOfAccess\"\n"
                        + "    }\n"
                        + "  }\n"
                        + "]}")));
    RiskContributorConfigs configs = provider.get();
    assertEquals(4, configs.getRiskFactorsCount());

    Map<String, RiskFactor> factorMap =
        configs.getRiskFactorsList().stream()
            .collect(
                Collectors.toMap(
                    riskFactor -> riskFactor.getRiskFactorConfig().getId(), Function.identity()));
    {
      RiskFactor factor = factorMap.get("customTagsLikelihood");
      assertTrue(factor.getIsDefault());
      assertEquals(
          RiskFactorInfo.newBuilder()
              .setName("Custom Tags")
              .setRiskFactorType(RiskFactorType.RISK_FACTOR_TYPE_CUSTOM_TAGS)
              .build(),
          factor.getRiskFactorInfo());
      assertTrue(
          factor
              .getCustomizationOptionsList()
              .contains(CUSTOMIZATION_OPTIONS_FACTOR_SCORE_CONTRIBUTION));
      assertFalse(
          factor.getCustomizationOptionsList().contains(CUSTOMIZATION_OPTIONS_ELEMENT_ADD_DELETE));
      assertEquals(
          RiskFactorScoring.getDefaultInstance(),
          factor.getRiskFactorConfig().getRiskFactorScoring());
      assertEquals(1, factor.getRiskFactorConfig().getRiskElementConfigsCount());
    }
    {
      RiskFactor factor = factorMap.get("motive");
      assertTrue(factor.getIsDefault());
      assertEquals(
          RiskFactorInfo.newBuilder()
              .setName("Motive")
              .setRiskFactorType(RiskFactorType.RISK_FACTOR_TYPE_MOTIVE)
              .build(),
          factor.getRiskFactorInfo());
      assertEquals(0, factor.getCustomizationOptionsCount());
      assertEquals(
          RiskFactorScoring.getDefaultInstance(),
          factor.getRiskFactorConfig().getRiskFactorScoring());
      assertEquals(1, factor.getRiskFactorConfig().getRiskElementConfigsCount());
    }
    {
      RiskFactor factor = factorMap.get("easeOfAccess");
      assertTrue(factor.getIsDefault());
      assertEquals(
          RiskFactorInfo.newBuilder()
              .setName("Ease Of Api Access")
              .setRiskFactorType(RiskFactorType.RISK_FACTOR_TYPE_EASE_OF_ACCESS)
              .build(),
          factor.getRiskFactorInfo());
      assertEquals(0, factor.getCustomizationOptionsCount());
      assertEquals(
          RiskFactorScoring.getDefaultInstance(),
          factor.getRiskFactorConfig().getRiskFactorScoring());
      assertEquals(0, factor.getRiskFactorConfig().getRiskElementConfigsCount());
    }
  }

  @Test
  public void testGetWithInvalidServiceConfig() {
    assertThrows(
        StatusRuntimeException.class,
        () ->
            new DefaultRiskLikelihoodConfigsProvider(
                configUtils,
                new RiskConfigServiceConfig(
                    ConfigFactory.parseString(
                        "riskLikelihoodConfigs={riskFactors = [\n"
                            + "  {\n"
                            + "    riskFactorInfo = {\n"
                            + "      name = \"Motive\"\n"
                            + "      riskFactorType = RISK_FACTOR_TYPE_MOTIVE\n"
                            + "    }\n"
                            + "    riskFactorConfig = {\n"
                            + "      id = \"motive\"\n"
                            + "      riskElementConfigs = [\n"
                            + "        {\n"
                            + "          id = \"responseHasPii\"\n"
                            + "          riskElementInfo = {\n"
                            + "            name = \"Response has PII\"\n"
                            + "            responsePiiCount = {\n"
                            + "              operator = INT_OPERATOR_EQUALS\n"
                            + "              value = 0\n"
                            + "            }\n"
                            + "          }\n"
                            + "          riskElementScoring = {\n"
                            + "            score = 18\n"
                            + "          }\n"
                            + "        }\n"
                            + "      ]\n"
                            + "    }\n"
                            + "  }\n"
                            + "]}"))));
    assertThrows(
        StatusRuntimeException.class,
        () ->
            new DefaultRiskLikelihoodConfigsProvider(
                configUtils,
                new RiskConfigServiceConfig(
                    ConfigFactory.parseString(
                        "riskLikelihoodConfigs={riskFactors = [\n"
                            + "  {\n"
                            + "    riskFactorInfo = {\n"
                            + "      name = \"Motive\"\n"
                            + "      riskFactorType = RISK_FACTOR_TYPE_MOTIVE\n"
                            + "    }\n"
                            + "  }\n"
                            + "]}"))));
  }
}
