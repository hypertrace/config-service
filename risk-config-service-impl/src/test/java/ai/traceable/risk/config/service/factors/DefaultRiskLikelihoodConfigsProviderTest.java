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
    assertEquals(4, configs.getRiskFactorsCount());

    Map<String, RiskFactor> factorMap =
        configs.getRiskFactorsList().stream()
            .collect(
                Collectors.toMap(
                    riskFactor -> riskFactor.getRiskFactorConfig().getId(), Function.identity()));
    {
      RiskFactor factor = factorMap.get("customTagsLikelihood");
      assertTrue(factor.getIsDefault());
      assertEquals("Tags", factor.getRiskFactorInfo().getName());
      assertEquals(
          RiskFactorType.RISK_FACTOR_TYPE_CUSTOM_TAGS,
          factor.getRiskFactorInfo().getRiskFactorType());
      assertTrue(factor.getRiskFactorInfo().getDescription().length() > 0);
      assertTrue(
          factor
              .getCustomizationOptionsList()
              .contains(CUSTOMIZATION_OPTIONS_FACTOR_SCORE_CONTRIBUTION));
      assertTrue(
          factor.getCustomizationOptionsList().contains(CUSTOMIZATION_OPTIONS_ELEMENT_ADD_DELETE));
      assertEquals(
          RiskFactorScoring.getDefaultInstance(),
          factor.getRiskFactorConfig().getRiskFactorScoring());
      assertEquals(4, factor.getRiskFactorConfig().getRiskElementConfigsCount());
    }
    {
      RiskFactor factor = factorMap.get("motive");
      assertTrue(factor.getIsDefault());
      assertEquals("Motive", factor.getRiskFactorInfo().getName());
      assertEquals(
          RiskFactorType.RISK_FACTOR_TYPE_MOTIVE, factor.getRiskFactorInfo().getRiskFactorType());
      assertTrue(factor.getRiskFactorInfo().getDescription().length() > 0);
      assertEquals(0, factor.getCustomizationOptionsCount());
      assertEquals(
          RiskFactorScoring.getDefaultInstance(),
          factor.getRiskFactorConfig().getRiskFactorScoring());
      assertEquals(1, factor.getRiskFactorConfig().getRiskElementConfigsCount());
    }
    {
      RiskFactor factor = factorMap.get("easeOfAccess");
      assertTrue(factor.getIsDefault());
      assertEquals("Ease Of Access", factor.getRiskFactorInfo().getName());
      assertEquals(
          RiskFactorType.RISK_FACTOR_TYPE_EASE_OF_ACCESS,
          factor.getRiskFactorInfo().getRiskFactorType());
      assertTrue(factor.getRiskFactorInfo().getDescription().length() > 0);
      assertEquals(0, factor.getCustomizationOptionsCount());
      assertEquals(
          RiskFactorScoring.getDefaultInstance(),
          factor.getRiskFactorConfig().getRiskFactorScoring());
      assertEquals(4, factor.getRiskFactorConfig().getRiskElementConfigsCount());
    }
    {
      RiskFactor factor = factorMap.get("exploitSurface");
      assertTrue(factor.getIsDefault());
      assertEquals("Exploit Surface", factor.getRiskFactorInfo().getName());
      assertEquals(
          RiskFactorType.RISK_FACTOR_TYPE_EXPLOIT_SURFACE,
          factor.getRiskFactorInfo().getRiskFactorType());
      assertTrue(factor.getRiskFactorInfo().getDescription().length() > 0);
      assertEquals(0, factor.getCustomizationOptionsCount());
      assertEquals(
          RiskFactorScoring.getDefaultInstance(),
          factor.getRiskFactorConfig().getRiskFactorScoring());
      assertEquals(5, factor.getRiskFactorConfig().getRiskElementConfigsCount());
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
                        + "  },\n"
                        + "  {\n"
                        + "    riskFactorInfo = {\n"
                        + "      name = \"Exploit Surface\"\n"
                        + "      riskFactorType = RISK_FACTOR_TYPE_EXPLOIT_SURFACE\n"
                        + "    }\n"
                        + "    riskFactorConfig = {\n"
                        + "      id = \"exploitSurface\"\n"
                        + "      riskElementConfigs = [\n"
                        + "        {\n"
                        + "          id = \"requestNoParams\"\n"
                        + "          riskElementInfo = {\n"
                        + "            name = \"Request has No Params\"\n"
                        + "            requestParamsCount = {\n"
                        + "              operator = INT_OPERATOR_EQUALS\n"
                        + "              value = 0\n"
                        + "            }\n"
                        + "          }\n"
                        + "          riskElementScoring = {\n"
                        + "            score = 0\n"
                        + "          }\n"
                        + "        },\n"
                        + "        {\n"
                        + "          id = \"request1Param\"\n"
                        + "          riskElementInfo = {\n"
                        + "            name = \"Request has only 1 Param\"\n"
                        + "            requestParamsCount = {\n"
                        + "              operator = INT_OPERATOR_EQUALS\n"
                        + "              value = 1\n"
                        + "            }\n"
                        + "          }\n"
                        + "          riskElementScoring = {\n"
                        + "            score = 1\n"
                        + "          }\n"
                        + "        },\n"
                        + "        {\n"
                        + "          id = \"request2orMoreParams\"\n"
                        + "          riskElementInfo = {\n"
                        + "            name = \"Request has 2 or more Params\"\n"
                        + "            requestParamsCount = {\n"
                        + "              operator = INT_OPERATOR_GREATER_THAN_EQUALS\n"
                        + "              value = 2\n"
                        + "            }\n"
                        + "          }\n"
                        + "          riskElementScoring = {\n"
                        + "            score = 3\n"
                        + "          }\n"
                        + "        },\n"
                        + "        {\n"
                        + "          id = \"request5orMoreParams\"\n"
                        + "          riskElementInfo = {\n"
                        + "            name = \"Request has 5 or more Params\"\n"
                        + "            requestParamsCount = {\n"
                        + "              operator = INT_OPERATOR_GREATER_THAN_EQUALS\n"
                        + "              value = 5\n"
                        + "            }\n"
                        + "          }\n"
                        + "          riskElementScoring = {\n"
                        + "            score = 7\n"
                        + "          }\n"
                        + "        },\n"
                        + "        {\n"
                        + "          id = \"request10orMoreParams\"\n"
                        + "          riskElementInfo = {\n"
                        + "            name = \"Request has 10 or more Params\"\n"
                        + "            requestParamsCount = {\n"
                        + "              operator = INT_OPERATOR_GREATER_THAN_EQUALS\n"
                        + "              value = 10\n"
                        + "            }\n"
                        + "          }\n"
                        + "          riskElementScoring = {\n"
                        + "            score = 10\n"
                        + "          }\n"
                        + "        }\n"
                        + "      ]\n"
                        + "    }\n"
                        + "  }"
                        + "]}\n")));
    RiskContributorConfigs configs = provider.get();
    assertEquals(5, configs.getRiskFactorsCount());

    Map<String, RiskFactor> factorMap =
        configs.getRiskFactorsList().stream()
            .collect(
                Collectors.toMap(
                    riskFactor -> riskFactor.getRiskFactorConfig().getId(), Function.identity()));
    {
      RiskFactor factor = factorMap.get("customTagsLikelihood");
      assertTrue(factor.getIsDefault());
      assertEquals("Custom Tags", factor.getRiskFactorInfo().getName());
      assertEquals(
          RiskFactorType.RISK_FACTOR_TYPE_CUSTOM_TAGS,
          factor.getRiskFactorInfo().getRiskFactorType());
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
      assertEquals("Motive", factor.getRiskFactorInfo().getName());
      assertEquals(
          RiskFactorType.RISK_FACTOR_TYPE_MOTIVE, factor.getRiskFactorInfo().getRiskFactorType());
      assertEquals(0, factor.getCustomizationOptionsCount());
      assertEquals(
          RiskFactorScoring.getDefaultInstance(),
          factor.getRiskFactorConfig().getRiskFactorScoring());
      assertEquals(1, factor.getRiskFactorConfig().getRiskElementConfigsCount());
    }
    {
      RiskFactor factor = factorMap.get("easeOfAccess");
      assertTrue(factor.getIsDefault());
      assertEquals("Ease Of Api Access", factor.getRiskFactorInfo().getName());
      assertEquals(
          RiskFactorType.RISK_FACTOR_TYPE_EASE_OF_ACCESS,
          factor.getRiskFactorInfo().getRiskFactorType());
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
