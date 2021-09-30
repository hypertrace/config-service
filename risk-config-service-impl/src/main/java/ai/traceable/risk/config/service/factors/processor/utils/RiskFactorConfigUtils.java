package ai.traceable.risk.config.service.factors.processor.utils;

import static ai.traceable.risk.config.service.v1.CustomizationOptions.CUSTOMIZATION_OPTIONS_ELEMENT_ADD_DELETE;
import static ai.traceable.risk.config.service.v1.CustomizationOptions.CUSTOMIZATION_OPTIONS_FACTOR_SCORE_CONTRIBUTION;

import ai.traceable.risk.config.service.processor.RiskConfigUtils;
import ai.traceable.risk.config.service.v1.RiskElementConfig;
import ai.traceable.risk.config.service.v1.RiskFactor;
import ai.traceable.risk.config.service.v1.RiskFactorConfig;
import ai.traceable.risk.config.service.v1.RiskFactorScoring;
import io.grpc.Status;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.inject.Inject;

public class RiskFactorConfigUtils extends RiskConfigUtils<RiskFactorConfig> {

  private final RiskElementConfigUtils riskElementConfigUtils;

  @Inject
  public RiskFactorConfigUtils(RiskElementConfigUtils riskElementConfigUtils) {
    this.riskElementConfigUtils = riskElementConfigUtils;
  }

  @Override
  public RiskFactorConfig.Builder getNewBuilder() {
    return RiskFactorConfig.newBuilder();
  }

  @Override
  public RiskFactorConfig mergeConfigs(
      RiskFactorConfig highPriorityConfig, RiskFactorConfig lowPriorityConfig) {
    if (highPriorityConfig.equals(RiskFactorConfig.getDefaultInstance())) {
      return lowPriorityConfig;
    }
    return highPriorityConfig;
  }

  RiskFactor mergeConfigs(
      RiskFactorConfig specificConfig,
      List<RiskElementConfig> elementConfigs,
      RiskFactor defaultConfig) {

    RiskFactorConfig.Builder mergedBuilder =
        RiskFactorConfig.newBuilder()
            .setId(defaultConfig.getRiskFactorConfig().getId())
            .setRiskFactorScoring(
                specificConfig.hasRiskFactorScoring()
                    ? mergeConfigs(
                        specificConfig.getRiskFactorScoring(),
                        defaultConfig.getRiskFactorConfig().getRiskFactorScoring(),
                        defaultConfig
                            .getCustomizationOptionsList()
                            .contains(CUSTOMIZATION_OPTIONS_FACTOR_SCORE_CONTRIBUTION))
                    : defaultConfig.getRiskFactorConfig().getRiskFactorScoring());

    Map<String, RiskElementConfig> elementConfigMap =
        mergeConfigs(
                specificConfig.getRiskElementConfigsList(),
                defaultConfig.getRiskFactorConfig().getRiskElementConfigsList(),
                defaultConfig
                    .getCustomizationOptionsList()
                    .contains(CUSTOMIZATION_OPTIONS_ELEMENT_ADD_DELETE),
                specificConfig.getId())
            .stream()
            .collect(Collectors.toMap(RiskElementConfig::getId, Function.identity()));

    if (!defaultConfig
        .getCustomizationOptionsList()
        .contains(CUSTOMIZATION_OPTIONS_ELEMENT_ADD_DELETE)) {
      // risk element associations with the factors cannot be modified by user
      // their configs are stored separately..
      for (RiskElementConfig config : elementConfigs) {
        if (elementConfigMap.containsKey(config.getId())) {
          elementConfigMap.put(config.getId(), config);
        }
      }
    }

    RiskFactorConfig mergedConfig =
        mergedBuilder.addAllRiskElementConfigs(elementConfigMap.values()).build();

    return defaultConfig.toBuilder()
        .setRiskFactorConfig(mergedConfig)
        .setIsDefault(isConfigDefault(mergedConfig, defaultConfig.getRiskFactorConfig()))
        .build();
  }

  @Override
  public boolean isConfigDefault(RiskFactorConfig specificConfig, RiskFactorConfig defaultConfig) {
    if (!specificConfig.getRiskFactorScoring().equals(defaultConfig.getRiskFactorScoring())) {
      return false;
    }
    if (specificConfig.getRiskElementConfigsCount() != defaultConfig.getRiskElementConfigsCount()) {
      return false;
    }
    Map<String, RiskElementConfig> elementConfigMap =
        defaultConfig.getRiskElementConfigsList().stream()
            .collect(Collectors.toMap(RiskElementConfig::getId, Function.identity()));
    for (RiskElementConfig config : specificConfig.getRiskElementConfigsList()) {
      if (!elementConfigMap.containsKey(config.getId())
          || !riskElementConfigUtils.isConfigDefault(
              config, elementConfigMap.get(config.getId()))) {
        return false;
      }
    }
    return true;
  }

  @Override
  public Status validateConfig(RiskFactorConfig config) {
    if (config.getId().isBlank()) {
      return Status.INVALID_ARGUMENT.withDescription("Factor should have a valid ID");
    }
    for (RiskElementConfig elementConfig : config.getRiskElementConfigsList()) {
      Status status = riskElementConfigUtils.validateConfig(elementConfig);
      if (!status.isOk()) {
        return status;
      }
    }
    return Status.OK;
  }

  private RiskFactorScoring mergeConfigs(
      RiskFactorScoring highPriorityConfig,
      RiskFactorScoring lowPriorityConfig,
      boolean overrideScoreContribution) {
    if (overrideScoreContribution) {
      return highPriorityConfig;
    } else {
      return lowPriorityConfig.toBuilder().setDisabled(highPriorityConfig.getDisabled()).build();
    }
  }

  private Collection<RiskElementConfig> mergeConfigs(
      List<RiskElementConfig> highPriorityConfigs,
      List<RiskElementConfig> lowPriorityConfigs,
      boolean overrideElements,
      String factorId) {
    if (overrideElements) {
      return highPriorityConfigs;
    } else {
      Map<String, RiskElementConfig> elementConfigMap =
          lowPriorityConfigs.stream()
              .collect(Collectors.toMap(RiskElementConfig::getId, Function.identity()));
      for (RiskElementConfig config : highPriorityConfigs) {
        String id = config.getId();
        if (elementConfigMap.containsKey(id)) {
          elementConfigMap.put(
              config.getId(),
              riskElementConfigUtils.mergeConfigs(config, elementConfigMap.get(id)));
        } else {
          throw Status.NOT_FOUND
              .withDescription(
                  String.format("Risk element id:%s NOT FOUND for factor id:%s", id, factorId))
              .asRuntimeException();
        }
      }
      return elementConfigMap.values();
    }
  }
}
