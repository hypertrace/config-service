package ai.traceable.risk.config.service.v2.factors.builder;

import ai.traceable.risk.config.service.v2.RiskConfigBuilder;
import ai.traceable.risk.config.service.v2.RiskFactor;
import ai.traceable.risk.config.service.v2.RiskFactorCategory;
import ai.traceable.risk.config.service.v2.RiskFactorConfig;
import ai.traceable.risk.config.service.v2.RiskFactorInfo;
import ai.traceable.risk.config.service.v2.factors.comparator.RiskFactorConfigsComparator;
import io.grpc.Status;
import jakarta.inject.Inject;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class RiskFactorListBuilder {

  private final RiskConfigBuilder<RiskFactorConfig> factorConfigBuilder;
  private final RiskFactorConfigsComparator factorConfigsComparator;

  public Collection<RiskFactor> mergeDefaultFactors(
      Collection<RiskFactor> factorList, Collection<RiskFactor> defaultRiskFactors) {

    Map<RiskFactorCategory, RiskFactor> defaultFactorMap =
        defaultRiskFactors.stream()
            .collect(
                Collectors.toMap(
                    config -> config.getRiskFactorConfig().getRiskFactorCategory(),
                    Function.identity()));

    factorList.forEach(
        factor ->
            defaultFactorMap.put(factor.getRiskFactorConfig().getRiskFactorCategory(), factor));

    return defaultFactorMap.values();
  }

  public Collection<RiskFactor> mergeFactors(
      Collection<RiskFactorConfig> fetchedRiskFactorConfigs,
      Collection<RiskFactor> defaultRiskFactors) {

    Map<RiskFactorCategory, RiskFactor> defaultFactorsMap =
        defaultRiskFactors.stream()
            .collect(
                Collectors.toMap(
                    riskFactor -> riskFactor.getRiskFactorConfig().getRiskFactorCategory(),
                    Function.identity()));

    for (RiskFactorConfig riskFactorConfig : fetchedRiskFactorConfigs) {
      RiskFactorCategory riskFactorCategory = riskFactorConfig.getRiskFactorCategory();
      if (defaultFactorsMap.containsKey(riskFactorCategory)) {
        defaultFactorsMap.put(
            riskFactorCategory,
            mergeEachFactor(riskFactorConfig, defaultFactorsMap.get(riskFactorCategory)));
      } else {
        throw Status.NOT_FOUND
            .withDescription(String.format("Risk factor category:%s NOT FOUND", riskFactorCategory))
            .asRuntimeException();
      }
    }

    return defaultFactorsMap.values();
  }

  private RiskFactor mergeEachFactor(
      RiskFactorConfig fetchedRiskFactorConfig, RiskFactor defaultRiskFactor) {

    RiskFactorConfig mergedRiskFactorConfig =
        factorConfigBuilder.mergeConfigs(
            fetchedRiskFactorConfig, defaultRiskFactor.getRiskFactorConfig());

    return RiskFactor.newBuilder()
        .setRiskFactorConfig(mergedRiskFactorConfig)
        .setRiskFactorInfo(
            RiskFactorInfo.newBuilder()
                .setRiskContributorCategory(
                    defaultRiskFactor.getRiskFactorInfo().getRiskContributorCategory())
                .setIsDefault(
                    factorConfigsComparator.isFactorConfigEqual(
                        mergedRiskFactorConfig, defaultRiskFactor.getRiskFactorConfig())))
        .build();
  }
}
