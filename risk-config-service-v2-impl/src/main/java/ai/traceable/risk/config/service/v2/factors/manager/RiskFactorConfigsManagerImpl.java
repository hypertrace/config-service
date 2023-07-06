package ai.traceable.risk.config.service.v2.factors.manager;

import static java.util.function.Predicate.not;

import ai.traceable.risk.config.service.v2.RiskConfigIdGenerator;
import ai.traceable.risk.config.service.v2.RiskConfigScope;
import ai.traceable.risk.config.service.v2.RiskContributorConfigs;
import ai.traceable.risk.config.service.v2.RiskElementConfig;
import ai.traceable.risk.config.service.v2.RiskElementConfigUpdates;
import ai.traceable.risk.config.service.v2.RiskFactor;
import ai.traceable.risk.config.service.v2.RiskFactorCategory;
import ai.traceable.risk.config.service.v2.RiskFactorConfig;
import ai.traceable.risk.config.service.v2.RiskFactorConfigUpdateDetails;
import ai.traceable.risk.config.service.v2.factors.builder.RiskFactorConfigBuilder;
import ai.traceable.risk.config.service.v2.factors.builder.RiskFactorListBuilder;
import ai.traceable.risk.config.service.v2.factors.comparator.RiskFactorConfigsComparator;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.AllArgsConstructor;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class RiskFactorConfigsManagerImpl implements RiskFactorConfigsManager {

  private final IdentifiedObjectStore<RiskFactorConfig> factorConfigStore;
  private final RiskFactorListBuilder riskFactorListBuilder;
  private final RiskFactorConfigBuilder riskFactorConfigBuilder;
  private final RiskFactorConfigsComparator factorConfigsComparator;
  private final RiskContributorConfigs defaultRiskContributorConfigs;
  private final RiskConfigIdGenerator configIdGenerator;

  @Override
  public RiskContributorConfigs getRiskContributorConfigs(
      RequestContext requestContext, RiskConfigScope riskConfigScope) {
    Collection<RiskFactor> scopedDefaultRiskFactors =
        buildMergedGlobalAndDefaultRiskFactors(requestContext, riskConfigScope);
    return RiskContributorConfigs.newBuilder()
        .addAllRiskFactors(
            getRiskFactors(requestContext, scopedDefaultRiskFactors, riskConfigScope))
        .build();
  }

  @Override
  public RiskContributorConfigs updateRiskContributorConfigs(
      RequestContext requestContext,
      Collection<RiskFactorConfigUpdateDetails> riskFactorConfigUpdateDetailsList,
      RiskConfigScope riskConfigScope) {
    Collection<RiskFactor> scopedDefaultRiskFactors =
        buildMergedGlobalAndDefaultRiskFactors(requestContext, riskConfigScope);
    return RiskContributorConfigs.newBuilder()
        .addAllRiskFactors(
            updateRiskFactors(
                requestContext,
                riskFactorConfigUpdateDetailsList,
                scopedDefaultRiskFactors,
                riskConfigScope))
        .build();
  }

  @Override
  public RiskContributorConfigs resetRiskContributorConfigs(
      RequestContext requestContext,
      Collection<RiskFactorCategory> riskFactorCategoriesList,
      RiskConfigScope riskConfigScope) {
    Collection<RiskFactor> scopedDefaultRiskFactors =
        buildMergedGlobalAndDefaultRiskFactors(requestContext, riskConfigScope);
    return RiskContributorConfigs.newBuilder()
        .addAllRiskFactors(
            resetRiskFactors(
                requestContext,
                riskFactorCategoriesList,
                scopedDefaultRiskFactors,
                riskConfigScope))
        .build();
  }

  private Collection<RiskFactor> getRiskFactors(
      RequestContext requestContext,
      Collection<RiskFactor> defaultRiskFactors,
      RiskConfigScope riskConfigScope) {
    Collection<RiskFactorConfig> fetchedRiskFactorConfigs =
        defaultRiskFactors.stream()
            .map(RiskFactor::getRiskFactorConfig)
            .map(RiskFactorConfig::getRiskFactorCategory)
            .map(
                riskFactorCategory ->
                    getFactorConfigFromStore(requestContext, riskFactorCategory, riskConfigScope))
            .flatMap(Optional::stream)
            .collect(Collectors.toUnmodifiableList());
    return riskFactorListBuilder.mergeFactors(fetchedRiskFactorConfigs, defaultRiskFactors);
  }

  private Collection<RiskFactor> updateRiskFactors(
      RequestContext requestContext,
      Collection<RiskFactorConfigUpdateDetails> riskFactorConfigUpdateDetailsList,
      Collection<RiskFactor> defaultRiskFactorsList,
      RiskConfigScope riskConfigScope) {

    Map<RiskFactorCategory, RiskFactor> defaultListFactorsMap =
        defaultRiskFactorsList.stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    riskFactor -> riskFactor.getRiskFactorConfig().getRiskFactorCategory(),
                    Function.identity()));

    Collection<RiskFactorConfig> updateRiskFactorConfigs =
        buildUpdateConfigs(riskFactorConfigUpdateDetailsList, riskConfigScope);

    Collection<RiskFactorConfig> updatedRiskFactorConfigs =
        updateRiskFactorConfigs.stream()
            .map(
                updateConfig ->
                    updateEachFactorConfig(
                        requestContext, updateConfig, riskConfigScope, defaultListFactorsMap))
            .filter(not(RiskFactorConfig.getDefaultInstance()::equals))
            .collect(Collectors.toUnmodifiableList());
    return riskFactorListBuilder.mergeFactors(updatedRiskFactorConfigs, defaultRiskFactorsList);
  }

  private RiskFactorConfig buildScopedFactorConfig(
      RiskFactorConfig riskFactorConfig, RiskConfigScope riskConfigScope) {
    return RiskFactorConfig.newBuilder()
        .setRiskFactorCategory(riskFactorConfig.getRiskFactorCategory())
        .setRiskConfigScope(riskConfigScope)
        .setDisabled(riskFactorConfig.getDisabled())
        .addAllRiskElementConfigs(riskFactorConfig.getRiskElementConfigsList())
        .build();
  }

  private Collection<RiskFactorConfig> buildUpdateConfigs(
      Collection<RiskFactorConfigUpdateDetails> riskFactorConfigUpdateDetailsList,
      RiskConfigScope riskConfigScope) {
    return riskFactorConfigUpdateDetailsList.stream()
        .map(
            riskFactorConfigUpdateDetails ->
                buildEachUpdateConfig(riskFactorConfigUpdateDetails, riskConfigScope))
        .collect(Collectors.toUnmodifiableList());
  }

  private RiskFactorConfig buildEachUpdateConfig(
      RiskFactorConfigUpdateDetails riskFactorConfigUpdateDetails,
      RiskConfigScope riskConfigScope) {
    return RiskFactorConfig.newBuilder()
        .setRiskConfigScope(riskConfigScope)
        .setRiskFactorCategory(riskFactorConfigUpdateDetails.getRiskFactorCategory())
        .setDisabled(riskFactorConfigUpdateDetails.getDisabled())
        .addAllRiskElementConfigs(
            buildElementConfigsFromUpdateDetails(
                riskFactorConfigUpdateDetails.getRiskElementConfigUpdates()))
        .build();
  }

  private Collection<RiskFactor> resetRiskFactors(
      RequestContext requestContext,
      Collection<RiskFactorCategory> riskFactorCategoriesList,
      Collection<RiskFactor> defaultRiskFactorsList,
      RiskConfigScope riskConfigScope) {
    riskFactorCategoriesList.forEach(
        riskFactorCategory ->
            deleteEachRiskFactorConfig(requestContext, riskFactorCategory, riskConfigScope));
    return getRiskFactors(requestContext, defaultRiskFactorsList, riskConfigScope);
  }

  private void deleteEachRiskFactorConfig(
      RequestContext requestContext,
      RiskFactorCategory riskFactorCategory,
      RiskConfigScope riskConfigScope) {
    factorConfigStore.deleteObject(
        requestContext, configIdGenerator.generateId(riskFactorCategory.name(), riskConfigScope));
  }

  private Collection<RiskElementConfig> buildElementConfigsFromUpdateDetails(
      RiskElementConfigUpdates riskElementConfigUpdates) {
    return riskElementConfigUpdates.getRiskElementConfigUpdateDetailsList().stream()
        .map(
            riskElementConfigUpdateDetails ->
                RiskElementConfig.newBuilder()
                    .setRiskElementScoring(riskElementConfigUpdateDetails.getRiskElementScoring())
                    .setId(riskElementConfigUpdateDetails.getId())
                    .setDisabled(riskElementConfigUpdateDetails.getDisabled())
                    .build())
        .collect(Collectors.toUnmodifiableList());
  }

  public Collection<RiskFactor> buildMergedGlobalAndDefaultRiskFactors(
      RequestContext requestContext, RiskConfigScope riskConfigScope) {
    Collection<RiskFactor> globalRiskFactors =
        getRiskFactors(
            requestContext,
            defaultRiskContributorConfigs.getRiskFactorsList(),
            RiskConfigScope.getDefaultInstance());
    Collection<RiskFactor> mergedGlobalAndDefaultFactors =
        riskFactorListBuilder.mergeDefaultFactors(
            globalRiskFactors, defaultRiskContributorConfigs.getRiskFactorsList());
    return mergedGlobalAndDefaultFactors.stream()
        .map(
            riskFactor ->
                RiskFactor.newBuilder()
                    .setRiskFactorConfig(
                        buildScopedFactorConfig(riskFactor.getRiskFactorConfig(), riskConfigScope))
                    .setRiskFactorInfo(riskFactor.getRiskFactorInfo())
                    .build())
        .collect(Collectors.toUnmodifiableList());
  }

  Optional<RiskFactorConfig> getFactorConfigFromStore(
      RequestContext requestContext,
      RiskFactorCategory riskFactorCategory,
      RiskConfigScope riskConfigScope) {
    String id = configIdGenerator.generateId(riskFactorCategory.name(), riskConfigScope);
    return factorConfigStore.getData(requestContext, id);
  }

  private RiskFactorConfig updateEachFactorConfig(
      RequestContext requestContext,
      RiskFactorConfig updateConfig,
      RiskConfigScope riskConfigScope,
      Map<RiskFactorCategory, RiskFactor> defaultListFactorsMap) {
    Optional<RiskFactorConfig> fetchedRiskFactorConfig =
        getFactorConfigFromStore(
            requestContext, updateConfig.getRiskFactorCategory(), riskConfigScope);

    RiskFactorConfig scopedDefaultConfig =
        defaultListFactorsMap.get(updateConfig.getRiskFactorCategory()).getRiskFactorConfig();
    if (fetchedRiskFactorConfig.isPresent()) {
      updateConfig =
          riskFactorConfigBuilder.mergeConfigs(updateConfig, fetchedRiskFactorConfig.get());
    }

    if (factorConfigsComparator.isFactorConfigEqual(updateConfig, scopedDefaultConfig)) {
      deleteEachRiskFactorConfig(
          requestContext, updateConfig.getRiskFactorCategory(), riskConfigScope);
      return updateConfig;
    }

    return factorConfigStore.upsertObject(requestContext, updateConfig).getData();
  }
}
