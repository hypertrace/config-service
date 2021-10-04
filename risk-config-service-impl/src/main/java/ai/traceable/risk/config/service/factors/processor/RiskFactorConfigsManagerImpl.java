package ai.traceable.risk.config.service.factors.processor;

import ai.traceable.risk.config.service.factors.RiskFactorConfigsManager;
import ai.traceable.risk.config.service.factors.processor.utils.RiskFactorListUtils;
import ai.traceable.risk.config.service.v1.CustomizationOptions;
import ai.traceable.risk.config.service.v1.RiskContributorConfigs;
import ai.traceable.risk.config.service.v1.RiskContributorConfigsResetFilter;
import ai.traceable.risk.config.service.v1.RiskElementConfig;
import ai.traceable.risk.config.service.v1.RiskFactor;
import ai.traceable.risk.config.service.v1.RiskFactorConfig;
import io.grpc.Status;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Named;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class RiskFactorConfigsManagerImpl implements RiskFactorConfigsManager {

  private final IdentifiedObjectStore<RiskFactorConfig> factorConfigStore;
  private final IdentifiedObjectStore<RiskElementConfig> elementConfigStore;
  private final RiskFactorListUtils riskFactorListUtils;
  private final RiskContributorConfigs defaultRiskLikelihoodConfigs;
  private final RiskContributorConfigs defaultRiskImpactConfigs;

  @Inject
  public RiskFactorConfigsManagerImpl(
      IdentifiedObjectStore<RiskFactorConfig> factorConfigStore,
      IdentifiedObjectStore<RiskElementConfig> elementConfigStore,
      RiskFactorListUtils riskFactorListUtils,
      @Named(LIKELIHOOD_ANNOTATION) RiskContributorConfigs defaultRiskLikelihoodConfigs,
      @Named(IMPACT_ANNOTATION) RiskContributorConfigs defaultRiskImpactConfigs) {
    this.factorConfigStore = factorConfigStore;
    this.elementConfigStore = elementConfigStore;
    this.riskFactorListUtils = riskFactorListUtils;
    this.defaultRiskLikelihoodConfigs = defaultRiskLikelihoodConfigs;
    this.defaultRiskImpactConfigs = defaultRiskImpactConfigs;
  }

  @Override
  public RiskContributorConfigs getRiskLikelihoodConfigs(RequestContext requestContext) {
    return RiskContributorConfigs.newBuilder()
        .addAllRiskFactors(
            getRiskFactors(requestContext, defaultRiskLikelihoodConfigs.getRiskFactorsList()))
        .build();
  }

  @Override
  public RiskContributorConfigs updateRiskLikelihoodConfigs(
      RequestContext requestContext, List<RiskFactorConfig> riskFactorConfigs) {
    return RiskContributorConfigs.newBuilder()
        .addAllRiskFactors(
            updateRiskFactors(
                requestContext,
                riskFactorConfigs,
                defaultRiskLikelihoodConfigs.getRiskFactorsList()))
        .build();
  }

  @Override
  public RiskContributorConfigs resetRiskLikelihoodConfigs(
      RequestContext requestContext, RiskContributorConfigsResetFilter filter) {
    deleteFactorElementConfigs(
        requestContext, defaultRiskLikelihoodConfigs.getRiskFactorsList(), filter);
    return getRiskLikelihoodConfigs(requestContext);
  }

  @Override
  public RiskContributorConfigs getRiskImpactConfigs(RequestContext requestContext) {
    return RiskContributorConfigs.newBuilder()
        .addAllRiskFactors(
            getRiskFactors(requestContext, defaultRiskImpactConfigs.getRiskFactorsList()))
        .build();
  }

  @Override
  public RiskContributorConfigs updateRiskImpactConfigs(
      RequestContext requestContext, List<RiskFactorConfig> riskFactorConfigs) {
    return RiskContributorConfigs.newBuilder()
        .addAllRiskFactors(
            updateRiskFactors(
                requestContext, riskFactorConfigs, defaultRiskImpactConfigs.getRiskFactorsList()))
        .build();
  }

  @Override
  public RiskContributorConfigs resetRiskImpactConfigs(
      RequestContext requestContext, RiskContributorConfigsResetFilter filter) {
    deleteFactorElementConfigs(
        requestContext, defaultRiskImpactConfigs.getRiskFactorsList(), filter);
    return getRiskImpactConfigs(requestContext);
  }

  private Collection<RiskFactor> getRiskFactors(
      RequestContext requestContext, List<RiskFactor> defaultRiskFactors) {
    List<RiskFactorConfig> factorConfigs = factorConfigStore.getAllObjects(requestContext);
    List<RiskElementConfig> elementConfigs = elementConfigStore.getAllObjects(requestContext);
    return riskFactorListUtils.mergeFactorConfigs(
        factorConfigs, elementConfigs, defaultRiskFactors, false);
  }

  private Collection<RiskFactor> updateRiskFactors(
      RequestContext requestContext,
      List<RiskFactorConfig> riskFactorConfigs,
      List<RiskFactor> defaultFactors) {
    Collection<RiskFactor> mergedRiskFactors =
        riskFactorListUtils.mergeFactorConfigs(
            riskFactorConfigs, Collections.emptyList(), defaultFactors, true);

    Status validationStatus =
        riskFactorListUtils.validateFactorConfigs(
            mergedRiskFactors.stream()
                .map(RiskFactor::getRiskFactorConfig)
                .collect(Collectors.toList()));
    if (!validationStatus.isOk()) {
      throw validationStatus.asRuntimeException();
    }

    for (RiskFactor factor : mergedRiskFactors) {
      if (!factor.getIsDefault()) {
        RiskFactorConfig factorConfig = factor.getRiskFactorConfig();
        if (factor
            .getCustomizationOptionsList()
            .contains(CustomizationOptions.CUSTOMIZATION_OPTIONS_ELEMENT_ADD_DELETE)) {
          factorConfigStore.upsertObject(requestContext, factorConfig);
        } else {
          // risk element associations with the factors cannot be modified by user
          // their configs are stored separately..
          factorConfigStore.upsertObject(
              requestContext,
              RiskFactorConfig.newBuilder()
                  .setId(factorConfig.getId())
                  .setRiskFactorScoring(factorConfig.getRiskFactorScoring())
                  .build());
          factorConfig
              .getRiskElementConfigsList()
              .forEach(
                  elementConfig -> elementConfigStore.upsertObject(requestContext, elementConfig));
        }
      }
    }
    return mergedRiskFactors;
  }

  private void deleteFactorElementConfigs(
      RequestContext requestContext,
      List<RiskFactor> riskFactors,
      RiskContributorConfigsResetFilter filter) {
    if (filter.equals(RiskContributorConfigsResetFilter.getDefaultInstance())) {
      deleteFactorElementConfigs(requestContext, riskFactors);
    } else if (!filter.getRiskFactorIdsList().isEmpty()) {
      List<String> riskFactorIds = filter.getRiskFactorIdsList();
      deleteFactorElementConfigs(
          requestContext,
          riskFactors.stream()
              .filter(
                  riskFactor -> riskFactorIds.contains(riskFactor.getRiskFactorConfig().getId()))
              .collect(Collectors.toList()));
    }
  }

  private void deleteFactorElementConfigs(
      RequestContext requestContext, List<RiskFactor> riskFactors) {
    riskFactors.forEach(
        factor -> {
          try {
            factorConfigStore.deleteObject(requestContext, factor.getRiskFactorConfig().getId());
          } catch (Exception e) {
            if (!Status.fromThrowable(e).equals(Status.NOT_FOUND)) {
              throw e;
            }
          }
          factor
              .getRiskFactorConfig()
              .getRiskElementConfigsList()
              .forEach(
                  element -> {
                    try {
                      elementConfigStore.deleteObject(requestContext, element.getId());
                    } catch (Exception e) {
                      if (!Status.fromThrowable(e).equals(Status.NOT_FOUND)) {
                        throw e;
                      }
                    }
                  });
        });
  }
}
