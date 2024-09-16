package ai.traceable.blocking.config.service.common.modsec;

import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.AnomalyEnvironmentScope;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleType;
import ai.traceable.anomaly.config.service.v1.modsec.AnomalyModsecConfigServiceGrpc.AnomalyModsecConfigServiceBlockingStub;
import ai.traceable.anomaly.config.service.v1.modsec.GetModsecCrsRulesRequest;
import ai.traceable.anomaly.config.service.v1.modsec.GetModsecCrsRulesResponse;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecCrsRulesData;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecCrsRulesTarget;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ClientConfig;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class BlockingModsecBlobFetcher {
  private static final String ENABLE_NEW_MODSEC_CRS_BLOCKING_FLOW =
      "enableNewModsecCrsBlockingFlow";
  private static final AnomalyConfigScope CUSTOMER_CONFIG_SCOPE =
      AnomalyConfigScope.newBuilder()
          .setCustomerScope(AnomalyCustomerScope.getDefaultInstance())
          .build();
  private final AnomalyModsecConfigServiceBlockingStub configServiceBlockingStub;
  private final ClientConfig clientConfig;
  private final boolean enableNewModsecCrsBlockingFlow;

  @Inject
  public BlockingModsecBlobFetcher(
      AnomalyModsecConfigServiceBlockingStub anomalyModsecConfigServiceBlockingStub,
      ClientConfig clientConfig,
      Config config) {
    this.configServiceBlockingStub = anomalyModsecConfigServiceBlockingStub;
    this.clientConfig = clientConfig;
    this.enableNewModsecCrsBlockingFlow =
        config.hasPath(ENABLE_NEW_MODSEC_CRS_BLOCKING_FLOW)
            && config.getBoolean(ENABLE_NEW_MODSEC_CRS_BLOCKING_FLOW);
  }

  public String getEnabledRulesBlob(
      RequestContext requestContext,
      ModsecRuleVersion modsecRuleVersion,
      Optional<String> environmentId) {
    if (enableNewModsecCrsBlockingFlow) {
      GetModsecCrsRulesResponse response =
          requestContext.call(
              () ->
                  configServiceBlockingStub
                      .withDeadlineAfter(
                          clientConfig.getTimeout().toMillis(), TimeUnit.MILLISECONDS)
                      .getModsecCrsRules(
                          GetModsecCrsRulesRequest.newBuilder()
                              .setTarget(ModsecCrsRulesTarget.MODSEC_CRS_RULES_TARGET_TA_BLOCKING)
                              .setRuleVersion(modsecRuleVersion)
                              .setRemoveDisabledRules(true)
                              .setConfigScope(
                                  environmentId
                                      .map(
                                          id ->
                                              AnomalyConfigScope.newBuilder()
                                                  .setEnvironmentScope(
                                                      AnomalyEnvironmentScope.newBuilder()
                                                          .setEnvironmentId(id))
                                                  .build())
                                      .orElse(CUSTOMER_CONFIG_SCOPE))
                              .build()));
      if (response.getAggregatedModsecCrsRulesBlob().isEmpty()) {
        log.debug(
            "Empty modsec crs blob returned for blocking for requestContext : {}", requestContext);
      }

      return response.getAggregatedModsecCrsRulesBlob();
    } else {
      // TODO : Deprecate this old flow
      GetModsecCrsRulesResponse response =
          requestContext.call(
              () ->
                  configServiceBlockingStub
                      .withDeadlineAfter(
                          clientConfig.getTimeout().toMillis(), TimeUnit.MILLISECONDS)
                      .getModsecCrsRules(
                          GetModsecCrsRulesRequest.newBuilder()
                              .addAllSubRuleTypes(
                                  List.of(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK))
                              .setRuleVersion(modsecRuleVersion)
                              .setRemoveDisabledRules(true)
                              .setConfigScope(
                                  environmentId
                                      .map(
                                          id ->
                                              AnomalyConfigScope.newBuilder()
                                                  .setEnvironmentScope(
                                                      AnomalyEnvironmentScope.newBuilder()
                                                          .setEnvironmentId(id))
                                                  .build())
                                      .orElse(CUSTOMER_CONFIG_SCOPE))
                              .build()));
      String blockingCrsRulesBlob;
      if (response.getModsecCrsRulesList().size() == 1
          && response.getModsecCrsRulesList().get(0).getSubRuleType()
              == AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK) {
        // The request was done for only one modsec type, so we should be getting only 1 element
        blockingCrsRulesBlob = response.getModsecCrsRulesList().get(0).getModsecCrsRulesBlob();
      } else {
        throw (new RuntimeException(
            String.format(
                "Error in fetching modsec crs rules - received subRuleTypes: %s",
                response.getModsecCrsRulesList().stream()
                    .map(ModsecCrsRulesData::getSubRuleType)
                    .map(AnomalySubRuleType::name)
                    .collect(Collectors.joining()))));
      }

      return blockingCrsRulesBlob;
    }
  }
}
