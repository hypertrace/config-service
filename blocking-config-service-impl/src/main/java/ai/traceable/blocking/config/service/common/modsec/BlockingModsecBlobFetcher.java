package ai.traceable.blocking.config.service.common.modsec;

import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.AnomalyEnvironmentScope;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleType;
import ai.traceable.anomaly.config.service.v1.modsec.AnomalyModsecConfigServiceGrpc.AnomalyModsecConfigServiceBlockingStub;
import ai.traceable.anomaly.config.service.v1.modsec.GetModsecCrsRulesRequest;
import ai.traceable.anomaly.config.service.v1.modsec.GetModsecCrsRulesResponse;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import com.google.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.hypertrace.config.objectstore.ClientConfig;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class BlockingModsecBlobFetcher {
  private static final AnomalyConfigScope CUSTOMER_CONFIG_SCOPE =
      AnomalyConfigScope.newBuilder()
          .setCustomerScope(AnomalyCustomerScope.getDefaultInstance())
          .build();
  private final AnomalyModsecConfigServiceBlockingStub configServiceBlockingStub;
  private final ClientConfig clientConfig;

  @Inject
  public BlockingModsecBlobFetcher(
      AnomalyModsecConfigServiceBlockingStub anomalyModsecConfigServiceBlockingStub,
      ClientConfig clientConfig) {
    this.configServiceBlockingStub = anomalyModsecConfigServiceBlockingStub;
    this.clientConfig = clientConfig;
  }

  public String getEnabledRulesBlob(
      RequestContext requestContext,
      ModsecRuleVersion modsecRuleVersion,
      Optional<String> environmentId) {
    GetModsecCrsRulesResponse response =
        requestContext.call(
            () ->
                configServiceBlockingStub
                    .withDeadlineAfter(clientConfig.getTimeout().toMillis(), TimeUnit.MILLISECONDS)
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
      throw (new RuntimeException("Unable to get modsec crs blocking rules"));
    }

    return blockingCrsRulesBlob;
  }
}
