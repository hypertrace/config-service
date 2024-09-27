package ai.traceable.localprocessing.config.service.regularmodsec;

import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.AnomalyEnvironmentScope;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleType;
import ai.traceable.anomaly.config.service.v1.modsec.AnomalyModsecConfigServiceGrpc.AnomalyModsecConfigServiceBlockingStub;
import ai.traceable.anomaly.config.service.v1.modsec.GetModsecCrsRulesRequest;
import ai.traceable.anomaly.config.service.v1.modsec.GetModsecCrsRulesResponse;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecCrsRulesTarget;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import ai.traceable.localprocessing.config.service.utils.UuidGenerator;
import ai.traceable.localprocessing.config.service.v1.RegularModsecDetectionRules;
import com.google.inject.Inject;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ClientConfig;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class DefaultRegularModsecDetectionManager implements RegularModsecDetectionManager {
  private static final AnomalyConfigScope CUSTOMER_CONFIG_SCOPE =
      AnomalyConfigScope.newBuilder()
          .setCustomerScope(AnomalyCustomerScope.getDefaultInstance())
          .build();
  private static final RegularModsecDetectionRules EMPTY_MODSEC_RULES =
      RegularModsecDetectionRules.newBuilder()
          .setRegularModsecDetectionRulesBlob("")
          .setHash(UuidGenerator.EMPTY_STRING_UUID)
          .build();

  private final AnomalyModsecConfigServiceBlockingStub configServiceBlockingStub;
  private final UuidGenerator uuidGenerator;
  private final ClientConfig clientConfig;

  @Inject
  public DefaultRegularModsecDetectionManager(
      AnomalyModsecConfigServiceBlockingStub anomalyModsecConfigServiceBlockingStub,
      UuidGenerator uuidGenerator,
      ClientConfig clientConfig) {
    this.configServiceBlockingStub = anomalyModsecConfigServiceBlockingStub;
    this.uuidGenerator = uuidGenerator;
    this.clientConfig = clientConfig;
  }

  @Override
  public RegularModsecDetectionRules getDetectionRules(
      RequestContext requestContext,
      String requestHash,
      boolean shouldUseCoraza,
      boolean shouldHideMatchValueInCrsMsg,
      String environmentId) {
    // https://traceableai.atlassian.net/browse/ENG-15496
    // Only Safe CRS rules will be evaluated on sensitive params on Traceable Platform Agent due
    // to perf constraints
    ModsecRuleVersion version;
    if (shouldHideMatchValueInCrsMsg) {
      version =
          shouldUseCoraza
              ? ModsecRuleVersion.MODSEC_RULE_VERSION_SENSITIVE_AGENT_CORAZA_V3
              : ModsecRuleVersion.MODSEC_RULE_VERSION_SENSITIVE_AGENT_V3;
    } else {
      version =
          shouldUseCoraza
              ? ModsecRuleVersion.MODSEC_RULE_VERSION_CORAZA_V3
              : ModsecRuleVersion.MODSEC_RULE_VERSION_UNSPECIFIED;
    }

    GetModsecCrsRulesResponse response =
        requestContext.call(
            () ->
                configServiceBlockingStub
                    .withDeadlineAfter(clientConfig.getTimeout().toMillis(), TimeUnit.MILLISECONDS)
                    .getModsecCrsRules(
                        GetModsecCrsRulesRequest.newBuilder()
                            .setTarget(ModsecCrsRulesTarget.MODSEC_CRS_RULES_TARGET_TPA_DETECTION)
                            .addAllSubRuleTypes(
                                List.of(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE))
                            .setRemoveDisabledRules(true)
                            .setRuleVersion(version)
                            .setConfigScope(
                                environmentId.isBlank()
                                    ? CUSTOMER_CONFIG_SCOPE
                                    : AnomalyConfigScope.newBuilder()
                                        .setEnvironmentScope(
                                            AnomalyEnvironmentScope.newBuilder()
                                                .setEnvironmentId(environmentId))
                                        .build())
                            .build()));

    if (response.getAggregatedModsecCrsRulesBlob().isEmpty()) {
      log.debug(
          "Empty modsec crs blob returned for local processing for requestContext : {}",
          requestContext);
      ;
    }

    String regularCrsRulesBlob = response.getAggregatedModsecCrsRulesBlob();

    String responseHash = uuidGenerator.generateId(regularCrsRulesBlob);

    RegularModsecDetectionRules.Builder builder =
        RegularModsecDetectionRules.newBuilder().setHash(responseHash);
    if (!responseHash.equals(requestHash)) {
      builder.setRegularModsecDetectionRulesBlob(regularCrsRulesBlob);
    }
    return builder.build();
  }

  @Override
  public RegularModsecDetectionRules getEmptyRules() {
    return EMPTY_MODSEC_RULES;
  }
}
