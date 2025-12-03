package ai.traceable.edge.config.service.supplier;

import ai.traceable.anomaly.config.service.v1.AnomalySubRuleType;
import ai.traceable.anomaly.config.service.v1.RuleEvaluationPoint;
import ai.traceable.anomaly.config.service.v1.modsec.AnomalyModsecConfigServiceGrpc;
import ai.traceable.anomaly.config.service.v1.modsec.GetWebAppEvaluationConfigContextRequest;
import ai.traceable.anomaly.config.service.v1.modsec.GetWebAppEvaluationConfigContextResponse;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.edge.config.service.TraceableEdgeConfigSupplier;
import ai.traceable.edge.config.service.config.TraceableEdgeConfig;
import ai.traceable.edge.config.service.v1.AgentCapabilities;
import ai.traceable.edge.config.service.v1.ConfigPayloads;
import ai.traceable.edge.config.service.v1.ConfigRequestElement;
import ai.traceable.edge.config.service.v1.ConfigResponseElement;
import ai.traceable.protection.engine.config.webapp.v1.WebAppEvaluationConfigContext;
import com.google.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = {@Inject})
public class WebAppEvaluationConfigContextSupplier implements TraceableEdgeConfigSupplier {
  private static final String CONFIG_TYPE = WebAppEvaluationConfigContext.class.getSimpleName();
  private static final List<AnomalySubRuleType> STANDARD_RULE_TYPES =
      List.of(
          AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE,
          AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK);
  private static final List<AnomalySubRuleType> AGGRESSIVE_RULE_TYPES =
      List.of(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_REGULAR);
  private final AnomalyModsecConfigServiceGrpc.AnomalyModsecConfigServiceBlockingStub stub;
  private final TraceableEdgeConfig config;
  private final UuidGenerator uuidGenerator;

  @Override
  public String getConfigType() {
    return CONFIG_TYPE;
  }

  @Override
  public ConfigResponseElement getConfigs(
      RequestContext requestContext,
      String environment,
      ConfigRequestElement requestElement,
      AgentCapabilities agentCapabilities) {
    log.debug(
        "Received request for WebAppEvaluationConfigContext for tenantId: {}",
        requestContext.getTenantId());
    GetWebAppEvaluationConfigContextResponse response =
        getWebAppEvaluationConfigContext(requestContext, requestElement);
    ConfigPayloads configPayloads =
        ConfigPayloads.newBuilder()
            .addConfigBytes(response.getWebAppEvaluationConfigContext())
            .build();
    log.debug(
        "Returning WebAppEvaluationConfigContext for tenantId: {}", requestContext.getTenantId());
    return ConfigResponseElement.newBuilder()
        .setConfigType(getConfigType())
        .setEnabled(true)
        .addSupportedAgentCapabilities(agentCapabilities)
        .setRefreshAfterDuration(config.getAgentPollingFrequency(getConfigType()))
        .setConfigPayloads(configPayloads)
        .setHash(uuidGenerator.generateId(configPayloads))
        .build();
  }

  private GetWebAppEvaluationConfigContextResponse getWebAppEvaluationConfigContext(
      RequestContext requestContext, ConfigRequestElement requestElement) {
    GetWebAppEvaluationConfigContextRequest.Builder requestBuilder =
        GetWebAppEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE);
    Optional.ofNullable(
            requestElement
                .getAgentCapabilities()
                .getAdditionalFieldsMap()
                .get("webAppThreatRuleType"))
        .ifPresent(
            ruleType -> {
              switch (ruleType) {
                case "WEB_APP_THREAT_RULE_TYPE_STANDARD":
                  requestBuilder.addAllSubRuleTypes(STANDARD_RULE_TYPES);
                  break;
                case "WEB_APP_THREAT_RULE_TYPE_AGGRESSIVE":
                  requestBuilder.addAllSubRuleTypes(AGGRESSIVE_RULE_TYPES);
                  break;
                default:
                  log.error("Unknown rule type: {}", ruleType);
              }
            });
    return requestContext.call(
        () ->
            stub.withDeadlineAfter(
                    config.getClientConfig().getTimeout().toMillis(), TimeUnit.MILLISECONDS)
                .getWebAppEvaluationConfigContext(requestBuilder.build()));
  }
}
