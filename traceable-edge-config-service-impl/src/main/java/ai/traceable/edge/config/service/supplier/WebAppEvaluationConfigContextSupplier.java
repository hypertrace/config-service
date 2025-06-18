package ai.traceable.edge.config.service.supplier;

import ai.traceable.anomaly.config.service.v1.modsec.AnomalyModsecConfigServiceGrpc;
import ai.traceable.anomaly.config.service.v1.modsec.GetWebAppEvaluationConfigContextRequest;
import ai.traceable.anomaly.config.service.v1.modsec.GetWebAppEvaluationConfigContextResponse;
import ai.traceable.anomaly.config.service.v1.modsec.RuleEvaluationPoint;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.edge.config.service.TraceableEdgeConfigSupplier;
import ai.traceable.edge.config.service.config.TraceableEdgeConfig;
import ai.traceable.edge.config.service.v1.AgentCapabilities;
import ai.traceable.edge.config.service.v1.ConfigPayloads;
import ai.traceable.edge.config.service.v1.ConfigRequestElement;
import ai.traceable.edge.config.service.v1.ConfigResponseElement;
import ai.traceable.protection.engine.config.webapp.v1.WebAppEvaluationConfigContext;
import com.google.inject.Inject;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class WebAppEvaluationConfigContextSupplier implements TraceableEdgeConfigSupplier {
  private static final String CONFIG_TYPE = WebAppEvaluationConfigContext.class.getSimpleName();
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
      ConfigRequestElement requestElement,
      AgentCapabilities agentCapabilities) {
    GetWebAppEvaluationConfigContextResponse response =
        getWebAppEvaluationConfigContext(requestContext);
    ConfigPayloads configPayloads =
        ConfigPayloads.newBuilder()
            .addConfigBytes(response.getWebAppEvaluationConfigContext())
            .build();
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
      RequestContext requestContext) {
    return requestContext.call(
        () ->
            stub.withDeadlineAfter(
                    config.getClientConfig().getTimeout().toMillis(), TimeUnit.MILLISECONDS)
                .getWebAppEvaluationConfigContext(
                    GetWebAppEvaluationConfigContextRequest.newBuilder()
                        .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
                        .build()));
  }
}
