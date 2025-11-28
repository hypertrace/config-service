package ai.traceable.edge.config.service.supplier;

import ai.traceable.anomaly.config.service.v1.RuleEvaluationPoint;
import ai.traceable.anomaly.config.service.v1.apiprotect.AnomalyApiProtectConfigServiceGrpc;
import ai.traceable.anomaly.config.service.v1.apiprotect.GetApiProtectEvaluationConfigContextRequest;
import ai.traceable.anomaly.config.service.v1.apiprotect.GetApiProtectEvaluationConfigContextResponse;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.edge.config.service.TraceableEdgeConfigSupplier;
import ai.traceable.edge.config.service.config.TraceableEdgeConfig;
import ai.traceable.edge.config.service.v1.AgentCapabilities;
import ai.traceable.edge.config.service.v1.ConfigPayloads;
import ai.traceable.edge.config.service.v1.ConfigRequestElement;
import ai.traceable.edge.config.service.v1.ConfigResponseElement;
import ai.traceable.protection.engine.config.apiprotect.v1.ApiProtectionConfigContext;
import com.google.inject.Inject;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = {@Inject})
public class ApiProtectEvaluationConfigContextSupplier implements TraceableEdgeConfigSupplier {
  private static final String CONFIG_TYPE = ApiProtectionConfigContext.class.getSimpleName();
  private final AnomalyApiProtectConfigServiceGrpc.AnomalyApiProtectConfigServiceBlockingStub stub;
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
        "Received request for ApiProtectEvaluationConfigContext for tenantId: {}",
        requestContext.getTenantId());
    GetApiProtectEvaluationConfigContextResponse response =
        getApiProtectEvaluationConfigContext(requestContext);
    ConfigPayloads configPayloads =
        ConfigPayloads.newBuilder()
            .addConfigBytes(response.getApiProtectEvaluationConfigContext())
            .build();
    log.debug(
        "Returning ApiProtectEvaluationConfigContext for tenantId: {}",
        requestContext.getTenantId());
    return ConfigResponseElement.newBuilder()
        .setConfigType(getConfigType())
        .setEnabled(true)
        .addSupportedAgentCapabilities(agentCapabilities)
        .setRefreshAfterDuration(config.getAgentPollingFrequency(getConfigType()))
        .setConfigPayloads(configPayloads)
        .setHash(uuidGenerator.generateId(configPayloads))
        .build();
  }

  private GetApiProtectEvaluationConfigContextResponse getApiProtectEvaluationConfigContext(
      RequestContext requestContext) {
    GetApiProtectEvaluationConfigContextRequest.Builder requestBuilder =
        GetApiProtectEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE);
    return requestContext.call(
        () ->
            stub.withDeadlineAfter(
                    config.getClientConfig().getTimeout().toMillis(), TimeUnit.MILLISECONDS)
                .getApiProtectEvaluationConfigContext(requestBuilder.build()));
  }
}
