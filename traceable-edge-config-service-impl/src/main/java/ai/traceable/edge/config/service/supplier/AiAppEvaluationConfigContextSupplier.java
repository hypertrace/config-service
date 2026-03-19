package ai.traceable.edge.config.service.supplier;

import ai.traceable.aiapp.protection.config.service.v1.AiAppConfigServiceGrpc;
import ai.traceable.aiapp.protection.config.service.v1.GetAiAppEvaluationConfigContextRequest;
import ai.traceable.aiapp.protection.config.service.v1.GetAiAppEvaluationConfigContextResponse;
import ai.traceable.aiapp.protection.config.service.v1.RuleEvaluationPoint;
import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.edge.config.service.AbstractTraceableEdgeConfigSupplier;
import ai.traceable.edge.config.service.config.TraceableEdgeConfig;
import ai.traceable.edge.config.service.v1.AgentCapabilities;
import ai.traceable.edge.config.service.v1.ConfigPayloads;
import ai.traceable.edge.config.service.v1.ConfigRequestElement;
import ai.traceable.edge.config.service.v1.ConfigResponseElement;
import ai.traceable.protection.engine.config.aifirewall.v1.AiFirewallConfigContext;
import com.google.inject.Inject;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class AiAppEvaluationConfigContextSupplier extends AbstractTraceableEdgeConfigSupplier {
  private static final String CONFIG_TYPE = AiFirewallConfigContext.class.getSimpleName();
  private final AiAppConfigServiceGrpc.AiAppConfigServiceBlockingStub stub;
  private final FeatureCachingClient featureCachingClient;

  @Inject
  public AiAppEvaluationConfigContextSupplier(
      UuidGenerator uuidGenerator,
      TraceableEdgeConfig config,
      AiAppConfigServiceGrpc.AiAppConfigServiceBlockingStub stub,
      FeatureCachingClient featureCachingClient) {
    super(uuidGenerator, config);
    this.stub = stub;
    this.featureCachingClient = featureCachingClient;
  }

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
        "Received request for AiAppEvaluationConfigContext for tenantId: {}",
        requestContext.getTenantId());
    if (!featureCachingClient.isProtectionEngineAiAppProtectionEnabledForTenant(requestContext)) {
      log.debug("AI App protection not enabled for tenant: {}", requestContext.getTenantId());
      ConfigPayloads emptyPayloads = ConfigPayloads.getDefaultInstance();
      return buildConfigResponseElement(emptyPayloads, agentCapabilities);
    }
    GetAiAppEvaluationConfigContextResponse response =
        getAiAppEvaluationConfigContext(requestContext);
    ConfigPayloads configPayloads =
        ConfigPayloads.newBuilder()
            .addConfigBytes(response.getAiAppEvaluationConfigContext())
            .build();
    log.debug(
        "Returning AiAppEvaluationConfigContext for tenantId: {}", requestContext.getTenantId());

    return buildConfigResponseElement(configPayloads, agentCapabilities);
  }

  private GetAiAppEvaluationConfigContextResponse getAiAppEvaluationConfigContext(
      RequestContext requestContext) {
    GetAiAppEvaluationConfigContextRequest request =
        GetAiAppEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .build();
    return requestContext.call(
        () ->
            stub.withDeadlineAfter(
                    config.getClientConfig().getTimeout().toMillis(), TimeUnit.MILLISECONDS)
                .getAiAppEvaluationConfigContext(request));
  }
}
