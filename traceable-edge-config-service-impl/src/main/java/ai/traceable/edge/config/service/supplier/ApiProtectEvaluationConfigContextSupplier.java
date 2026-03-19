package ai.traceable.edge.config.service.supplier;

import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyEnvironmentScope;
import ai.traceable.anomaly.config.service.v1.RuleEvaluationPoint;
import ai.traceable.anomaly.config.service.v1.apiprotect.AnomalyApiProtectConfigServiceGrpc;
import ai.traceable.anomaly.config.service.v1.apiprotect.GetApiProtectEvaluationConfigContextRequest;
import ai.traceable.anomaly.config.service.v1.apiprotect.GetApiProtectEvaluationConfigContextResponse;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.edge.config.service.AbstractTraceableEdgeConfigSupplier;
import ai.traceable.edge.config.service.config.TraceableEdgeConfig;
import ai.traceable.edge.config.service.v1.AgentCapabilities;
import ai.traceable.edge.config.service.v1.ConfigPayloads;
import ai.traceable.edge.config.service.v1.ConfigRequestElement;
import ai.traceable.edge.config.service.v1.ConfigResponseElement;
import ai.traceable.protection.engine.config.apiprotect.v1.ApiProtectionConfigContext;
import com.google.inject.Inject;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class ApiProtectEvaluationConfigContextSupplier extends AbstractTraceableEdgeConfigSupplier {
  private static final String CONFIG_TYPE = ApiProtectionConfigContext.class.getSimpleName();
  private final AnomalyApiProtectConfigServiceGrpc.AnomalyApiProtectConfigServiceBlockingStub stub;

  @Inject
  public ApiProtectEvaluationConfigContextSupplier(
      AnomalyApiProtectConfigServiceGrpc.AnomalyApiProtectConfigServiceBlockingStub stub,
      UuidGenerator uuidGenerator,
      TraceableEdgeConfig config) {
    super(uuidGenerator, config);
    this.stub = stub;
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
        "Received request for ApiProtectEvaluationConfigContext for tenantId: {}",
        requestContext.getTenantId());
    GetApiProtectEvaluationConfigContextResponse response =
        getApiProtectEvaluationConfigContext(requestContext, environment);
    ConfigPayloads configPayloads =
        ConfigPayloads.newBuilder()
            .addConfigBytes(response.getApiProtectEvaluationConfigContext())
            .build();
    log.debug(
        "Returning ApiProtectEvaluationConfigContext for tenantId: {}",
        requestContext.getTenantId());
    return buildConfigResponseElement(configPayloads, agentCapabilities);
  }

  private GetApiProtectEvaluationConfigContextResponse getApiProtectEvaluationConfigContext(
      RequestContext requestContext, String environment) {
    GetApiProtectEvaluationConfigContextRequest.Builder requestBuilder =
        GetApiProtectEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE);
    Optional.ofNullable(environment)
        .filter(env -> !env.isBlank())
        .ifPresent(
            environmentName ->
                requestBuilder.setConfigScope(
                    AnomalyConfigScope.newBuilder()
                        .setEnvironmentScope(
                            AnomalyEnvironmentScope.newBuilder().setEnvironmentId(environmentName))
                        .build()));
    return requestContext.call(
        () ->
            stub.withDeadlineAfter(
                    config.getClientConfig().getTimeout().toMillis(), TimeUnit.MILLISECONDS)
                .getApiProtectEvaluationConfigContext(requestBuilder.build()));
  }
}
