package ai.traceable.edge.config.service.supplier;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc;
import ai.traceable.customsignature.config.service.v1.EventType;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureEvaluationConfigContextRequest;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureEvaluationConfigContextResponse;
import ai.traceable.customsignature.config.service.v1.RuleEvaluationPoint;
import ai.traceable.edge.config.service.TraceableEdgeConfigSupplier;
import ai.traceable.edge.config.service.config.TraceableEdgeConfig;
import ai.traceable.edge.config.service.v1.AgentCapabilities;
import ai.traceable.edge.config.service.v1.ConfigPayloads;
import ai.traceable.edge.config.service.v1.ConfigRequestElement;
import ai.traceable.edge.config.service.v1.ConfigResponseElement;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureConfigContext;
import com.google.inject.Inject;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = {@Inject})
public class CustomSignatureEvaluationConfigContextSupplier implements TraceableEdgeConfigSupplier {
  private static final String CONFIG_TYPE = CustomSignatureConfigContext.class.getSimpleName();
  private final CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub stub;
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
        "Received request for CustomSignatureEvaluationConfigContext for tenantId: {} {} {}",
        requestContext.getTenantId(),
        environment,
        requestElement);
    GetCustomSignatureEvaluationConfigContextResponse response =
        getCustomSignatureEvaluationConfigContext(requestContext, requestElement);
    ConfigPayloads configPayloads =
        ConfigPayloads.newBuilder()
            .addConfigBytes(response.getCustomSignatureEvaluationConfigContext())
            .build();
    log.debug(
        "Returning CustomSignatureEvaluationConfigContext for tenantId: {}",
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

  private GetCustomSignatureEvaluationConfigContextResponse
      getCustomSignatureEvaluationConfigContext(
          RequestContext requestContext, ConfigRequestElement requestElement) {
    try {
      GetCustomSignatureEvaluationConfigContextRequest.Builder requestBuilder =
          GetCustomSignatureEvaluationConfigContextRequest.newBuilder()
              .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
              .setRuleVersion(config.getCustomSignatureRuleVersion());
      Optional.ofNullable(
              requestElement
                  .getAgentCapabilities()
                  .getAdditionalFieldsMap()
                  .get("customSignatureEventType"))
          .ifPresent(
              eventType -> {
                switch (eventType) {
                  case "CUSTOM_SIGNATURE_EVENT_TYPE_ALLOW":
                    requestBuilder.setEventType(EventType.EVENT_TYPE_ALLOW);
                    break;
                  case "CUSTOM_SIGNATURE_EVENT_TYPE_BLOCK":
                    requestBuilder.setEventType(EventType.EVENT_TYPE_DETECTION_AND_BLOCKING);
                    break;
                  default:
                    log.error("Unknown rule type: {}", eventType);
                }
              });

      GetCustomSignatureEvaluationConfigContextRequest request = requestBuilder.build();

      return requestContext.call(
          () ->
              stub.withDeadlineAfter(
                      config.getClientConfig().getTimeout().toMillis(), TimeUnit.MILLISECONDS)
                  .getCustomSignatureEvaluationConfigContext(request));
    } catch (Exception e) {
      log.error(
          "Error fetching CustomSignatureEvaluationConfigContext for tenant: {}",
          requestContext.getTenantId(),
          e);
      throw new RuntimeException("Failed to fetch custom signature evaluation config context", e);
    }
  }
}
