package ai.traceable.edge.config.service.supplier;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.edge.bot.config.service.v1.CaptchaSiteKeyConfig;
import ai.traceable.edge.bot.config.service.v1.CaptchaSiteKeyConfigServiceGrpc;
import ai.traceable.edge.bot.config.service.v1.GetAllRequest;
import ai.traceable.edge.config.service.TraceableEdgeConfigSupplier;
import ai.traceable.edge.config.service.config.TraceableEdgeConfig;
import ai.traceable.edge.config.service.v1.AgentCapabilities;
import ai.traceable.edge.config.service.v1.ConfigPayloads;
import ai.traceable.edge.config.service.v1.ConfigRequestElement;
import ai.traceable.edge.config.service.v1.ConfigResponseElement;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class CaptchaSiteKeyConfigSupplier implements TraceableEdgeConfigSupplier {
  private static final String CONFIG_TYPE = CaptchaSiteKeyConfig.class.getSimpleName();
  private final CaptchaSiteKeyConfigServiceGrpc.CaptchaSiteKeyConfigServiceBlockingStub stub;
  private final TraceableEdgeConfig config;
  private final UuidGenerator uuidGenerator;

  @Inject
  public CaptchaSiteKeyConfigSupplier(
      TraceableEdgeConfig config,
      CaptchaSiteKeyConfigServiceGrpc.CaptchaSiteKeyConfigServiceBlockingStub stub,
      UuidGenerator uuidGenerator) {
    this.config = config;
    this.stub = stub;
    this.uuidGenerator = uuidGenerator;
  }

  @Override
  public String getConfigType() {
    return CONFIG_TYPE;
  }

  @Override
  public ConfigResponseElement getConfigs(
      RequestContext requestContext,
      ConfigRequestElement requestElement,
      AgentCapabilities agentCapabilities) {
    var allCaptchaSiteKeyConfigs =
        requestContext.call(
            () ->
                stub.withDeadlineAfter(
                        config.getClientConfig().getTimeout().toMillis(), TimeUnit.MILLISECONDS)
                    .getAll(GetAllRequest.getDefaultInstance()));
    ConfigPayloads.Builder configPayloadsBuilder = ConfigPayloads.newBuilder();
    for (var config : allCaptchaSiteKeyConfigs.getConfigsList()) {
      configPayloadsBuilder.addConfigBytes(config.toByteString());
    }
    ConfigPayloads configPayloads = configPayloadsBuilder.build();
    return ConfigResponseElement.newBuilder()
        .setConfigType(getConfigType())
        .setEnabled(true)
        .addSupportedAgentCapabilities(agentCapabilities)
        .setRefreshAfterDuration(config.getAgentPollingFrequency(getConfigType()))
        .setConfigPayloads(configPayloads)
        .setHash(uuidGenerator.generateId(configPayloads))
        .build();
  }
}
