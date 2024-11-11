package ai.traceable.edge.config.service.supplier;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.edge.bot.config.service.v1.CaptchaSiteKeyConfig;
import ai.traceable.edge.bot.config.service.v1.CaptchaSiteKeyConfigServiceGrpc;
import ai.traceable.edge.bot.config.service.v1.GetAllRequest;
import ai.traceable.edge.config.service.TraceableEdgeConfigSupplier;
import ai.traceable.edge.config.service.v1.AgentCapabilities;
import ai.traceable.edge.config.service.v1.ConfigPayloads;
import ai.traceable.edge.config.service.v1.ConfigRequestElement;
import ai.traceable.edge.config.service.v1.ConfigResponseElement;
import com.google.protobuf.Duration;
import com.typesafe.config.Config;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import org.hypertrace.config.objectstore.ClientConfig;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class CaptchaSiteKeyConfigSupplier implements TraceableEdgeConfigSupplier {
  private static final String CONFIG_TYPE = CaptchaSiteKeyConfig.class.getSimpleName();
  private final CaptchaSiteKeyConfigServiceGrpc.CaptchaSiteKeyConfigServiceBlockingStub stub;
  private final ClientConfig clientConfig;
  private final Duration agentPollingFrequency;
  private final UuidGenerator uuidGenerator;

  @Inject
  public CaptchaSiteKeyConfigSupplier(
      Config config,
      CaptchaSiteKeyConfigServiceGrpc.CaptchaSiteKeyConfigServiceBlockingStub stub,
      ClientConfig clientConfig,
      UuidGenerator uuidGenerator) {
    this.stub = stub;
    this.clientConfig = clientConfig;
    this.uuidGenerator = uuidGenerator;
    this.agentPollingFrequency = getAgentPollingFrequency(config, CONFIG_TYPE);
  }

  @Override
  public String getConfigType() {
    return CONFIG_TYPE;
  }

  @Override
  public String getConfigDeserializer() {
    return CaptchaSiteKeyConfig.class.getName();
  }

  @Override
  public ConfigResponseElement getConfigs(
      RequestContext requestContext,
      ConfigRequestElement requestElement,
      AgentCapabilities agentCapabilities) {
    var allCaptchaSiteKeyConfigs =
        requestContext.call(
            () ->
                stub.withDeadlineAfter(clientConfig.getTimeout().toMillis(), TimeUnit.MILLISECONDS)
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
        .setRefreshAfterDuration(agentPollingFrequency)
        .setConfigPayloads(configPayloads)
        .setHash(uuidGenerator.generateId(configPayloads))
        .build();
  }
}
