package ai.traceable.edge.config.service.supplier;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.edge.bot.config.service.v1.BotConfigServiceGrpc;
import ai.traceable.edge.bot.config.service.v1.CaptchaSiteKeyConfig;
import ai.traceable.edge.bot.config.service.v1.GetAllCaptchaSiteKeyConfigsRequest;
import ai.traceable.edge.config.service.AbstractTraceableEdgeConfigSupplier;
import ai.traceable.edge.config.service.config.TraceableEdgeConfig;
import ai.traceable.edge.config.service.v1.AgentCapabilities;
import ai.traceable.edge.config.service.v1.ConfigPayloads;
import ai.traceable.edge.config.service.v1.ConfigRequestElement;
import ai.traceable.edge.config.service.v1.ConfigResponseElement;
import com.google.protobuf.AbstractMessageLite;
import jakarta.inject.Inject;
import java.util.concurrent.TimeUnit;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class CaptchaSiteKeyConfigSupplier extends AbstractTraceableEdgeConfigSupplier {
  private static final String CONFIG_TYPE = CaptchaSiteKeyConfig.class.getSimpleName();
  private final BotConfigServiceGrpc.BotConfigServiceBlockingStub stub;

  @Inject
  public CaptchaSiteKeyConfigSupplier(
      TraceableEdgeConfig config,
      BotConfigServiceGrpc.BotConfigServiceBlockingStub stub,
      UuidGenerator uuidGenerator) {
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
    var allCaptchaSiteKeyConfigs =
        requestContext.call(
            () ->
                stub.withDeadlineAfter(
                        config.getClientConfig().getTimeout().toMillis(), TimeUnit.MILLISECONDS)
                    .getAllCaptchaSiteKeyConfigs(
                        GetAllCaptchaSiteKeyConfigsRequest.getDefaultInstance()));
    ConfigPayloads.Builder configPayloadsBuilder = ConfigPayloads.newBuilder();
    allCaptchaSiteKeyConfigs.getConfigsList().stream()
        .map(AbstractMessageLite::toByteString)
        .forEach(configPayloadsBuilder::addConfigBytes);
    ConfigPayloads configPayloads = configPayloadsBuilder.build();

    // Use the generic builder method which includes configType in hash
    return buildConfigResponseElement(configPayloads, agentCapabilities);
  }
}
