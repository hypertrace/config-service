package ai.traceable.genai.system.discovery.config.service.v1;

import ai.traceable.genai.system.discovery.config.service.v1.manager.GenAiSystemDiscoveryRuleManager;
import ai.traceable.genai.system.discovery.config.service.v1.manager.GenAiSystemDiscoveryRuleManagerImpl;
import ai.traceable.genai.system.discovery.config.service.v1.validation.GenAiSystemDiscoveryRulesValidator;
import ai.traceable.genai.system.discovery.config.service.v1.validation.GenAiSystemDiscoveryRulesValidatorImpl;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class GenAiSystemDiscoveryConfigServiceModule extends AbstractModule {
  private final Channel channel;
  private final ConfigChangeEventGenerator changeEventGenerator;

  public GenAiSystemDiscoveryConfigServiceModule(
      Channel channel, ConfigChangeEventGenerator changeEventGenerator) {
    this.channel = channel;
    this.changeEventGenerator = changeEventGenerator;
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(GenAiSystemDiscoveryConfigServiceImpl.class);
    bind(Channel.class).toInstance(channel);
    bind(ConfigChangeEventGenerator.class).toInstance(changeEventGenerator);
    bind(GenAiSystemDiscoveryRulesValidator.class).to(GenAiSystemDiscoveryRulesValidatorImpl.class);
    bind(GenAiSystemDiscoveryRuleManager.class).to(GenAiSystemDiscoveryRuleManagerImpl.class);
  }

  @Provides
  ConfigServiceGrpc.ConfigServiceBlockingStub providesConfigService(Channel channel) {
    return ConfigServiceGrpc.newBlockingStub(channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
