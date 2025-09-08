package ai.traceable.attribute.resolution.config.service.v1;

import ai.traceable.attribute.resolution.config.service.v1.manager.AttributeResolutionConfigManager;
import ai.traceable.attribute.resolution.config.service.v1.manager.AttributeResolutionConfigManagerImpl;
import ai.traceable.attribute.resolution.config.service.v1.validation.AttributeResolutionConfigValidator;
import ai.traceable.attribute.resolution.config.service.v1.validation.AttributeResolutionConfigValidatorImpl;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class AttributeResolutionConfigServiceModule extends AbstractModule {
  private final Channel channel;
  private final ConfigChangeEventGenerator changeEventGenerator;

  public AttributeResolutionConfigServiceModule(
      Channel channel, ConfigChangeEventGenerator changeEventGenerator) {
    this.channel = channel;
    this.changeEventGenerator = changeEventGenerator;
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(AttributeResolutionConfigServiceImpl.class);
    bind(Channel.class).toInstance(channel);
    bind(ConfigChangeEventGenerator.class).toInstance(changeEventGenerator);
    bind(AttributeResolutionConfigValidator.class).to(AttributeResolutionConfigValidatorImpl.class);
    bind(AttributeResolutionConfigManager.class).to(AttributeResolutionConfigManagerImpl.class);
  }

  @Provides
  ConfigServiceGrpc.ConfigServiceBlockingStub providesConfigService(Channel channel) {
    return ConfigServiceGrpc.newBlockingStub(channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
