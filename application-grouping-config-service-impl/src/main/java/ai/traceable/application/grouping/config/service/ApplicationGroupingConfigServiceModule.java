package ai.traceable.application.grouping.config.service;

import ai.traceable.application.grouping.config.service.manager.ApplicationGroupingRuleConfigManager;
import ai.traceable.application.grouping.config.service.manager.ApplicationGroupingRuleConfigService;
import ai.traceable.application.grouping.config.service.validations.ApplicationGroupingConfigServiceRequestValidator;
import ai.traceable.application.grouping.config.service.validations.ApplicationGroupingConfigServiceRequestValidatorImpl;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import java.time.Duration;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.DefaultTimeoutClientInterceptor;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class ApplicationGroupingConfigServiceModule extends AbstractModule {

  private final Channel channel;
  private final Config config;
  private final ConfigChangeEventGenerator configChangeEventGenerator;

  ApplicationGroupingConfigServiceModule(
      Channel channel, Config config, ConfigChangeEventGenerator configChangeEventGenerator) {
    this.channel = channel;
    this.config = config;
    this.configChangeEventGenerator = configChangeEventGenerator;
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(ApplicationGroupingConfigServiceImpl.class);
    bind(Config.class).toInstance(config);
    bind(ConfigChangeEventGenerator.class).toInstance(configChangeEventGenerator);
    bind(Channel.class).toInstance(channel);
    bind(ApplicationGroupingRuleConfigService.class).to(ApplicationGroupingRuleConfigManager.class);
    bind(ApplicationGroupingConfigServiceRequestValidator.class)
        .to(ApplicationGroupingConfigServiceRequestValidatorImpl.class);
  }

  @Singleton
  @Provides
  ConfigServiceGrpc.ConfigServiceBlockingStub provideConfigStub() {
    return ConfigServiceGrpc.newBlockingStub(this.channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get())
        .withInterceptors(new DefaultTimeoutClientInterceptor(Duration.ofSeconds(10)));
  }
}
