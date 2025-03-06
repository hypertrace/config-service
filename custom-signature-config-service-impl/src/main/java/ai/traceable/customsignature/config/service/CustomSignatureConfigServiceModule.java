package ai.traceable.customsignature.config.service;

import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.customsignature.config.service.modsec.ModsecRulesManagerModule;
import ai.traceable.customsignature.config.service.rules.RulesManagerModule;
import ai.traceable.customsignature.config.service.rules.converter.expression.ExpressionConverterModule;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;

class CustomSignatureConfigServiceModule extends AbstractModule {
  private final CustomSignatureConfigServiceConfig config;
  private final Channel channel;
  private final ConfigChangeEventGenerator configChangeEventGenerator;
  private final FeatureCachingClient featureCachingClient;

  CustomSignatureConfigServiceModule(
      Channel channel,
      Config config,
      ConfigChangeEventGenerator configChangeEventGenerator,
      FeatureCachingClient featureCachingClient) {
    this.channel = channel;
    this.config = new CustomSignatureConfigServiceConfig(config);
    this.configChangeEventGenerator = configChangeEventGenerator;
    this.featureCachingClient = featureCachingClient;
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(CustomSignatureConfigServiceImpl.class);
    bind(Channel.class).toInstance(channel);
    bind(CustomSignatureConfigServiceConfig.class).toInstance(this.config);
    bind(ConfigChangeEventGenerator.class).toInstance(configChangeEventGenerator);
    bind(FeatureCachingClient.class).toInstance(featureCachingClient);
    install(new RulesManagerModule());
    install(new ModsecRulesManagerModule());
    install(new ExpressionConverterModule());
  }

  @Provides
  ModsecRuleVersion providesModsecRuleVersion(
      CustomSignatureConfigServiceConfig customSignatureConfigServiceConfig) {
    return customSignatureConfigServiceConfig.getModsecRuleVersion();
  }
}
