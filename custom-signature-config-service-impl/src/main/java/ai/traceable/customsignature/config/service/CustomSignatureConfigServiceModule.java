package ai.traceable.customsignature.config.service;

import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import ai.traceable.customsignature.config.service.modsec.ModsecRulesManagerModule;
import ai.traceable.customsignature.config.service.rules.RulesManagerModule;
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

  CustomSignatureConfigServiceModule(
      Channel channel, Config config, ConfigChangeEventGenerator configChangeEventGenerator) {
    this.channel = channel;
    this.config = new CustomSignatureConfigServiceConfig(config);
    this.configChangeEventGenerator = configChangeEventGenerator;
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(CustomSignatureConfigServiceImpl.class);
    bind(Channel.class).toInstance(channel);
    bind(CustomSignatureConfigServiceConfig.class).toInstance(this.config);
    bind(ConfigChangeEventGenerator.class).toInstance(configChangeEventGenerator);
    install(new RulesManagerModule());
    install(new ModsecRulesManagerModule());
  }

  @Provides
  ModsecRuleVersion providesModsecRuleVersion(
      CustomSignatureConfigServiceConfig customSignatureConfigServiceConfig) {
    return customSignatureConfigServiceConfig.getModsecRuleVersion();
  }
}
