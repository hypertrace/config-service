package ai.traceable.detection.exclusion.config.service.v1.rules.modsec;

import ai.traceable.anomaly.config.service.registry.AnomalyConfigRegistryModule;
import ai.traceable.customsignature.config.service.modsec.ModsecRulesManagerModule;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import io.grpc.Channel;
import org.hypertrace.config.objectstore.ClientConfig;

public class ExclusionModsecRulesModule extends AbstractModule {

  private final Channel channel;

  public ExclusionModsecRulesModule(Channel channel) {
    this.channel = channel;
  }

  @Override
  protected void configure() {
    bind(Channel.class).toInstance(channel);
    bind(ModsecClauseConverter.class).to(ModsecClauseConverterImpl.class);
    install(new AnomalyConfigRegistryModule());
    install(new ModsecRulesManagerModule());
  }

  @Provides
  ClientConfig provideClientConfig() {
    return ClientConfig.DEFAULT;
  }
}
