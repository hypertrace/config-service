package ai.traceable.risk.config.service;

import ai.traceable.risk.config.service.factorgrid.RiskFactorGridConfigModule;
import ai.traceable.risk.config.service.factors.RiskFactorConfigsModule;
import ai.traceable.risk.config.service.level.RiskLevelConfigModule;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

public class RiskConfigServiceModule extends AbstractModule {

  private static final String RISK_CONFIG_SERVICE_CONFIG_PATH = "risk.config.service";

  private final Config config;
  private final Channel channel;
  private final ConfigChangeEventGenerator configChangeEventGenerator;

  RiskConfigServiceModule(
      Channel channel, Config config, ConfigChangeEventGenerator configChangeEventGenerator) {
    this.channel = channel;
    this.config = config;
    this.configChangeEventGenerator = configChangeEventGenerator;
  }

  @Override
  protected void configure() {
    bind(BindableService.class).to(RiskConfigServiceImpl.class);
    bind(ConfigChangeEventGenerator.class).toInstance(configChangeEventGenerator);
    install(new RiskLevelConfigModule());
    install(new RiskFactorGridConfigModule());
    install(new RiskFactorConfigsModule());
  }

  @Provides
  RiskConfigServiceConfig providesRiskServiceConfig() {
    return new RiskConfigServiceConfig(config.getConfig(RISK_CONFIG_SERVICE_CONFIG_PATH));
  }

  @Provides
  ConfigServiceGrpc.ConfigServiceBlockingStub providesConfigService() {
    return ConfigServiceGrpc.newBlockingStub(channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
