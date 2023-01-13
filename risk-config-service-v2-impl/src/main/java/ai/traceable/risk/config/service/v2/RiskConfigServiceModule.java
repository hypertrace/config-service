package ai.traceable.risk.config.service.v2;

import ai.traceable.risk.config.service.v2.contributors.RiskContributorConfigsModule;
import ai.traceable.risk.config.service.v2.elements.RiskElementConfigModule;
import ai.traceable.risk.config.service.v2.factors.RiskFactorConfigsModule;
import ai.traceable.risk.config.service.v2.grid.RiskScoringGridConfigModule;
import ai.traceable.risk.config.service.v2.level.RiskLevelConfigModule;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.typesafe.config.Config;
import io.grpc.BindableService;
import io.grpc.Channel;
import lombok.AllArgsConstructor;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;

@AllArgsConstructor
public class RiskConfigServiceModule extends AbstractModule {

  private static final String RISK_CONFIG_SERVICE_CONFIG_PATH = "risk.config.service";

  private final Channel channel;
  private final Config config;
  private final ConfigChangeEventGenerator configChangeEventGenerator;

  @Override
  protected void configure() {
    bind(BindableService.class).to(RiskConfigServiceImpl.class);
    bind(ConfigChangeEventGenerator.class).toInstance(configChangeEventGenerator);
    install(new RiskLevelConfigModule());
    install(new RiskScoringGridConfigModule());
    install(new RiskFactorConfigsModule());
    install(new RiskContributorConfigsModule());
    install(new RiskElementConfigModule());
  }

  @Provides
  @Singleton
  RiskConfigServiceConfig providesRiskServiceConfig() {
    return new RiskConfigServiceConfig(config.getConfig(RISK_CONFIG_SERVICE_CONFIG_PATH));
  }

  @Provides
  @Singleton
  ConfigServiceGrpc.ConfigServiceBlockingStub providesConfigService() {
    return ConfigServiceGrpc.newBlockingStub(channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }
}
