package ai.traceable.blocking.config.service.iptype;

import ai.traceable.config.utils.refresh.FileRefreshConfig;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesConfigServiceGrpc.MaliciousSourcesConfigServiceBlockingStub;
import com.google.inject.AbstractModule;
import com.typesafe.config.Config;

public class IpTypeBlockingManagerModule extends AbstractModule {
  private static final String IP_TYPE_BLOCKING_CONFIG = "blocking.config.service.iptype";
  private final FileRefreshConfig ipTypeRulesBuilderFileRefreshConfig;

  public IpTypeBlockingManagerModule(Config config) {
    this.ipTypeRulesBuilderFileRefreshConfig =
        new FileRefreshConfig(config.getConfig(IP_TYPE_BLOCKING_CONFIG));
  }

  @Override
  protected void configure() {
    bind(IpTypeBlockingManager.class).to(DefaultIpTypeBlockingManager.class);
    bind(FileRefreshConfig.class).toInstance(ipTypeRulesBuilderFileRefreshConfig);
    requireBinding(MaliciousSourcesConfigServiceBlockingStub.class);
  }
}
