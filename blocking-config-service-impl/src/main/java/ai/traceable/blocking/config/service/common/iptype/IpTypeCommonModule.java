package ai.traceable.blocking.config.service.common.iptype;

import ai.traceable.config.utils.refresh.FileRefreshConfig;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesConfigServiceGrpc.MaliciousSourcesConfigServiceBlockingStub;
import com.google.inject.AbstractModule;
import com.typesafe.config.Config;

public class IpTypeCommonModule extends AbstractModule {

  private static final String IP_TYPE_BLOCKING_CONFIG = "blocking.config.service.iptype";
  private final FileRefreshConfig ipTypeRulesBuilderFileRefreshConfig;

  public IpTypeCommonModule(Config config) {
    this.ipTypeRulesBuilderFileRefreshConfig =
        new FileRefreshConfig(config.getConfig(IP_TYPE_BLOCKING_CONFIG));
  }

  @Override
  protected void configure() {
    bind(FileRefreshConfig.class).toInstance(ipTypeRulesBuilderFileRefreshConfig);
    requireBinding(MaliciousSourcesConfigServiceBlockingStub.class);
  }
}
