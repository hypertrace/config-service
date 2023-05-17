package ai.traceable.blocking.config.service.common.iptype;

import ai.traceable.config.utils.refresh.FileRefreshConfig;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesConfigServiceGrpc.MaliciousSourcesConfigServiceBlockingStub;
import com.google.inject.AbstractModule;

public class IpTypeCommonModule extends AbstractModule {
  @Override
  protected void configure() {
    requireBinding(FileRefreshConfig.class);
    requireBinding(MaliciousSourcesConfigServiceBlockingStub.class);
  }
}
