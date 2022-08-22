package ai.traceable.blocking.config.service.blockingpolicy.fetchers;

import ai.traceable.anomaly.config.service.v1.detector.DetectorConfigServiceGrpc.DetectorConfigServiceBlockingStub;
import ai.traceable.anomaly.config.service.v1.global.AnomalyGlobalConfigServiceGrpc.AnomalyGlobalConfigServiceBlockingStub;
import ai.traceable.blocking.config.service.blockingpolicy.fetchers.actor.config.ActorServiceConfig;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub;
import ai.traceable.iprange.config.service.v1.IpRangeConfigServiceGrpc.IpRangeConfigServiceBlockingStub;
import ai.traceable.platform.actor.v1.ActorServiceGrpc.ActorServiceBlockingStub;
import ai.traceable.region.config.service.v1.RegionConfigServiceGrpc.RegionConfigServiceBlockingStub;
import com.google.inject.AbstractModule;

public class DataFetcherModule extends AbstractModule {
  @Override
  protected void configure() {
    requireBinding(AnomalyGlobalConfigServiceBlockingStub.class);
    requireBinding(DetectorConfigServiceBlockingStub.class);
    requireBinding(CustomSignatureConfigServiceBlockingStub.class);
    requireBinding(RegionConfigServiceBlockingStub.class);
    requireBinding(IpRangeConfigServiceBlockingStub.class);
    requireBinding(ActorServiceBlockingStub.class);
    requireBinding(ActorServiceConfig.class);
  }
}
