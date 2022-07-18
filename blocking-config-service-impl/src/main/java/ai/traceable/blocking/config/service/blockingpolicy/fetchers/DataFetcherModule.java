package ai.traceable.blocking.config.service.blockingpolicy.fetchers;

import ai.traceable.anomaly.config.service.v1.detector.DetectorConfigServiceGrpc.DetectorConfigServiceBlockingStub;
import ai.traceable.anomaly.config.service.v1.global.AnomalyGlobalConfigServiceGrpc.AnomalyGlobalConfigServiceBlockingStub;
import ai.traceable.blocking.config.service.blockingpolicy.fetchers.config.ActorServiceConfig;
import ai.traceable.blocking.config.service.blockingpolicy.fetchers.impl.ActorBasedDataFetcherImpl;
import ai.traceable.blocking.config.service.blockingpolicy.fetchers.impl.CustomIpBasedDataFetcherImpl;
import ai.traceable.blocking.config.service.blockingpolicy.fetchers.impl.CustomSignatureDataFetcherImpl;
import ai.traceable.blocking.config.service.blockingpolicy.fetchers.impl.ModsecDataFetcherImpl;
import ai.traceable.blocking.config.service.blockingpolicy.fetchers.impl.RegionDataFetcherImpl;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub;
import ai.traceable.iprange.config.service.v1.IpRangeConfigServiceGrpc.IpRangeConfigServiceBlockingStub;
import ai.traceable.platform.actor.v1.ActorServiceGrpc.ActorServiceBlockingStub;
import ai.traceable.region.config.service.v1.RegionConfigServiceGrpc.RegionConfigServiceBlockingStub;
import com.google.inject.AbstractModule;

public class DataFetcherModule extends AbstractModule {
  @Override
  protected void configure() {
    bind(ActorBasedDataFetcher.class).to(ActorBasedDataFetcherImpl.class);
    bind(CustomIpBasedDataFetcher.class).to(CustomIpBasedDataFetcherImpl.class);
    bind(CustomSignatureDataFetcher.class).to(CustomSignatureDataFetcherImpl.class);
    bind(ModsecDataFetcher.class).to(ModsecDataFetcherImpl.class);
    bind(RegionDataFetcher.class).to(RegionDataFetcherImpl.class);

    requireBinding(AnomalyGlobalConfigServiceBlockingStub.class);
    requireBinding(DetectorConfigServiceBlockingStub.class);
    requireBinding(CustomSignatureConfigServiceBlockingStub.class);
    requireBinding(RegionConfigServiceBlockingStub.class);
    requireBinding(IpRangeConfigServiceBlockingStub.class);
    requireBinding(ActorServiceBlockingStub.class);
    requireBinding(ActorServiceConfig.class);
  }
}
