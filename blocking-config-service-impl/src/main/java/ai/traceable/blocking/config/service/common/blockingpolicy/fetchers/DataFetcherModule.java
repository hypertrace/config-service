package ai.traceable.blocking.config.service.common.blockingpolicy.fetchers;

import ai.traceable.anomaly.config.service.v1.detector.DetectorConfigServiceGrpc.DetectorConfigServiceBlockingStub;
import ai.traceable.anomaly.config.service.v1.global.AnomalyGlobalConfigServiceGrpc.AnomalyGlobalConfigServiceBlockingStub;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.malicioussources.handlers.MaliciousSourcesDataFetcherModule;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub;
import ai.traceable.iprange.config.service.v1.IpRangeConfigServiceGrpc.IpRangeConfigServiceBlockingStub;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesConfigServiceGrpc.MaliciousSourcesConfigServiceBlockingStub;
import ai.traceable.platform.actor.v1.ActorServiceGrpc.ActorServiceBlockingStub;
import ai.traceable.region.config.service.v1.RegionConfigServiceGrpc.RegionConfigServiceBlockingStub;
import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;

public class DataFetcherModule extends AbstractModule {
  @Override
  protected void configure() {
    install(new MaliciousSourcesDataFetcherModule());

    Multibinder<BlockingPolicyDataFetcherBase> managerBaseMultibinder =
        Multibinder.newSetBinder(binder(), BlockingPolicyDataFetcherBase.class);
    managerBaseMultibinder.addBinding().to(CustomSignatureBlockingPolicyDataFetcher.class);
    managerBaseMultibinder.addBinding().to(CustomIpBasedBlockingPolicyDataFetcher.class);
    managerBaseMultibinder.addBinding().to(ActorBasedBlockingPolicyDataFetcher.class);
    managerBaseMultibinder.addBinding().to(ModsecBlockingPolicyDataFetcher.class);
    managerBaseMultibinder.addBinding().to(RegionBlockingPolicyDataFetcher.class);
    managerBaseMultibinder.addBinding().to(MaliciousSourcesBlockingPolicyDataFetcher.class);

    requireBinding(CustomSignatureConfigServiceBlockingStub.class);
    requireBinding(RegionConfigServiceBlockingStub.class);
    requireBinding(IpRangeConfigServiceBlockingStub.class);
    requireBinding(MaliciousSourcesConfigServiceBlockingStub.class);
    requireBinding(ActorServiceBlockingStub.class);
    requireBinding(AnomalyGlobalConfigServiceBlockingStub.class);
    requireBinding(DetectorConfigServiceBlockingStub.class);
  }
}
