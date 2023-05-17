package ai.traceable.blocking.config.service.v2;

import ai.traceable.blocking.config.service.common.BlockingConfigCommonModule;
import ai.traceable.blocking.config.service.v2.blockingpolicy.BlockingPolicyConfigurationManager;
import ai.traceable.blocking.config.service.v2.blockingpolicy.BlockingPolicyConfigurationManagerModule;
import ai.traceable.blocking.config.service.v2.customsignature.CustomSignatureBlockingManager;
import ai.traceable.blocking.config.service.v2.iptype.IpTypeBlockingManager;
import ai.traceable.blocking.config.service.v2.iptype.IpTypeBlockingManagerModule;
import ai.traceable.blocking.config.service.v2.modsec.ModsecBlockingManager;
import ai.traceable.blocking.config.service.v2.regions.RegionBlockingManager;
import ai.traceable.blocking.config.service.v2.regions.RegionBlockingManagerModule;
import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;
import io.grpc.BindableService;

public class BlockingConfigServiceModuleV2 extends AbstractModule {
  @Override
  protected void configure() {
    Multibinder<BlockingConfigManagerBase> managerBaseMultibinder =
        Multibinder.newSetBinder(binder(), BlockingConfigManagerBase.class);
    managerBaseMultibinder.addBinding().to(BlockingPolicyConfigurationManager.class);
    managerBaseMultibinder.addBinding().to(IpTypeBlockingManager.class);
    managerBaseMultibinder.addBinding().to(ModsecBlockingManager.class);
    managerBaseMultibinder.addBinding().to(RegionBlockingManager.class);
    managerBaseMultibinder.addBinding().to(CustomSignatureBlockingManager.class);

    install(new RegionBlockingManagerModule());
    install(new BlockingPolicyConfigurationManagerModule());
    install(new IpTypeBlockingManagerModule());
    install(new BlockingConfigCommonModule());

    Multibinder.newSetBinder(binder(), BindableService.class)
        .addBinding()
        .to(BlockingConfigServiceImpl.class);
  }
}
