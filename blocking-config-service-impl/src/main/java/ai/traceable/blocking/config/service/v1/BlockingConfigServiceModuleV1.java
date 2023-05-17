package ai.traceable.blocking.config.service.v1;

import ai.traceable.blocking.config.service.common.BlockingConfigCommonModule;
import ai.traceable.blocking.config.service.v1.blockingmodsec.ModsecBlockingManagerModule;
import ai.traceable.blocking.config.service.v1.blockingpolicy.BlockingPolicyConfigurationManagerModule;
import ai.traceable.blocking.config.service.v1.customsignature.CustomModsecBlockingManagerModule;
import ai.traceable.blocking.config.service.v1.iptype.IpTypeBlockingManagerModule;
import ai.traceable.blocking.config.service.v1.regions.RegionBlockingManagerModule;
import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;
import io.grpc.BindableService;

public class BlockingConfigServiceModuleV1 extends AbstractModule {
  @Override
  protected void configure() {
    install(new RegionBlockingManagerModule());
    install(new CustomModsecBlockingManagerModule());
    install(new ModsecBlockingManagerModule());
    install(new BlockingPolicyConfigurationManagerModule());
    install(new IpTypeBlockingManagerModule());
    install(new BlockingConfigCommonModule());

    Multibinder.newSetBinder(binder(), BindableService.class)
        .addBinding()
        .to(BlockingConfigServiceImpl.class);
  }
}
