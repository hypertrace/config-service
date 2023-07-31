package ai.traceable.blocking.config.service.common;

import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.DataFetcherModule;
import ai.traceable.blocking.config.service.common.customsignature.CustomSignatureCommonModule;
import ai.traceable.blocking.config.service.common.iptype.IpTypeCommonModule;
import ai.traceable.blocking.config.service.common.modsec.ModsecCommonModule;
import ai.traceable.blocking.config.service.common.regions.RegionRulesCommonModule;
import ai.traceable.blocking.config.service.common.rules.BlockingRulesFetcherModule;
import com.google.inject.AbstractModule;
import java.time.Clock;

public class BlockingConfigCommonModule extends AbstractModule {
  @Override
  protected void configure() {
    bind(Clock.class).toInstance(Clock.systemUTC());
    install(new DataFetcherModule());
    install(new IpTypeCommonModule());
    install(new RegionRulesCommonModule());
    install(new ModsecCommonModule());
    install(new CustomSignatureCommonModule());
    install(new BlockingRulesFetcherModule());
  }
}
