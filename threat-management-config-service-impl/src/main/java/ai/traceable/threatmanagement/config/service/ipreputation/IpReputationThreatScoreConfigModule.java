package ai.traceable.threatmanagement.config.service.ipreputation;

import com.google.inject.AbstractModule;

public class IpReputationThreatScoreConfigModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(IpReputationThreatScoreConfigManager.class)
        .to(IpReputationThreatScoreConfigManagerImpl.class);
  }
}
