package ai.traceable.blocking.config.service.common.modsec;

import ai.traceable.anomaly.config.service.v1.modsec.AnomalyModsecConfigServiceGrpc.AnomalyModsecConfigServiceBlockingStub;
import com.google.inject.AbstractModule;

public class ModsecCommonModule extends AbstractModule {
  @Override
  protected void configure() {
    requireBinding(AnomalyModsecConfigServiceBlockingStub.class);
  }
}
