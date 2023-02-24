package ai.traceable.blocking.config.service.v1.blockingmodsec;

import com.google.inject.AbstractModule;

public class ModsecBlockingManagerModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(ModsecBlockingManager.class).to(DefaultModsecBlockingManager.class);
  }
}
