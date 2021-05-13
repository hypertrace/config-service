package ai.traceable.blocking.config.service.safecrs;

import com.google.inject.AbstractModule;

public class SafeCrsBlockingManagerModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(SafeCrsBlockingManager.class).to(DefaultSafeCrsBlockingManager.class);
  }
}
