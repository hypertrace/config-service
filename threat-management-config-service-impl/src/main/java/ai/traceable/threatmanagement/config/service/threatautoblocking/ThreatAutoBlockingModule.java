package ai.traceable.threatmanagement.config.service.threatautoblocking;

import com.google.inject.AbstractModule;
import java.time.Clock;

public class ThreatAutoBlockingModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(Clock.class).toInstance(Clock.systemUTC());
    bind(ThreatAutoBlockingManager.class).to(DefaultThreatAutoBlockingManager.class);
  }
}
