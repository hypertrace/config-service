package ai.traceable.ratelimiting.service.v2.rules.converter.condition;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.google.inject.multibindings.Multibinder;

@Singleton
public class RateLimitingConditionModule extends AbstractModule {

  @Override
  protected void configure() {
    final Multibinder<RateLimitingConditionConverter> multiBinder =
        Multibinder.newSetBinder(binder(), RateLimitingConditionConverter.class);
    multiBinder.addBinding().to(RateLimitingScopeConditionConverter.class);
    multiBinder.addBinding().to(RateLimitingUserIdConditionConverter.class);
    multiBinder.addBinding().to(RateLimitingRegionConditionConverter.class);
    multiBinder.addBinding().to(RateLimitingIpAddressConditionConverter.class);
    multiBinder.addBinding().to(RateLimitingIpTypeConditionConverter.class);
    multiBinder.addBinding().to(RateLimitingIpAbuseVelocityConditionConverter.class);
  }
}
