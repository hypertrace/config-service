package ai.traceable.api.gateway.config.service.filter;

import static ai.traceable.api.gateway.config.service.v1.ApiRouteFilter.TypeCase.API_INFO;
import static ai.traceable.api.gateway.config.service.v1.ApiRouteFilter.TypeCase.ORG_IDS;
import static ai.traceable.api.gateway.config.service.v1.ApiRouteFilter.TypeCase.TYPE_NOT_SET;

import ai.traceable.api.gateway.config.service.v1.ApiRouteFilter;
import com.google.inject.AbstractModule;
import com.google.inject.multibindings.MapBinder;

public class ApiRouteFilterModule extends AbstractModule {

  @Override
  protected void configure() {
    bindFilterToPredicateConverters();
  }

  private void bindFilterToPredicateConverters() {
    final MapBinder<ApiRouteFilter.TypeCase, ApiRouteFilterToPredicateConverter> binderMap =
        MapBinder.newMapBinder(
            binder(), ApiRouteFilter.TypeCase.class, ApiRouteFilterToPredicateConverter.class);
    binderMap.addBinding(TYPE_NOT_SET).to(EmptyFilterToPredicateConverter.class);
    binderMap.addBinding(ORG_IDS).to(OrgIdFilterToPredicateConverter.class);
    binderMap.addBinding(API_INFO).to(ApiInfoFilterToPredicateConverter.class);
  }
}
