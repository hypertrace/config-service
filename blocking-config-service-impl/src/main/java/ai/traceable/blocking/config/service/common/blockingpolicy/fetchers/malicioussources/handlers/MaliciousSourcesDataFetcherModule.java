package ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.malicioussources.handlers;

import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleCondition;
import com.google.inject.AbstractModule;
import com.google.inject.multibindings.MapBinder;

public class MaliciousSourcesDataFetcherModule extends AbstractModule {

  @Override
  protected void configure() {
    MapBinder<MaliciousSourcesRuleCondition.ConditionCase, MaliciousSourceDataHandler> multiBinder =
        MapBinder.newMapBinder(
            binder(),
            MaliciousSourcesRuleCondition.ConditionCase.class,
            MaliciousSourceDataHandler.class);
    multiBinder
        .addBinding(MaliciousSourcesRuleCondition.ConditionCase.IP_RANGE_CONDITION)
        .to(IpRangeDataHandler.class);
    multiBinder
        .addBinding(MaliciousSourcesRuleCondition.ConditionCase.IP_LOCATION_TYPE_CONDITION)
        .to(IpTypeDataHandler.class);
    multiBinder
        .addBinding(MaliciousSourcesRuleCondition.ConditionCase.REGION_CONDITION)
        .to(RegionDataHandler.class);
  }
}
