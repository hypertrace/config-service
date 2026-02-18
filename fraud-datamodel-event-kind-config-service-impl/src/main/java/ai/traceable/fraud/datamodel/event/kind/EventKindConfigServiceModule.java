package ai.traceable.fraud.datamodel.event.kind;

import ai.traceable.fraud.datamodel.event.kind.aggregationfunction.AggregationFunctionProvider;
import ai.traceable.fraud.datamodel.event.kind.aggregationfunction.AggregationFunctionServiceImpl;
import ai.traceable.fraud.datamodel.event.kind.aggregationfunction.DefaultAggregationFunctionProvider;
import ai.traceable.fraud.datamodel.event.kind.eventkind.DefaultEventKindProvider;
import ai.traceable.fraud.datamodel.event.kind.eventkind.EventKindHierarchyResolver;
import ai.traceable.fraud.datamodel.event.kind.eventkind.EventKindProvider;
import ai.traceable.fraud.datamodel.event.kind.eventkind.EventKindServiceImpl;
import ai.traceable.fraud.datamodel.event.kind.transformationfunction.DefaultTransformationFunctionProvider;
import ai.traceable.fraud.datamodel.event.kind.transformationfunction.TransformationFunctionProvider;
import ai.traceable.fraud.datamodel.event.kind.transformationfunction.TransformationFunctionServiceImpl;
import com.google.inject.AbstractModule;

/** Guice module for Event Kind Config Services. */
public class EventKindConfigServiceModule extends AbstractModule {

  @Override
  protected void configure() {
    // Bind providers to their default implementations
    bind(EventKindProvider.class).to(DefaultEventKindProvider.class);
    bind(EventKindHierarchyResolver.class);
    bind(TransformationFunctionProvider.class).to(DefaultTransformationFunctionProvider.class);
    bind(AggregationFunctionProvider.class).to(DefaultAggregationFunctionProvider.class);

    // Bind service implementations
    bind(EventKindServiceImpl.class);
    bind(TransformationFunctionServiceImpl.class);
    bind(AggregationFunctionServiceImpl.class);
  }
}
