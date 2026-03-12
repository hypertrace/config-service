package ai.traceable.fraud.datamodel.event.kind;

import ai.traceable.fraud.datamodel.event.kind.aggregationfunction.AggregationFunctionProvider;
import ai.traceable.fraud.datamodel.event.kind.aggregationfunction.AggregationFunctionServiceImpl;
import ai.traceable.fraud.datamodel.event.kind.aggregationfunction.DefaultAggregationFunctionProvider;
import ai.traceable.fraud.datamodel.event.kind.eventkind.DefaultEventKindProvider;
import ai.traceable.fraud.datamodel.event.kind.eventkind.EventKindHierarchyResolver;
import ai.traceable.fraud.datamodel.event.kind.eventkind.EventKindProvider;
import ai.traceable.fraud.datamodel.event.kind.eventkind.EventKindServiceImpl;
import ai.traceable.fraud.datamodel.event.kind.operator.DefaultOperatorProvider;
import ai.traceable.fraud.datamodel.event.kind.operator.OperatorProvider;
import ai.traceable.fraud.datamodel.event.kind.operator.OperatorServiceImpl;
import ai.traceable.fraud.datamodel.event.kind.transformationfunction.DefaultTransformationFunctionProvider;
import ai.traceable.fraud.datamodel.event.kind.transformationfunction.TransformationFunctionProvider;
import ai.traceable.fraud.datamodel.event.kind.transformationfunction.TransformationFunctionServiceImpl;
import ai.traceable.fraud.datamodel.event.kind.v1.FraudDataModelEventKindRegistry;
import com.google.inject.AbstractModule;

/** Guice module for Event Kind Config Services. */
public class EventKindConfigServiceModule extends AbstractModule {

  @Override
  protected void configure() {
    // Bind providers to their default implementations
    bind(EventKindProvider.class).to(DefaultEventKindProvider.class);
    bind(EventKindHierarchyResolver.class);
    bind(TransformationFunctionProvider.class).to(DefaultTransformationFunctionProvider.class);
    bind(OperatorProvider.class).to(DefaultOperatorProvider.class);
    bind(AggregationFunctionProvider.class).to(DefaultAggregationFunctionProvider.class);

    // Bind registry for cross-service validation
    bind(FraudDataModelEventKindRegistry.class).to(DefaultFraudDataModelEventKindRegistry.class);

    // Bind service implementations
    bind(EventKindServiceImpl.class);
    bind(TransformationFunctionServiceImpl.class);
    bind(OperatorServiceImpl.class);
    bind(AggregationFunctionServiceImpl.class);
  }
}
