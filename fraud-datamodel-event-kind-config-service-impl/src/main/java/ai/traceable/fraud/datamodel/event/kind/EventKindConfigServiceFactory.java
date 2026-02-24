package ai.traceable.fraud.datamodel.event.kind;

import ai.traceable.fraud.datamodel.event.kind.aggregationfunction.AggregationFunctionServiceImpl;
import ai.traceable.fraud.datamodel.event.kind.eventkind.EventKindServiceImpl;
import ai.traceable.fraud.datamodel.event.kind.operator.OperatorServiceImpl;
import ai.traceable.fraud.datamodel.event.kind.transformationfunction.TransformationFunctionServiceImpl;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.grpc.BindableService;
import java.util.List;

/** Factory for creating Event Kind Config Services. */
public class EventKindConfigServiceFactory {

  private EventKindConfigServiceFactory() {
    // Utility class
  }

  /**
   * Builds all Event Kind Config Services.
   *
   * @return list of bindable gRPC services
   */
  public static List<BindableService> build() {
    Injector injector = Guice.createInjector(new EventKindConfigServiceModule());
    return List.of(
        injector.getInstance(EventKindServiceImpl.class),
        injector.getInstance(TransformationFunctionServiceImpl.class),
        injector.getInstance(OperatorServiceImpl.class),
        injector.getInstance(AggregationFunctionServiceImpl.class));
  }
}
