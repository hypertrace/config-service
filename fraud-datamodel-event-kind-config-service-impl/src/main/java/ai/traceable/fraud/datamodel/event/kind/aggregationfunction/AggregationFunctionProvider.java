package ai.traceable.fraud.datamodel.event.kind.aggregationfunction;

import ai.traceable.fraud.datamodel.event.kind.v1.AggregationFunctionsByKind;
import ai.traceable.fraud.datamodel.event.kind.v1.ComplexDataModelEventKind;
import java.util.List;

/**
 * Provider interface for aggregation functions. Allows for future extensibility (e.g., object
 * store).
 */
public interface AggregationFunctionProvider {

  /**
   * Gets aggregation functions grouped by the requested kinds.
   *
   * @param requestedKinds the kinds to get functions for
   * @return list of kind-to-functions mappings
   */
  List<AggregationFunctionsByKind> getFunctionsByKinds(
      List<ComplexDataModelEventKind> requestedKinds);
}
