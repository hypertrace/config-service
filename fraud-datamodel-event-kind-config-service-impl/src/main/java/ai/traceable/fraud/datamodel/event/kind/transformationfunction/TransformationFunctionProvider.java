package ai.traceable.fraud.datamodel.event.kind.transformationfunction;

import ai.traceable.fraud.datamodel.event.kind.v1.ComplexDataModelEventKind;
import ai.traceable.fraud.datamodel.event.kind.v1.TransformationFunctionsByKind;
import java.util.List;

/**
 * Provider interface for transformation functions. Allows for future extensibility (e.g., object
 * store).
 */
public interface TransformationFunctionProvider {

  /**
   * Gets transformation functions grouped by the requested kinds.
   *
   * @param requestedKinds the kinds to get functions for
   * @return list of kind-to-functions mappings
   */
  List<TransformationFunctionsByKind> getFunctionsByKinds(
      List<ComplexDataModelEventKind> requestedKinds);

  /**
   * Gets all transformation functions grouped by their input kinds.
   *
   * @return list of kind-to-functions mappings for all available functions
   */
  List<TransformationFunctionsByKind> getAllFunctions();
}
