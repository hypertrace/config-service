package ai.traceable.fraud.datamodel.event.kind.operator;

import ai.traceable.fraud.datamodel.event.kind.v1.ComplexDataModelEventKind;
import ai.traceable.fraud.datamodel.event.kind.v1.Operator;
import ai.traceable.fraud.datamodel.event.kind.v1.OperatorsByKind;
import java.util.List;

/** Provider interface for operators. Allows for future extensibility (e.g., object store). */
public interface OperatorProvider {

  /**
   * Gets operators grouped by the requested kinds.
   *
   * @param requestedKinds the kinds to get operators for
   * @return list of kind-to-operators mappings
   */
  List<OperatorsByKind> getOperatorsByKinds(List<ComplexDataModelEventKind> requestedKinds);

  /**
   * Gets all operators without filtering by kind.
   *
   * @return list of all available operators
   */
  List<Operator> getAllOperators();
}
