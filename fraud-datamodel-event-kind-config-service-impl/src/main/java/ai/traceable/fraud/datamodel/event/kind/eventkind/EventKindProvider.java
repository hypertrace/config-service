package ai.traceable.fraud.datamodel.event.kind.eventkind;

import ai.traceable.fraud.datamodel.event.kind.v1.DataModelEventKind;
import ai.traceable.fraud.datamodel.event.kind.v1.EventKindFilter;
import java.util.List;

/** Provider interface for event kinds. Allows for future extensibility (e.g., object store). */
public interface EventKindProvider {

  /**
   * Gets selectable event kinds matching the filter. Root/internal kinds (e.g., the abstract
   * "value" kind) are excluded since they have no operators and should not be assigned to
   * attributes.
   *
   * @param filter optional filter criteria
   * @return list of selectable event kinds
   */
  List<DataModelEventKind> getEventKinds(EventKindFilter filter);

  /**
   * Gets all event kinds including internal root types. Used by hierarchy resolver to build the
   * full type tree. Default implementation delegates to getEventKinds with no filter.
   *
   * @return list of all event kinds
   */
  default List<DataModelEventKind> getAllEventKinds() {
    return getEventKinds(EventKindFilter.getDefaultInstance());
  }
}
