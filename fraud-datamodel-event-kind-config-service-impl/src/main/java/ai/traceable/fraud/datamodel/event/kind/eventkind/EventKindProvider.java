package ai.traceable.fraud.datamodel.event.kind.eventkind;

import ai.traceable.fraud.datamodel.event.kind.v1.DataModelEventKind;
import ai.traceable.fraud.datamodel.event.kind.v1.EventKindFilter;
import java.util.List;

/** Provider interface for event kinds. Allows for future extensibility (e.g., object store). */
public interface EventKindProvider {

  /**
   * Gets all event kinds matching the filter.
   *
   * @param filter optional filter criteria
   * @return list of event kinds
   */
  List<DataModelEventKind> getEventKinds(EventKindFilter filter);
}
