package org.hypertrace.config.objectstore;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;

/**
 * IdentifiedObjectStoreWithFilter is an abstraction over {@link IdentifiedObjectStore} to allow
 * filtering on fetched configs
 *
 * @param <T> Config Object
 * @param <F> Filter
 */
public abstract class IdentifiedObjectStoreWithFilter<T, F> extends IdentifiedObjectStore<T> {
  protected IdentifiedObjectStoreWithFilter(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      String resourceNamespace,
      String resourceName,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(configServiceBlockingStub, resourceNamespace, resourceName, configChangeEventGenerator);
  }

  public List<ContextualConfigObject<T>> getAllFilteredObjects(RequestContext context, F filter) {
    return getAllObjects(context).stream()
        .flatMap(configObject -> filterObject(configObject, filter).stream())
        .collect(Collectors.toUnmodifiableList());
  }

  public List<T> getAllFilteredConfigData(RequestContext context, F filter) {
    return getAllFilteredObjects(context, filter).stream()
        .map(ConfigObject::getData)
        .collect(Collectors.toUnmodifiableList());
  }

  public Optional<ContextualConfigObject<T>> getFilteredObject(
      RequestContext context, String contextId, F filter) {
    return getObject(context, contextId)
        .flatMap(configObject -> filterObject(configObject, filter));
  }

  public Optional<T> getFilteredData(RequestContext context, String id, F filter) {
    return getFilteredObject(context, id, filter).map(ConfigObject::getData);
  }

  /**
   * Method to obtain filtered config A config object can consist of multiple configs, e.g. array,
   * which can be partially filtered and returned. In cases where filter applies to entire config
   * object, Optional.empty() is returned
   *
   * @param data
   * @param filter
   * @return
   */
  protected abstract Optional<T> filterConfigData(T data, F filter);

  private Optional<ContextualConfigObject<T>> filterObject(
      ContextualConfigObject<T> configObject, F filter) {
    return filterConfigData(configObject.getData(), filter)
        .map(filteredData -> ContextualConfigObjectImpl.updateData(configObject, filteredData));
  }
}
