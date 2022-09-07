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

  public List<ContextualConfigObject<T>> getAllObjects(RequestContext context, F filter) {
    return getAllObjects(context).stream()
        .flatMap(configObject -> filterObject(configObject, filter).stream())
        .collect(Collectors.toUnmodifiableList());
  }

  public List<T> getAllConfigData(RequestContext context, F filter) {
    return getAllObjects(context, filter).stream()
        .map(ConfigObject::getData)
        .collect(Collectors.toUnmodifiableList());
  }

  public Optional<ContextualConfigObject<T>> getObject(
      RequestContext context, String id, F filter) {
    return getObject(context, id).flatMap(configObject -> filterObject(configObject, filter));
  }

  public Optional<T> getData(RequestContext context, String id, F filter) {
    return getObject(context, id, filter).map(ConfigObject::getData);
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
        .map(filteredData -> updateConfigData(configObject, filteredData));
  }

  private ContextualConfigObject<T> updateConfigData(
      ContextualConfigObject<T> configObject, T updatedData) {
    return ((ContextualConfigObjectImpl) configObject).toBuilder().data(updatedData).build();
  }
}
