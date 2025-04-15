package org.hypertrace.config.objectstore;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.Filter;
import org.hypertrace.config.service.v1.Pagination;
import org.hypertrace.config.service.v1.SortBy;
import org.hypertrace.core.grpcutils.context.RequestContext;

public abstract class IdentifiedFilterPushedDownObjectStore<T, F, S>
    extends IdentifiedObjectStore<T> {

  protected IdentifiedFilterPushedDownObjectStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      String resourceNamespace,
      String resourceName,
      ConfigChangeEventGenerator configChangeEventGenerator,
      ClientConfig clientConfig) {
    super(
        configServiceBlockingStub,
        resourceNamespace,
        resourceName,
        configChangeEventGenerator,
        clientConfig);
  }

  public List<ContextualConfigObject<T>> getMatchingObjects(
      RequestContext context, F filterInput, List<S> sortInput, Pagination pagination) {
    Filter filter = buildFilter(filterInput);
    List<SortBy> sortByList = buildSort(sortInput);
    return getMatchingObjectsWithFilter(context, filter, pagination, sortByList);
  }

  public List<T> getMatchingConfigData(
      RequestContext context, F filterInput, List<S> sortInput, Pagination pagination) {
    return getMatchingObjects(context, filterInput, sortInput, pagination).stream()
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

  private Optional<ContextualConfigObject<T>> filterObject(
      ContextualConfigObject<T> configObject, F filter) {
    return filterConfigData(configObject.getData(), filter)
        .map(filteredData -> updateConfigData(configObject, filteredData));
  }

  private ContextualConfigObject<T> updateConfigData(
      ContextualConfigObject<T> configObject, T updatedData) {
    return ((ContextualConfigObjectImpl) configObject).toBuilder().data(updatedData).build();
  }

  protected abstract Optional<T> filterConfigData(T data, F filter);

  protected abstract Filter buildFilter(F filterInput);

  protected abstract List<SortBy> buildSort(List<S> sortInput);
}
