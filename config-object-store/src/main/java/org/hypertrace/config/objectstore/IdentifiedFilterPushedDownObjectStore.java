package org.hypertrace.config.objectstore;

import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.Filter;
import org.hypertrace.config.service.v1.Pagination;
import org.hypertrace.config.service.v1.SortBy;
import org.hypertrace.core.grpcutils.context.RequestContext;

import java.util.Collections;
import java.util.List;

public abstract class IdentifiedFilterPushedDownObjectStore<T, F> extends IdentifiedObjectStore<T> {

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

  public List<ContextualConfigObject<T>> getAllObjects(
      RequestContext context, F filterInput, Pagination pagination) {
    Filter filter = buildFilter(filterInput);
    List<SortBy> sortByList = buildSortByList(filterInput);
    return getAllObjectsWithFilter(context, filter, pagination, sortByList);
  }

  protected abstract Filter buildFilter(F filterInput);

  /** Constructs a list of SortBy from client filter or returns an empty list. */
  protected List<SortBy> buildSortByList(F filterInput) {
    return Collections.emptyList(); // Override if needed
  }
}
