package org.hypertrace.config.objectstore;

import io.grpc.Status;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.Filter;
import org.hypertrace.config.service.v1.GetAllConfigsRequest;
import org.hypertrace.config.service.v1.Pagination;
import org.hypertrace.config.service.v1.SortBy;
import org.hypertrace.core.grpcutils.context.RequestContext;

public abstract class IdentifiedFilterPushedDownObjectStore<T, F, S>
    extends IdentifiedObjectStore<T> {

  private final ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub;
  private final String resourceNamespace;
  private final String resourceName;

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
    this.configServiceBlockingStub = configServiceBlockingStub;
    this.resourceNamespace = resourceNamespace;
    this.resourceName = resourceName;
  }

  public List<ContextualConfigObject<T>> getMatchingObjects(
      RequestContext context, F filterInput, List<S> sortInput, @Nullable Pagination pagination) {
    Filter filter = buildFilter(filterInput);
    List<SortBy> sortByList = sortInput.stream().map(this::buildSort).collect(Collectors.toList());
    return getMatchingObjects(context, filter, sortByList, pagination);
  }

  public List<ContextualConfigObject<T>> getMatchingObjects(
      RequestContext context, F filterInput, List<S> sortInput) {
    return getMatchingObjects(context, filterInput, sortInput, null);
  }

  public List<T> getMatchingData(
      RequestContext context, F filterInput, List<S> sortInput, @Nullable Pagination pagination) {
    return getMatchingObjects(context, filterInput, sortInput, pagination).stream()
        .map(ConfigObject::getData)
        .collect(Collectors.toUnmodifiableList());
  }

  public Optional<ContextualConfigObject<T>> getMatchingObject(
      RequestContext context, F filterInput, List<S> sortInput) {
    List<ContextualConfigObject<T>> results = getMatchingObjects(context, filterInput, sortInput);
    if (results.size() > 1) {
      throw Status.FAILED_PRECONDITION
          .withDescription("Multiple objects found when expecting at most one")
          .asRuntimeException();
    }
    return results.stream().findFirst();
  }

  public Optional<T> getMatchingData(RequestContext context, F filterInput, List<S> sortInput) {
    return getMatchingObject(context, filterInput, sortInput).map(ConfigObject::getData);
  }

  List<ContextualConfigObject<T>> getMatchingObjects(
      RequestContext context, Filter filter, List<SortBy> sortByList, Pagination pagination) {
    return context
        .call(
            () ->
                this.configServiceBlockingStub
                    .withDeadline(getDeadline())
                    .getAllConfigs(buildGetAllConfigsRequest(filter, sortByList, pagination)))
        .getContextSpecificConfigsList()
        .stream()
        .map(
            contextSpecificConfig ->
                ContextualConfigObjectImpl.tryBuild(
                    contextSpecificConfig, this::buildDataFromValue))
        .flatMap(Optional::stream)
        .collect(
            Collectors.collectingAndThen(
                Collectors.toUnmodifiableList(), this::orderFetchedObjects));
  }

  private GetAllConfigsRequest buildGetAllConfigsRequest(
      Filter filter, List<SortBy> sortByList, Pagination pagination) {
    GetAllConfigsRequest.Builder getAllConfigsRequest =
        GetAllConfigsRequest.newBuilder()
            .setResourceName(this.resourceName)
            .setResourceNamespace(this.resourceNamespace)
            .setFilter(filter)
            .addAllSortBy(sortByList);
    if (pagination != null) {
      getAllConfigsRequest.setPagination(pagination);
    }
    return getAllConfigsRequest.build();
  }

  protected abstract SortBy buildSort(S sortInput);

  protected abstract Filter buildFilter(F filterInput);
}
