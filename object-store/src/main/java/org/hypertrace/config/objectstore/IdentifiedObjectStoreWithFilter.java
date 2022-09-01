package org.hypertrace.config.objectstore;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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

  public List<T> getAllFilteredConfigs(RequestContext context, F filter) {
    Stream<T> configObjectsStream = getAllObjects(context).stream().map(ConfigObject::getData);
    if (!isFilterEmpty(filter)) {
      configObjectsStream =
          configObjectsStream
              .map(configObject -> filterConfig(configObject, filter))
              .filter(Optional::isPresent)
              .map(Optional::get);
    }
    return configObjectsStream.collect(Collectors.toUnmodifiableList());
  }

  public Optional<T> getFilteredConfig(RequestContext context, String contextId, F filter) {
    Optional<T> fetchedConfig = getData(context, contextId);
    if (!isFilterEmpty(filter)) {
      return fetchedConfig.flatMap(config -> filterConfig(config, filter));
    }
    return fetchedConfig;
  }

  /**
   * Method to obtain filtered config A config object can consist of multiple configs, e.g. array,
   * which can be partially filtered and returned. In cases where filter applies to entire config
   * object, Optional.empty() is returned
   *
   * @param config
   * @param filter
   * @return
   */
  protected abstract Optional<T> filterConfig(T config, F filter);

  /**
   * Method to check if any filter confitions exist at all
   *
   * @param filter
   * @return
   */
  protected abstract boolean isFilterEmpty(F filter);
}
