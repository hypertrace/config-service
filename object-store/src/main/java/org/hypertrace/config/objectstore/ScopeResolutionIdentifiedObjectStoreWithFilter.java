package org.hypertrace.config.objectstore;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;

/**
 * ScopeResolutionIdentifiedObjectStoreWithFilter is an abstraction over {@link
 * IdentifiedObjectStoreWithFilter} to allow scope resolution of fetched configs
 *
 * @param <T> Config Object
 * @param <F> Filter
 * @param <S> Scope
 */
public abstract class ScopeResolutionIdentifiedObjectStoreWithFilter<T extends Message, F, S>
    extends IdentifiedObjectStoreWithFilter<T, F> {

  protected final T defaultConfig;

  protected ScopeResolutionIdentifiedObjectStoreWithFilter(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      String resourceNamespace,
      String resourceName,
      ConfigChangeEventGenerator configChangeEventGenerator,
      T defaultConfig) {
    super(configServiceBlockingStub, resourceNamespace, resourceName, configChangeEventGenerator);
    this.defaultConfig = defaultConfig;
  }

  public T getDefaultConfig() {
    return defaultConfig;
  }

  public List<ContextualConfigObject<T>> getAllFilteredResolvedObjects(
      RequestContext context, F filter) {
    Map<S, ContextualConfigObject<T>> configObjectsMap = getConfigObjectsMap(context, filter);
    return configObjectsMap.values().stream()
        .map(
            configObject ->
                ContextualConfigObjectImpl.updateData(
                    configObject,
                    resolveConfig(extractScope(configObject.getData()), configObjectsMap)))
        .collect(Collectors.toUnmodifiableList());
  }

  public List<T> getAllFilteredResolvedConfigData(RequestContext context, F filter) {
    return getAllFilteredResolvedObjects(context, filter).stream()
        .map(ConfigObject::getData)
        .collect(Collectors.toUnmodifiableList());
  }

  public Optional<ContextualConfigObject<T>> getFilteredResolvedObject(
      RequestContext context, F filter, S scope) {
    Map<S, ContextualConfigObject<T>> configObjectsMap = getConfigObjectsMap(context, filter);
    return Optional.ofNullable(configObjectsMap.get(scope))
        .map(
            configObject ->
                ContextualConfigObjectImpl.updateData(
                    configObject, resolveConfig(scope, configObjectsMap)));
  }

  public Optional<T> getFilteredResolvedData(RequestContext context, F filter, S scope) {
    return getFilteredResolvedObject(context, filter, scope).map(ConfigObject::getData);
  }

  /**
   * Method to extract scope object from given config object
   *
   * @param configObject
   * @return Scope object
   */
  protected abstract S extractScope(T configObject);

  /**
   * Method to return scopes with which the given scope needs to be resolved in increasing order of
   * priority For example, API scope would return a list {customer, env, service, api}
   *
   * @param scopeObject
   * @return list of associated scopes in increasing order of priority
   */
  protected abstract List<S> getResolutionScopesWithIncreasingPriority(S scopeObject);

  /**
   * Method to merge 2 config objects Should be overridden in case the default merging logic doesn't
   * apply, e.g. configs with arrays
   *
   * @param preferredConfig
   * @param fallbackConfig
   * @return mergedConfig
   */
  protected T mergeConfigs(T fallbackConfig, T preferredConfig) {
    T.Builder builder = fallbackConfig.toBuilder();
    mergeFromProto(builder, preferredConfig);
    return (T) builder.build();
  }

  /**
   * Custom implementation of merging protos to replace the list of messages rather than appending
   * (which is done in default proto implementation)
   */
  protected void mergeFromProto(T.Builder builder, T source) {
    List<Descriptors.FieldDescriptor> fieldDescriptors = builder.getDescriptorForType().getFields();
    for (Descriptors.FieldDescriptor fieldDescriptor : fieldDescriptors) {
      if (fieldDescriptor.isRepeated()) {
        builder.setField(fieldDescriptor, source.getField(fieldDescriptor));
      } else if (source.hasField(fieldDescriptor)) {
        if (fieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
          Message.Builder childBuilder = ((T) builder.getField(fieldDescriptor)).toBuilder();
          mergeFromProto(childBuilder, (T) source.getField(fieldDescriptor));
          builder.setField(fieldDescriptor, childBuilder.build());
        } else {
          builder.setField(fieldDescriptor, source.getField(fieldDescriptor));
        }
      }
    }
  }

  private Map<S, ContextualConfigObject<T>> getConfigObjectsMap(RequestContext context, F filter) {
    List<ContextualConfigObject<T>> configObjects = getAllFilteredObjects(context, filter);
    return configObjects.stream()
        .collect(
            Collectors.toMap(
                configObject -> extractScope(configObject.getData()), Function.identity()));
  }

  private T resolveConfig(S scope, Map<S, ContextualConfigObject<T>> configObjectsMap) {
    return getResolutionScopesWithIncreasingPriority(scope).stream()
        .flatMap(scopeObject -> Optional.ofNullable(configObjectsMap.get(scope).getData()).stream())
        .reduce(defaultConfig, this::mergeConfigs);
  }
}
