package org.hypertrace.config.objectstore;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import java.util.List;
import java.util.Map;
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

  protected ScopeResolutionIdentifiedObjectStoreWithFilter(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      String resourceNamespace,
      String resourceName,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(configServiceBlockingStub, resourceNamespace, resourceName, configChangeEventGenerator);
  }

  public List<T> getAllFilteredResolvedConfigs(RequestContext context, F filter, T defaultConfig) {
    Map<S, T> configObjectsMap =
        getAllFilteredConfigs(context, filter).stream()
            .collect(Collectors.toMap(this::extractScope, Function.identity()));

    return configObjectsMap.entrySet().stream()
        .map(
            entry -> resolveConfig(extractScope(entry.getValue()), defaultConfig, configObjectsMap))
        .collect(Collectors.toUnmodifiableList());
  }

  public T getFilteredResolvedConfig(RequestContext context, F filter, S scope, T defaultConfig) {
    List<T> filteredConfigObjects = getAllFilteredConfigs(context, filter);
    Map<S, T> configObjectsMap =
        filteredConfigObjects.stream()
            .collect(Collectors.toMap(this::extractScope, Function.identity()));
    return resolveConfig(scope, defaultConfig, configObjectsMap);
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
  protected T mergeConfigs(T preferredConfig, T fallbackConfig) {
    T.Builder builder = fallbackConfig.toBuilder();
    mergeFromProto(builder, preferredConfig);
    return (T) builder.build();
  }

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

  private T resolveConfig(S scope, T defaultConfig, Map<S, T> configObjectsMap) {
    List<S> scopesWithIncreasingPriority = getResolutionScopesWithIncreasingPriority(scope);
    T resultConfig = defaultConfig;
    for (S scopeObject : scopesWithIncreasingPriority) {
      if (configObjectsMap.containsKey(scopeObject)) {
        resultConfig = mergeConfigs(configObjectsMap.get(scopeObject), resultConfig);
      }
    }
    return resultConfig;
  }
}
