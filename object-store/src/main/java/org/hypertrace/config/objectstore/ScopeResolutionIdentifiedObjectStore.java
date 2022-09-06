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
public abstract class ScopeResolutionIdentifiedObjectStore<T extends Message, F, S>
    extends IdentifiedObjectStoreWithFilter<T, F> {

  protected final T defaultConfig;

  protected ScopeResolutionIdentifiedObjectStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      String resourceNamespace,
      String resourceName,
      ConfigChangeEventGenerator configChangeEventGenerator,
      T defaultConfig) {
    super(configServiceBlockingStub, resourceNamespace, resourceName, configChangeEventGenerator);
    this.defaultConfig = defaultConfig;
  }

  public List<T> getAllResolvedConfigData(RequestContext context, F filter) {
    Map<S, T> configDataMap = getConfigDataMap(context, filter);
    return configDataMap.values().stream()
        .map(configData -> resolveConfig(extractScope(configData), configDataMap))
        .collect(Collectors.toUnmodifiableList());
  }

  public List<T> getAllResolvedConfigData(RequestContext context) {
    Map<S, T> configDataMap = getConfigDataMap(context);
    return configDataMap.values().stream()
        .map(configData -> resolveConfig(extractScope(configData), configDataMap))
        .collect(Collectors.toUnmodifiableList());
  }

  public Optional<T> getResolvedData(RequestContext context, S scope, F filter) {
    Map<S, T> configDataMap = getConfigDataMap(context, filter);
    return Optional.ofNullable(configDataMap.get(scope))
        .map(configData -> resolveConfig(scope, configDataMap));
  }

  public Optional<T> getResolvedData(RequestContext context, S scope) {
    Map<S, T> configDataMap = getConfigDataMap(context);
    return Optional.ofNullable(configDataMap.get(scope))
        .map(configData -> resolveConfig(scope, configDataMap));
  }

  /**
   * Method to extract scope object from given config object
   *
   * @param configData
   * @return Scope object
   */
  protected abstract S extractScope(T configData);

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

  private Map<S, T> getConfigDataMap(RequestContext context, F filter) {
    List<T> configData = getAllFilteredConfigData(context, filter);
    return configData.stream().collect(Collectors.toMap(this::extractScope, Function.identity()));
  }

  private Map<S, T> getConfigDataMap(RequestContext context) {
    List<T> configData = getAllConfigData(context);
    return configData.stream().collect(Collectors.toMap(this::extractScope, Function.identity()));
  }

  private T resolveConfig(S scope, Map<S, T> configDataMap) {
    return getResolutionScopesWithIncreasingPriority(scope).stream()
        .flatMap(scopeObject -> Optional.ofNullable(configDataMap.get(scope)).stream())
        .reduce(defaultConfig, this::mergeConfigs);
  }
}
