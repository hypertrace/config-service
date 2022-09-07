package org.hypertrace.config.objectstore;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Resolver class to resolve configs based on scope
 *
 * @param <T>
 * @param <U>
 * @param <S>
 */
public abstract class ConfigObjectScopeResolver<T extends Message, U extends Message, S> {

  protected final Optional<U> defaultConfig;

  public ConfigObjectScopeResolver(U defaultConfig) {
    this.defaultConfig = Optional.of(defaultConfig);
  }

  public ConfigObjectScopeResolver() {
    this.defaultConfig = Optional.empty();
  }

  public List<U> getAllResolvedConfigData(List<T> configDataList) {
    Map<S, T> configDataMap = getConfigDataMap(configDataList);
    return configDataList.stream()
        .flatMap(configData -> resolveConfig(extractScope(configData), configDataMap).stream())
        .collect(Collectors.toUnmodifiableList());
  }

  public Optional<U> getResolvedData(List<T> configDataList, S scope) {
    Map<S, T> configDataMap = getConfigDataMap(configDataList);
    return resolveConfig(scope, configDataMap);
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
   * Method to return object of resolved config type from object store config data
   *
   * @param configStoreData
   * @return
   */
  protected abstract U convertConfig(T configStoreData);

  /**
   * Method to resolve config store object with object of resolved config type
   *
   * @param fallbackConfig
   * @param preferredConfig
   * @return
   */
  protected abstract U resolveConfig(U fallbackConfig, T preferredConfig);

  private Map<S, T> getConfigDataMap(List<T> configDataList) {
    return configDataList.stream()
        .collect(Collectors.toMap(this::extractScope, Function.identity()));
  }

  private Optional<U> resolveConfig(S scope, Map<S, T> configDataMap) {
    Optional<T> mergedConfig =
        getResolutionScopesWithIncreasingPriority(scope).stream()
            .flatMap(scopeObject -> Optional.ofNullable(configDataMap.get(scope)).stream())
            .reduce(
                (fallbackConfig, preferredConfig) ->
                    (T) mergeConfigs(fallbackConfig, preferredConfig));
    return mergedConfig
        .map(
            preferredConfig ->
                defaultConfig
                    .map(fallbackConfig -> resolveConfig(fallbackConfig, preferredConfig))
                    .orElse(convertConfig(preferredConfig)))
        .or(() -> defaultConfig);
  }

  /**
   * Method to merge 2 config objects Should be overridden in case the default merging logic doesn't
   * apply, e.g. configs with arrays
   *
   * @param preferredConfig
   * @param fallbackConfig
   * @return mergedConfig
   */
  public static Message mergeConfigs(Message fallbackConfig, Message preferredConfig) {
    Message.Builder builder = fallbackConfig.toBuilder();
    mergeFromProto(builder, preferredConfig);
    return builder.build();
  }

  /**
   * Custom implementation of merging protos to replace the list of messages rather than appending
   * (which is done in default proto implementation)
   */
  private static void mergeFromProto(Message.Builder builder, Message source) {
    List<Descriptors.FieldDescriptor> fieldDescriptors = builder.getDescriptorForType().getFields();
    for (Descriptors.FieldDescriptor fieldDescriptor : fieldDescriptors) {
      if (fieldDescriptor.isRepeated()) {
        builder.setField(fieldDescriptor, source.getField(fieldDescriptor));
      } else if (source.hasField(fieldDescriptor)) {
        if (fieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
          Message.Builder childBuilder = ((Message) builder.getField(fieldDescriptor)).toBuilder();
          mergeFromProto(childBuilder, ((Message) source.getField(fieldDescriptor)));
          builder.setField(fieldDescriptor, childBuilder.build());
        } else {
          builder.setField(fieldDescriptor, source.getField(fieldDescriptor));
        }
      }
    }
  }
}
