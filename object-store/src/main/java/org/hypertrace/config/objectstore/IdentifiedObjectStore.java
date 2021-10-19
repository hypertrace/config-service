package org.hypertrace.config.objectstore;

import com.google.protobuf.Value;
import io.grpc.Status;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.config.service.v1.ContextSpecificConfig;
import org.hypertrace.config.service.v1.DeleteConfigRequest;
import org.hypertrace.config.service.v1.GetAllConfigsRequest;
import org.hypertrace.config.service.v1.GetConfigRequest;
import org.hypertrace.config.service.v1.UpsertAllConfigsRequest;
import org.hypertrace.config.service.v1.UpsertAllConfigsRequest.ConfigToUpsert;
import org.hypertrace.config.service.v1.UpsertAllConfigsResponse.UpsertedConfig;
import org.hypertrace.config.service.v1.UpsertConfigRequest;
import org.hypertrace.config.service.v1.UpsertConfigResponse;
import org.hypertrace.core.grpcutils.context.RequestContext;

/**
 * IdentifiedObjectStore is an abstraction over the grpc api for config implementations working with
 * multiple objects with unique identifiers
 *
 * @param <T>
 */
public abstract class IdentifiedObjectStore<T> {
  private final ConfigServiceBlockingStub configServiceBlockingStub;
  private final String resourceNamespace;
  private final String resourceName;
  private final Optional<ConfigChangeEventGenerator> configChangeEventGeneratorOptional;

  protected IdentifiedObjectStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      String resourceNamespace,
      String resourceName,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    this.configServiceBlockingStub = configServiceBlockingStub;
    this.resourceNamespace = resourceNamespace;
    this.resourceName = resourceName;
    this.configChangeEventGeneratorOptional = Optional.of(configChangeEventGenerator);
  }

  protected IdentifiedObjectStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      String resourceNamespace,
      String resourceName) {
    this.configServiceBlockingStub = configServiceBlockingStub;
    this.resourceNamespace = resourceNamespace;
    this.resourceName = resourceName;
    this.configChangeEventGeneratorOptional = Optional.empty();
  }

  protected abstract Optional<T> buildObjectFromValue(Value value);

  protected abstract Value buildValueFromObject(T object);

  protected abstract String getContextFromObject(T object);

  protected List<T> orderFetchedObjects(List<T> objects) {
    return objects;
  }

  public List<T> getAllObjects(RequestContext context) {
    return context
        .call(
            () ->
                this.configServiceBlockingStub.getAllConfigs(
                    GetAllConfigsRequest.newBuilder()
                        .setResourceName(this.resourceName)
                        .setResourceNamespace(this.resourceNamespace)
                        .build()))
        .getContextSpecificConfigsList()
        .stream()
        .map(ContextSpecificConfig::getConfig)
        .map(this::buildObjectFromValue)
        .flatMap(Optional::stream)
        .collect(
            Collectors.collectingAndThen(
                Collectors.toUnmodifiableList(), this::orderFetchedObjects));
  }

  public Optional<T> getObject(RequestContext context, String id) {
    try {
      Value value =
          context.call(
              () ->
                  this.configServiceBlockingStub
                      .getConfig(
                          GetConfigRequest.newBuilder()
                              .setResourceName(this.resourceName)
                              .setResourceNamespace(this.resourceNamespace)
                              .addContexts(id)
                              .build())
                      .getConfig());
      T object = this.buildObjectFromValue(value).orElseThrow(Status.INTERNAL::asRuntimeException);
      return Optional.of(object);
    } catch (Exception exception) {
      if (Status.fromThrowable(exception).equals(Status.NOT_FOUND)) {
        return Optional.empty();
      }
      throw exception;
    }
  }

  public T upsertObject(RequestContext context, T object) {
    UpsertConfigResponse response =
        context.call(
            () ->
                this.configServiceBlockingStub.upsertConfig(
                    UpsertConfigRequest.newBuilder()
                        .setResourceName(this.resourceName)
                        .setResourceNamespace(this.resourceNamespace)
                        .setContext(this.getContextFromObject(object))
                        .setConfig(this.buildValueFromObject(object))
                        .build()));

    return this.processUpsertResult(context, response)
        .orElseThrow(Status.INTERNAL::asRuntimeException);
  }

  public Optional<T> deleteObject(RequestContext context, String id) {
    try {
      Value deletedValue =
          context
              .call(
                  () ->
                      this.configServiceBlockingStub.deleteConfig(
                          DeleteConfigRequest.newBuilder()
                              .setResourceName(this.resourceName)
                              .setResourceNamespace(this.resourceNamespace)
                              .setContext(id)
                              .build()))
              .getDeletedConfig()
              .getConfig();
      T object =
          this.buildObjectFromValue(deletedValue).orElseThrow(Status.INTERNAL::asRuntimeException);
      configChangeEventGeneratorOptional.ifPresent(
          configChangeEventGenerator ->
              configChangeEventGenerator.sendDeleteNotification(
                  context, object.getClass().getName(), id, deletedValue));
      return Optional.of(object);
    } catch (Exception exception) {
      if (Status.fromThrowable(exception).equals(Status.NOT_FOUND)) {
        return Optional.empty();
      }
      throw exception;
    }
  }

  public List<T> upsertObjects(RequestContext context, List<T> objects) {
    List<ConfigToUpsert> configs =
        objects.stream()
            .map(
                object ->
                    ConfigToUpsert.newBuilder()
                        .setResourceName(this.resourceName)
                        .setResourceNamespace(this.resourceNamespace)
                        .setContext(this.getContextFromObject(object))
                        .setConfig(this.buildValueFromObject(object))
                        .build())
            .collect(Collectors.toUnmodifiableList());

    return context
        .call(
            () ->
                this.configServiceBlockingStub.upsertAllConfigs(
                    UpsertAllConfigsRequest.newBuilder().addAllConfigs(configs).build()))
        .getUpsertedConfigsList()
        .stream()
        .map(upsertedConfig -> this.processUpsertResult(context, upsertedConfig))
        .flatMap(Optional::stream)
        .collect(Collectors.toUnmodifiableList());
  }

  private Optional<T> processUpsertResult(
      RequestContext requestContext, UpsertConfigResponse response) {
    if (response.hasPrevConfig()) {
      return this.processUpdateResult(
          requestContext, response.getConfig(), response.getPrevConfig());
    }
    return this.processCreateResult(requestContext, response.getConfig());
  }

  private Optional<T> processUpsertResult(
      RequestContext requestContext, UpsertedConfig upsertedConfig) {
    if (upsertedConfig.hasPrevConfig()) {
      return this.processUpdateResult(
          requestContext, upsertedConfig.getConfig(), upsertedConfig.getPrevConfig());
    }
    return this.processCreateResult(requestContext, upsertedConfig.getConfig());
  }

  private Optional<T> processUpdateResult(
      RequestContext requestContext, Value updatedValue, Value previousValue) {
    Optional<T> parsedResult = this.buildObjectFromValue(updatedValue);
    parsedResult.ifPresent(
        updatedObject ->
            configChangeEventGeneratorOptional.ifPresent(
                configChangeEventGenerator ->
                    configChangeEventGenerator.sendUpdateNotification(
                        requestContext,
                        updatedObject.getClass().getName(),
                        this.getContextFromObject(updatedObject),
                        previousValue,
                        updatedValue)));
    return parsedResult;
  }

  private Optional<T> processCreateResult(RequestContext requestContext, Value newValue) {
    Optional<T> parsedResult = this.buildObjectFromValue(newValue);
    parsedResult.ifPresent(
        newObject ->
            configChangeEventGeneratorOptional.ifPresent(
                configChangeEventGenerator ->
                    configChangeEventGenerator.sendCreateNotification(
                        requestContext,
                        newObject.getClass().getName(),
                        this.getContextFromObject(newObject),
                        newValue)));
    return parsedResult;
  }
}
