package org.hypertrace.config.objectstore;

import com.google.protobuf.Value;
import io.grpc.Status;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.config.service.v1.ContextSpecificConfig;
import org.hypertrace.config.service.v1.DeleteConfigRequest;
import org.hypertrace.config.service.v1.DeleteConfigsRequest;
import org.hypertrace.config.service.v1.DeleteConfigsRequest.ConfigToDelete;
import org.hypertrace.config.service.v1.GetAllConfigsRequest;
import org.hypertrace.config.service.v1.GetConfigRequest;
import org.hypertrace.config.service.v1.GetConfigResponse;
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
@Slf4j
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

  protected abstract Optional<T> buildDataFromValue(Value value);

  protected abstract Value buildValueFromData(T data);

  protected abstract String getContextFromData(T data);

  protected Value buildValueForChangeEvent(T data) {
    return this.buildValueFromData(data);
  }

  protected String buildClassNameForChangeEvent(T data) {
    return data.getClass().getName();
  }

  protected List<ContextualConfigObject<T>> orderFetchedObjects(
      List<ContextualConfigObject<T>> objects) {
    return objects;
  }

  public List<ContextualConfigObject<T>> getAllObjects(RequestContext context) {
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
        .map(
            contextSpecificConfig -> {
              ContextualConfigObject<T> contextualConfigObject =
                  ContextualConfigObjectImpl.tryBuild(
                      contextSpecificConfig, this::buildDataFromValue);
              if (contextualConfigObject.getData().isEmpty()) {
                throw Status.INTERNAL
                    .withDescription(
                        "Could not convert config: "
                            + contextSpecificConfig.getConfig()
                            + " into proto")
                    .asRuntimeException(context.buildTrailers());
              }
              return contextualConfigObject;
            })
        .collect(
            Collectors.collectingAndThen(
                Collectors.toUnmodifiableList(), this::orderFetchedObjects));
  }

  public List<T> getAllConfigData(RequestContext context) {
    return getAllObjects(context).stream()
        .map(ConfigObject::getData)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toUnmodifiableList());
  }

  public Optional<ContextualConfigObject<T>> getObject(RequestContext context, String id) {
    try {
      GetConfigResponse getConfigResponse =
          context.call(
              () ->
                  this.configServiceBlockingStub.getConfig(
                      GetConfigRequest.newBuilder()
                          .setResourceName(this.resourceName)
                          .setResourceNamespace(this.resourceNamespace)
                          .addContexts(id)
                          .build()));

      ContextSpecificConfig contextSpecificConfig =
          ContextSpecificConfig.newBuilder()
              .setContext(id)
              .setConfig(getConfigResponse.getConfig())
              .setCreationTimestamp(getConfigResponse.getCreationTimestamp())
              .setUpdateTimestamp(getConfigResponse.getUpdateTimestamp())
              .build();
      return Optional.of(
          ContextualConfigObjectImpl.tryBuild(contextSpecificConfig, this::buildDataFromValue));
    } catch (Exception exception) {
      if (Status.fromThrowable(exception).equals(Status.NOT_FOUND)) {
        return Optional.empty();
      }
      throw exception;
    }
  }

  public Optional<T> getData(RequestContext context, String id) {
    Optional<Optional<T>> object = getObject(context, id).map(ConfigObject::getData);
    if (object.isEmpty()) {
      return Optional.empty();
    }
    return object.get();
  }

  public ContextualConfigObject<T> upsertObject(RequestContext context, T data) {
    UpsertConfigResponse response =
        context.call(
            () ->
                this.configServiceBlockingStub.upsertConfig(
                    UpsertConfigRequest.newBuilder()
                        .setResourceName(this.resourceName)
                        .setResourceNamespace(this.resourceNamespace)
                        .setContext(this.getContextFromData(data))
                        .setConfig(this.buildValueFromData(data))
                        .build()));

    return this.processUpsertResult(context, response);
  }

  public Optional<DeletedContextualConfigObject<T>> deleteObject(
      RequestContext requestContext, String context) {
    try {
      ContextSpecificConfig deletedConfig =
          requestContext
              .call(
                  () ->
                      this.configServiceBlockingStub.deleteConfig(
                          DeleteConfigRequest.newBuilder()
                              .setResourceName(this.resourceName)
                              .setResourceNamespace(this.resourceNamespace)
                              .setContext(context)
                              .build()))
              .getDeletedConfig();
      return Optional.of(processDeleteResult(requestContext, deletedConfig));
    } catch (Exception exception) {
      if (Status.fromThrowable(exception).equals(Status.NOT_FOUND)) {
        return Optional.empty();
      }
      throw exception;
    }
  }

  public List<ContextualConfigObject<T>> upsertObjects(RequestContext context, List<T> data) {
    List<ConfigToUpsert> configs =
        data.stream()
            .map(
                singleData ->
                    ConfigToUpsert.newBuilder()
                        .setResourceName(this.resourceName)
                        .setResourceNamespace(this.resourceNamespace)
                        .setContext(this.getContextFromData(singleData))
                        .setConfig(this.buildValueFromData(singleData))
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
        .collect(Collectors.toUnmodifiableList());
  }

  public List<DeletedContextualConfigObject<T>> deleteObjects(
      RequestContext requestContext, List<String> contexts) {
    List<ConfigToDelete> configsToDelete =
        contexts.stream()
            .map(
                context ->
                    ConfigToDelete.newBuilder()
                        .setResourceName(this.resourceName)
                        .setResourceNamespace(this.resourceNamespace)
                        .setContext(context)
                        .build())
            .collect(Collectors.toUnmodifiableList());
    return requestContext
        .call(
            () ->
                this.configServiceBlockingStub.deleteConfigs(
                    DeleteConfigsRequest.newBuilder().addAllConfigs(configsToDelete).build()))
        .getDeletedConfigsList()
        .stream()
        .map(deletedConfig -> processDeleteResult(requestContext, deletedConfig))
        .collect(Collectors.toUnmodifiableList());
  }

  private ContextualConfigObject<T> processUpsertResult(
      RequestContext requestContext, UpsertConfigResponse response) {
    ContextualConfigObject<T> result =
        ContextualConfigObjectImpl.tryBuild(
            response, this::buildDataFromValue, this::getContextFromData);

    if (result.getData().isEmpty()) {
      throw Status.INTERNAL
          .withDescription("Could not convert upserted config into corresponding proto")
          .asRuntimeException(requestContext.buildTrailers());
    }

    if (response.hasPrevConfig()) {
      tryReportUpdate(requestContext, result, response.getPrevConfig());
    } else {
      tryReportCreation(requestContext, result);
    }

    return result;
  }

  private ContextualConfigObject<T> processUpsertResult(
      RequestContext requestContext, UpsertedConfig upsertedConfig) {
    ContextualConfigObject<T> result =
        ContextualConfigObjectImpl.tryBuild(upsertedConfig, this::buildDataFromValue);
    if (result.getData().isEmpty()) {
      throw Status.INTERNAL
          .withDescription("Could not convert upserted config into corresponding proto")
          .asRuntimeException(requestContext.buildTrailers());
    }

    if (upsertedConfig.hasPrevConfig()) {
      tryReportUpdate(requestContext, result, upsertedConfig.getPrevConfig());
    } else {
      tryReportCreation(requestContext, result);
    }
    return result;
  }

  private DeletedContextualConfigObject<T> processDeleteResult(
      RequestContext requestContext, ContextSpecificConfig deletedConfig) {
    DeletedContextualConfigObject<T> deletedContextualConfigObject =
        DeletedContextualConfigObjectImpl.tryBuild(deletedConfig, this::buildDataFromValue);

    if (deletedContextualConfigObject.getOptionalData().isPresent()) {
      T data = deletedContextualConfigObject.getOptionalData().get();
      configChangeEventGeneratorOptional.ifPresent(
          configChangeEventGenerator ->
              configChangeEventGenerator.sendDeleteNotification(
                  requestContext,
                  this.buildClassNameForChangeEvent(data),
                  deletedConfig.getContext(),
                  this.buildValueForChangeEvent(data)));
    }
    return deletedContextualConfigObject;
  }

  private void tryReportCreation(RequestContext requestContext, ContextualConfigObject<T> result) {
    configChangeEventGeneratorOptional.ifPresent(
        configChangeEventGenerator ->
            configChangeEventGenerator.sendCreateNotification(
                requestContext,
                this.buildClassNameForChangeEvent(result.getData().get()),
                result.getContext(),
                this.buildValueForChangeEvent(result.getData().get())));
  }

  private void tryReportUpdate(
      RequestContext requestContext, ContextualConfigObject<T> result, Value previousValue) {
    configChangeEventGeneratorOptional.ifPresent(
        configChangeEventGenerator ->
            configChangeEventGenerator.sendUpdateNotification(
                requestContext,
                this.buildClassNameForChangeEvent(result.getData().get()),
                result.getContext(),
                this.buildDataFromValue(previousValue)
                    .map(this::buildValueForChangeEvent)
                    .orElseGet(
                        () -> {
                          log.error(
                              "Unable to convert previousValue back to data for change event. Falling back to raw value {}",
                              previousValue);
                          return previousValue;
                        }),
                this.buildValueForChangeEvent(result.getData().get())));
  }
}
