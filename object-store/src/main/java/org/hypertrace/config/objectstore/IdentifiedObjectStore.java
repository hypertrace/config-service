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

  protected abstract Optional<T> buildDataFromValue(Value value);

  protected abstract Value buildValueFromData(T data);

  protected abstract String getContextFromData(T data);

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
            contextSpecificConfig ->
                ContextualConfigObjectImpl.tryBuild(
                    contextSpecificConfig, this::buildDataFromValue))
        .flatMap(Optional::stream)
        .collect(
            Collectors.collectingAndThen(
                Collectors.toUnmodifiableList(), this::orderFetchedObjects));
  }

  public Optional<T> getData(RequestContext context, String id) {
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
      T object = this.buildDataFromValue(value).orElseThrow(Status.INTERNAL::asRuntimeException);
      return Optional.of(object);
    } catch (Exception exception) {
      if (Status.fromThrowable(exception).equals(Status.NOT_FOUND)) {
        return Optional.empty();
      }
      throw exception;
    }
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

    return this.processUpsertResult(context, response)
        .orElseThrow(Status.INTERNAL::asRuntimeException);
  }

  public Optional<ContextualConfigObject<T>> deleteObject(RequestContext context, String id) {
    try {
      ContextSpecificConfig deletedConfig =
          context
              .call(
                  () ->
                      this.configServiceBlockingStub.deleteConfig(
                          DeleteConfigRequest.newBuilder()
                              .setResourceName(this.resourceName)
                              .setResourceNamespace(this.resourceNamespace)
                              .setContext(id)
                              .build()))
              .getDeletedConfig();
      ContextualConfigObject<T> object =
          ContextualConfigObjectImpl.tryBuild(deletedConfig, this::buildDataFromValue)
              .orElseThrow(Status.INTERNAL::asRuntimeException);
      sendDeleteNotification(
          context, object.getData().getClass().getName(), id, deletedConfig.getConfig());
      return Optional.of(object);
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
        .flatMap(Optional::stream)
        .collect(Collectors.toUnmodifiableList());
  }

  private Optional<ContextualConfigObject<T>> processUpsertResult(
      RequestContext requestContext, UpsertConfigResponse response) {
    Optional<ContextualConfigObject<T>> optionalResult =
        ContextualConfigObjectImpl.tryBuild(
            response, this::buildDataFromValue, this::getContextFromData);
    optionalResult.ifPresent(
        result -> {
          if (response.hasPrevConfig()) {
            tryReportUpdate(requestContext, result, response.getPrevConfig());
          } else {
            tryReportCreation(requestContext, result);
          }
        });
    return optionalResult;
  }

  private Optional<ContextualConfigObject<T>> processUpsertResult(
      RequestContext requestContext, UpsertedConfig upsertedConfig) {
    Optional<ContextualConfigObject<T>> optionalResult =
        ContextualConfigObjectImpl.tryBuild(upsertedConfig, this::buildDataFromValue);

    optionalResult.ifPresent(
        result -> {
          if (upsertedConfig.hasPrevConfig()) {
            tryReportUpdate(requestContext, result, upsertedConfig.getPrevConfig());
          } else {
            tryReportCreation(requestContext, result);
          }
        });
    return optionalResult;
  }

  private void tryReportCreation(RequestContext requestContext, ContextualConfigObject<T> result) {
    sendCreateNotification(requestContext, result);
  }

  private void tryReportUpdate(
      RequestContext requestContext, ContextualConfigObject<T> result, Value previousValue) {
    sendUpdateNotification(requestContext, result, previousValue);
  }

  protected void sendCreateNotification(
      RequestContext requestContext, ContextualConfigObject<T> result) {
    configChangeEventGeneratorOptional.ifPresent(
        configChangeEventGenerator ->
            configChangeEventGenerator.sendCreateNotification(
                requestContext,
                result.getData().getClass().getName(),
                result.getContext(),
                this.buildValueFromData(result.getData())));
  }

  protected void sendUpdateNotification(
      RequestContext requestContext, ContextualConfigObject<T> result, Value previousConfig) {
    configChangeEventGeneratorOptional.ifPresent(
        configChangeEventGenerator ->
            configChangeEventGenerator.sendUpdateNotification(
                requestContext,
                result.getData().getClass().getName(),
                result.getContext(),
                previousConfig,
                this.buildValueFromData(result.getData())));
  }

  protected void sendDeleteNotification(
      RequestContext requestContext, String objectClass, String id, Value deletedConfig) {
    configChangeEventGeneratorOptional.ifPresent(
        configChangeEventGenerator ->
            configChangeEventGenerator.sendDeleteNotification(
                requestContext, objectClass, id, deletedConfig));
  }
}
