package org.hypertrace.config.objectstore;

import com.google.protobuf.Value;
import io.grpc.Deadline;
import io.grpc.Status;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
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
  private final ClientConfig clientConfig;

  protected IdentifiedObjectStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      String resourceNamespace,
      String resourceName,
      ConfigChangeEventGenerator configChangeEventGenerator,
      ClientConfig clientConfig) {
    this.configServiceBlockingStub = configServiceBlockingStub;
    this.resourceNamespace = resourceNamespace;
    this.resourceName = resourceName;
    this.configChangeEventGeneratorOptional = Optional.ofNullable(configChangeEventGenerator);
    this.clientConfig = clientConfig;
  }

  protected IdentifiedObjectStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      String resourceNamespace,
      String resourceName,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    this(
        configServiceBlockingStub,
        resourceNamespace,
        resourceName,
        configChangeEventGenerator,
        ClientConfig.DEFAULT);
  }

  protected IdentifiedObjectStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      String resourceNamespace,
      String resourceName) {
    this(configServiceBlockingStub, resourceNamespace, resourceName, null);
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
                this.configServiceBlockingStub
                    .withDeadline(getDeadline())
                    .getAllConfigs(
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

  public List<T> getAllConfigData(RequestContext context) {
    return getAllObjects(context).stream()
        .map(ConfigObject::getData)
        .collect(Collectors.toUnmodifiableList());
  }

  public Optional<ContextualConfigObject<T>> getObject(RequestContext context, String id) {
    try {
      GetConfigResponse getConfigResponse =
          context.call(
              () ->
                  this.configServiceBlockingStub
                      .withDeadline(getDeadline())
                      .getConfig(
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
      return ContextualConfigObjectImpl.tryBuild(contextSpecificConfig, this::buildDataFromValue);
    } catch (Exception exception) {
      if (Status.fromThrowable(exception).equals(Status.NOT_FOUND)) {
        return Optional.empty();
      }
      throw exception;
    }
  }

  public Optional<T> getData(RequestContext context, String id) {
    return getObject(context, id).map(ConfigObject::getData);
  }

  public ContextualConfigObject<T> upsertObject(RequestContext context, T data) {
    UpsertConfigResponse response =
        context.call(
            () ->
                this.configServiceBlockingStub
                    .withDeadline(getDeadline())
                    .upsertConfig(
                        UpsertConfigRequest.newBuilder()
                            .setResourceName(this.resourceName)
                            .setResourceNamespace(this.resourceNamespace)
                            .setContext(this.getContextFromData(data))
                            .setConfig(this.buildValueFromData(data))
                            .build()));

    return this.processUpsertResult(context, response)
        .orElseThrow(Status.INTERNAL::asRuntimeException);
  }

  public Optional<DeletedContextualConfigObject<T>> deleteObject(
      RequestContext requestContext, String context) {
    try {
      ContextSpecificConfig deletedConfig =
          requestContext
              .call(
                  () ->
                      this.configServiceBlockingStub
                          .withDeadline(getDeadline())
                          .deleteConfig(
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
                this.configServiceBlockingStub
                    .withDeadline(getDeadline())
                    .upsertAllConfigs(
                        UpsertAllConfigsRequest.newBuilder().addAllConfigs(configs).build()))
        .getUpsertedConfigsList()
        .stream()
        .map(upsertedConfig -> this.processUpsertResult(context, upsertedConfig))
        .flatMap(Optional::stream)
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
                this.configServiceBlockingStub
                    .withDeadline(getDeadline())
                    .deleteConfigs(
                        DeleteConfigsRequest.newBuilder().addAllConfigs(configsToDelete).build()))
        .getDeletedConfigsList()
        .stream()
        .map(deletedConfig -> processDeleteResult(requestContext, deletedConfig))
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

  private DeletedContextualConfigObject<T> processDeleteResult(
      RequestContext requestContext, ContextSpecificConfig deletedConfig) {
    DeletedContextualConfigObject<T> deletedContextualConfigObject =
        DeletedContextualConfigObjectImpl.tryBuild(deletedConfig, this::buildDataFromValue);

    if (deletedContextualConfigObject.getDeletedData().isPresent()) {
      T data = deletedContextualConfigObject.getDeletedData().get();
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
                this.buildClassNameForChangeEvent(result.getData()),
                result.getContext(),
                this.buildValueForChangeEvent(result.getData())));
  }

  private void tryReportUpdate(
      RequestContext requestContext, ContextualConfigObject<T> result, Value previousValue) {
    configChangeEventGeneratorOptional.ifPresent(
        configChangeEventGenerator ->
            configChangeEventGenerator.sendUpdateNotification(
                requestContext,
                this.buildClassNameForChangeEvent(result.getData()),
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
                this.buildValueForChangeEvent(result.getData())));
  }

  protected Deadline getDeadline() {
    return Deadline.after(this.clientConfig.getTimeout().toMillis(), TimeUnit.MILLISECONDS);
  }
}
