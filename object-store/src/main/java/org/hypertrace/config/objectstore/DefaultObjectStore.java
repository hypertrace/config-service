package org.hypertrace.config.objectstore;

import com.google.protobuf.Value;
import io.grpc.Status;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.config.service.v1.ContextSpecificConfig;
import org.hypertrace.config.service.v1.DeleteConfigRequest;
import org.hypertrace.config.service.v1.GetConfigRequest;
import org.hypertrace.config.service.v1.GetConfigResponse;
import org.hypertrace.config.service.v1.UpsertConfigRequest;
import org.hypertrace.config.service.v1.UpsertConfigResponse;
import org.hypertrace.core.grpcutils.context.RequestContext;

/**
 * DefaultObjectStore is an abstraction over the grpc api for config implementations working with
 * single object with no identifier - a pattern often used for a tenant-wide config.
 *
 * @param <T>
 */
@Slf4j
public abstract class DefaultObjectStore<T> {
  private final ConfigServiceBlockingStub configServiceBlockingStub;
  private final String resourceNamespace;
  private final String resourceName;
  private final Optional<ConfigChangeEventGenerator> configChangeEventGeneratorOptional;

  protected DefaultObjectStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      String resourceNamespace,
      String resourceName,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    this.configServiceBlockingStub = configServiceBlockingStub;
    this.resourceNamespace = resourceNamespace;
    this.resourceName = resourceName;
    this.configChangeEventGeneratorOptional = Optional.of(configChangeEventGenerator);
  }

  protected DefaultObjectStore(
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

  protected Value buildValueForChangeEvent(T data) {
    return this.buildValueFromData(data);
  }

  protected String buildClassNameForChangeEvent(T data) {
    return data.getClass().getName();
  }

  public Optional<ConfigObject<T>> getObject(RequestContext context) {
    try {
      GetConfigResponse getConfigResponse =
          context.call(
              () ->
                  this.configServiceBlockingStub.getConfig(
                      GetConfigRequest.newBuilder()
                          .setResourceName(this.resourceName)
                          .setResourceNamespace(this.resourceNamespace)
                          .build()));

      ConfigObject<T> tConfigObject =
          ConfigObjectImpl.tryBuild(
              getConfigResponse.getConfig(),
              getConfigResponse.getCreationTimestamp(),
              getConfigResponse.getUpdateTimestamp(),
              this::buildDataFromValue);
      if (tConfigObject.getData().isEmpty()) {
        throw Status.INTERNAL
            .withDescription(
                "Could not convert config "
                    + getConfigResponse.getConfig()
                    + " into corresponding proto")
            .asRuntimeException(context.buildTrailers());
      }
      return Optional.of(tConfigObject);
    } catch (Exception exception) {
      if (Status.fromThrowable(exception).equals(Status.NOT_FOUND)) {
        return Optional.empty();
      }
      throw exception;
    }
  }

  public Optional<T> getData(RequestContext context) {
    try {
      Value value =
          context.call(
              () ->
                  this.configServiceBlockingStub
                      .getConfig(
                          GetConfigRequest.newBuilder()
                              .setResourceName(this.resourceName)
                              .setResourceNamespace(this.resourceNamespace)
                              .build())
                      .getConfig());
      T data = this.buildDataFromValue(value).orElseThrow(Status.INTERNAL::asRuntimeException);
      return Optional.of(data);
    } catch (Exception exception) {
      if (Status.fromThrowable(exception).equals(Status.NOT_FOUND)) {
        return Optional.empty();
      }
      throw exception;
    }
  }

  public ConfigObject<T> upsertObject(RequestContext context, T data) {
    UpsertConfigResponse response =
        context.call(
            () ->
                this.configServiceBlockingStub.upsertConfig(
                    UpsertConfigRequest.newBuilder()
                        .setResourceName(this.resourceName)
                        .setResourceNamespace(this.resourceNamespace)
                        .setConfig(this.buildValueFromData(data))
                        .build()));

    ConfigObject<T> upsertedObject = ConfigObjectImpl.tryBuild(response, this::buildDataFromValue);
    if (upsertedObject.getData().isEmpty()) {
      throw Status.INTERNAL
          .withDescription("Could not convert upserted config into corresponding proto")
          .asRuntimeException(context.buildTrailers());
    }

    configChangeEventGeneratorOptional.ifPresent(
        configChangeEventGenerator -> {
          if (response.hasPrevConfig()) {
            configChangeEventGenerator.sendUpdateNotification(
                context,
                this.buildClassNameForChangeEvent(upsertedObject.getData().get()),
                this.buildDataFromValue(response.getPrevConfig())
                    .map(this::buildValueForChangeEvent)
                    .orElseGet(
                        () -> {
                          log.error(
                              "Unable to convert previousValue back to data for change event. Falling back to raw value {}",
                              response.getPrevConfig());
                          return response.getPrevConfig();
                        }),
                this.buildValueForChangeEvent(upsertedObject.getData().get()));
          } else {
            configChangeEventGenerator.sendCreateNotification(
                context,
                this.buildClassNameForChangeEvent(upsertedObject.getData().get()),
                this.buildValueForChangeEvent(upsertedObject.getData().get()));
          }
        });
    return upsertedObject;
  }

  public Optional<ConfigObject<T>> deleteObject(RequestContext context) {
    try {
      ContextSpecificConfig deletedConfig =
          context
              .call(
                  () ->
                      this.configServiceBlockingStub.deleteConfig(
                          DeleteConfigRequest.newBuilder()
                              .setResourceName(this.resourceName)
                              .setResourceNamespace(this.resourceNamespace)
                              .build()))
              .getDeletedConfig();
      ConfigObject<T> object = ConfigObjectImpl.tryBuild(deletedConfig, this::buildDataFromValue);
      configChangeEventGeneratorOptional.ifPresent(
          configChangeEventGenerator -> {
            if (object.getData().isPresent()) {
              configChangeEventGenerator.sendDeleteNotification(
                  context,
                  this.buildClassNameForChangeEvent(object.getData().get()),
                  this.buildValueForChangeEvent(object.getData().get()));
            }
          });
      return Optional.of(object);
    } catch (Exception exception) {
      if (Status.fromThrowable(exception).equals(Status.NOT_FOUND)) {
        return Optional.empty();
      }
      throw exception;
    }
  }
}
