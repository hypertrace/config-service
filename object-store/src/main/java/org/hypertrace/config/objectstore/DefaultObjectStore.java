package org.hypertrace.config.objectstore;

import com.google.protobuf.Value;
import io.grpc.Status;
import java.util.Optional;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.config.service.v1.ContextSpecificConfig;
import org.hypertrace.config.service.v1.DeleteConfigRequest;
import org.hypertrace.config.service.v1.GetConfigRequest;
import org.hypertrace.config.service.v1.UpsertConfigRequest;
import org.hypertrace.config.service.v1.UpsertConfigResponse;
import org.hypertrace.core.grpcutils.context.RequestContext;

/**
 * DefaultObjectStore is an abstraction over the grpc api for config implementations working with
 * single object with no identifier - a pattern often used for a tenant-wide config.
 *
 * @param <T>
 */
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
                        .setConfig(this.buildValueForChangeEvent(data))
                        .build()));

    ConfigObject<T> upsertedObject =
        ConfigObjectImpl.tryBuild(response, this::buildDataFromValue)
            .orElseThrow(Status.INTERNAL::asRuntimeException);

    configChangeEventGeneratorOptional.ifPresent(
        configChangeEventGenerator -> {
          if (response.hasPrevConfig()) {
            configChangeEventGenerator.sendUpdateNotification(
                context,
                upsertedObject.getData().getClass().getName(),
                response.getPrevConfig(),
                response.getConfig());
          } else {
            configChangeEventGenerator.sendCreateNotification(
                context, upsertedObject.getData().getClass().getName(), response.getConfig());
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
      ConfigObject<T> object =
          ConfigObjectImpl.tryBuild(deletedConfig, this::buildDataFromValue)
              .orElseThrow(Status.INTERNAL::asRuntimeException);
      configChangeEventGeneratorOptional.ifPresent(
          configChangeEventGenerator ->
              configChangeEventGenerator.sendDeleteNotification(
                  context, object.getData().getClass().getName(), deletedConfig.getConfig()));
      return Optional.of(object);
    } catch (Exception exception) {
      if (Status.fromThrowable(exception).equals(Status.NOT_FOUND)) {
        return Optional.empty();
      }
      throw exception;
    }
  }
}
