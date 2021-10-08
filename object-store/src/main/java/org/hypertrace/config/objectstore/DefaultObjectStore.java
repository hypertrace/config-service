package org.hypertrace.config.objectstore;

import com.google.protobuf.Value;
import io.grpc.Status;
import java.util.Optional;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
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
  private final ConfigChangeEventGenerator configChangeEventGenerator;

  protected DefaultObjectStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      String resourceNamespace,
      String resourceName,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    this.configServiceBlockingStub = configServiceBlockingStub;
    this.resourceNamespace = resourceNamespace;
    this.resourceName = resourceName;
    this.configChangeEventGenerator = configChangeEventGenerator;
  }

  protected abstract Optional<T> buildObjectFromValue(Value value);

  protected abstract Value buildValueFromObject(T object);

  public Optional<T> getObject(RequestContext context) {
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
                        .setConfig(this.buildValueFromObject(object))
                        .build()));

    T upsertedObject =
        this.buildObjectFromValue(response.getConfig())
            .orElseThrow(Status.INTERNAL::asRuntimeException);

    if (response.hasPrevConfig()) {
      configChangeEventGenerator.sendUpdateNotification(
          context,
          upsertedObject.getClass().getName(),
          null,
          response.getPrevConfig(),
          response.getConfig());
    } else {
      configChangeEventGenerator.sendCreateNotification(
          context, upsertedObject.getClass().getName(), null, response.getConfig());
    }
    return upsertedObject;
  }

  public Optional<T> deleteObject(RequestContext context) {
    try {
      Value deletedValue =
          context
              .call(
                  () ->
                      this.configServiceBlockingStub.deleteConfig(
                          DeleteConfigRequest.newBuilder()
                              .setResourceName(this.resourceName)
                              .setResourceNamespace(this.resourceNamespace)
                              .build()))
              .getDeletedConfig()
              .getConfig();
      T object =
          this.buildObjectFromValue(deletedValue).orElseThrow(Status.INTERNAL::asRuntimeException);
      configChangeEventGenerator.sendDeleteNotification(
          context, object.getClass().getName(), null, deletedValue);
      return Optional.of(object);
    } catch (Exception exception) {
      if (Status.fromThrowable(exception).equals(Status.NOT_FOUND)) {
        return Optional.empty();
      }
      throw exception;
    }
  }
}
