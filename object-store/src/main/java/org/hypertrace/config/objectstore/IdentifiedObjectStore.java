package org.hypertrace.config.objectstore;

import com.google.protobuf.Value;
import io.grpc.Status;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.config.service.v1.ContextSpecificConfig;
import org.hypertrace.config.service.v1.DeleteConfigRequest;
import org.hypertrace.config.service.v1.GetAllConfigsRequest;
import org.hypertrace.config.service.v1.GetConfigRequest;
import org.hypertrace.config.service.v1.UpsertConfigRequest;
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

  protected IdentifiedObjectStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      String resourceNamespace,
      String resourceName) {
    this.configServiceBlockingStub = configServiceBlockingStub;
    this.resourceNamespace = resourceNamespace;
    this.resourceName = resourceName;
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
    Value upsertedValue =
        context.call(
            () ->
                this.configServiceBlockingStub
                    .upsertConfig(
                        UpsertConfigRequest.newBuilder()
                            .setResourceName(this.resourceName)
                            .setResourceNamespace(this.resourceNamespace)
                            .setContext(this.getContextFromObject(object))
                            .setConfig(this.buildValueFromObject(object))
                            .build())
                    .getConfig());

    return this.buildObjectFromValue(upsertedValue)
        .orElseThrow(Status.INTERNAL::asRuntimeException);
  }

  public void deleteObject(RequestContext context, String id) {
    context.call(
        () ->
            this.configServiceBlockingStub.deleteConfig(
                DeleteConfigRequest.newBuilder()
                    .setResourceName(this.resourceName)
                    .setResourceNamespace(this.resourceNamespace)
                    .setContext(id)
                    .build()));
  }

  public List<T> upsertObjects(RequestContext context, List<T> objects) {
    // TODO push down a bulk upsert API
    return objects.stream()
        .map(object -> this.upsertObject(context, object))
        .collect(Collectors.toUnmodifiableList());
  }
}
