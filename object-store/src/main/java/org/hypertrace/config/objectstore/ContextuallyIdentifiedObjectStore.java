package org.hypertrace.config.objectstore;

import com.google.protobuf.Value;
import java.util.Optional;
import java.util.function.Function;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.context.RequestContext;

public abstract class ContextuallyIdentifiedObjectStore<T> {
  private final ConfigServiceBlockingStub configServiceBlockingStub;
  private final String resourceNamespace;
  private final String resourceName;
  private final Optional<ConfigChangeEventGenerator> configChangeEventGenerator;

  protected ContextuallyIdentifiedObjectStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      String resourceNamespace,
      String resourceName,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    this.configServiceBlockingStub = configServiceBlockingStub;
    this.resourceNamespace = resourceNamespace;
    this.resourceName = resourceName;
    this.configChangeEventGenerator = Optional.of(configChangeEventGenerator);
  }

  protected ContextuallyIdentifiedObjectStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      String resourceNamespace,
      String resourceName) {
    this.configServiceBlockingStub = configServiceBlockingStub;
    this.resourceNamespace = resourceNamespace;
    this.resourceName = resourceName;
    this.configChangeEventGenerator = Optional.empty();
  }

  protected abstract Optional<T> buildDataFromValue(Value value);

  protected abstract Value buildValueFromData(T data);

  protected abstract String getConfigContextFromRequestContext(RequestContext requestContext);

  private IdentifiedObjectStore<T> buildObjectStoreForContext(RequestContext context) {
    return this.configChangeEventGenerator
        .map(generator -> new ContextAwareIdentifiedObjectStoreDelegate(context, generator))
        .orElseGet(() -> new ContextAwareIdentifiedObjectStoreDelegate(context));
  }

  public Optional<T> getData(RequestContext context) {
    return this.buildObjectStoreForContext(context)
        .getData(context, this.getConfigContextFromRequestContext(context));
  }

  public ConfigObject<T> upsertObject(RequestContext context, T data) {
    return this.buildObjectStoreForContext(context).upsertObject(context, data);
  }

  public Optional<ConfigObject<T>> deleteObject(RequestContext context) {
    return this.buildObjectStoreForContext(context)
        .deleteObject(context, this.getConfigContextFromRequestContext(context))
        .map(Function.identity());
  }

  private class ContextAwareIdentifiedObjectStoreDelegate extends IdentifiedObjectStore<T> {

    private final RequestContext requestContext;

    ContextAwareIdentifiedObjectStoreDelegate(RequestContext requestContext) {
      super(configServiceBlockingStub, resourceNamespace, resourceName);
      this.requestContext = requestContext;
    }

    ContextAwareIdentifiedObjectStoreDelegate(
        RequestContext requestContext, ConfigChangeEventGenerator configChangeEventGenerator) {
      super(configServiceBlockingStub, resourceNamespace, resourceName, configChangeEventGenerator);
      this.requestContext = requestContext;
    }

    @Override
    protected Optional<T> buildDataFromValue(Value value) {
      return ContextuallyIdentifiedObjectStore.this.buildDataFromValue(value);
    }

    @Override
    protected Value buildValueFromData(T data) {
      return ContextuallyIdentifiedObjectStore.this.buildValueFromData(data);
    }

    @Override
    protected String getContextFromData(T data) {
      return ContextuallyIdentifiedObjectStore.this.getConfigContextFromRequestContext(
          requestContext);
    }
  }
}
