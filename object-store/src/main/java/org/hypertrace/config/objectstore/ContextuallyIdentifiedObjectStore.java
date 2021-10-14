package org.hypertrace.config.objectstore;

import com.google.protobuf.Value;
import java.util.Optional;
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

  protected abstract Optional<T> buildObjectFromValue(Value value);

  protected abstract Value buildValueFromObject(T object);

  protected abstract String getConfigContextFromRequestContext(RequestContext requestContext);

  private IdentifiedObjectStore<T> buildObjectStoreForContext(RequestContext context) {
    return this.configChangeEventGenerator
        .map(generator -> new ContextAwareIdentifiedObjectStoreDelegate(context, generator))
        .orElseGet(() -> new ContextAwareIdentifiedObjectStoreDelegate(context));
  }

  public Optional<T> getObject(RequestContext context) {
    return this.buildObjectStoreForContext(context)
        .getObject(context, this.getConfigContextFromRequestContext(context));
  }

  public T upsertObject(RequestContext context, T object) {
    return this.buildObjectStoreForContext(context).upsertObject(context, object);
  }

  public Optional<T> deleteObject(RequestContext context) {
    return this.buildObjectStoreForContext(context)
        .deleteObject(context, this.getConfigContextFromRequestContext(context));
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
    protected Optional<T> buildObjectFromValue(Value value) {
      return ContextuallyIdentifiedObjectStore.this.buildObjectFromValue(value);
    }

    @Override
    protected Value buildValueFromObject(T object) {
      return ContextuallyIdentifiedObjectStore.this.buildValueFromObject(object);
    }

    @Override
    protected String getContextFromObject(T object) {
      return ContextuallyIdentifiedObjectStore.this.getConfigContextFromRequestContext(
          requestContext);
    }
  }
}
