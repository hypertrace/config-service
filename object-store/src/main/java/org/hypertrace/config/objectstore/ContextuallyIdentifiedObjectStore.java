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
  private final ClientConfig clientConfig;

  protected ContextuallyIdentifiedObjectStore(
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

  protected ContextuallyIdentifiedObjectStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      String resourceNamespace,
      String resourceName,
      ConfigChangeEventGenerator configChangeEventGenerator,
      ClientConfig clientConfig) {
    this.configServiceBlockingStub = configServiceBlockingStub;
    this.resourceNamespace = resourceNamespace;
    this.resourceName = resourceName;
    this.configChangeEventGenerator = Optional.ofNullable(configChangeEventGenerator);
    this.clientConfig = clientConfig;
  }

  protected ContextuallyIdentifiedObjectStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      String resourceNamespace,
      String resourceName) {
    this(configServiceBlockingStub, resourceNamespace, resourceName, null);
  }

  protected abstract Optional<T> buildDataFromValue(Value value);

  protected abstract Value buildValueFromData(T data);

  protected Value buildValueForChangeEvent(T data) {
    return this.buildValueFromData(data);
  }

  protected String buildClassNameForChangeEvent(T data) {
    return data.getClass().getName();
  }

  protected abstract String getConfigContextFromRequestContext(RequestContext requestContext);

  private IdentifiedObjectStore<T> buildObjectStoreForContext(RequestContext context) {
    return this.configChangeEventGenerator
        .map(generator -> new ContextAwareIdentifiedObjectStoreDelegate(context, generator))
        .orElseGet(() -> new ContextAwareIdentifiedObjectStoreDelegate(context));
  }

  public Optional<ConfigObject<T>> getObject(RequestContext context) {
    return this.buildObjectStoreForContext(context)
        .getObject(context, this.getConfigContextFromRequestContext(context))
        .map(Function.identity());
  }

  public Optional<T> getData(RequestContext context) {
    return this.buildObjectStoreForContext(context)
        .getData(context, this.getConfigContextFromRequestContext(context));
  }

  public ConfigObject<T> upsertObject(RequestContext context, T data) {
    return this.buildObjectStoreForContext(context).upsertObject(context, data);
  }

  public Optional<DeletedConfigObject<T>> deleteObject(RequestContext context) {
    return this.buildObjectStoreForContext(context)
        .deleteObject(context, this.getConfigContextFromRequestContext(context))
        .map(Function.identity());
  }

  private class ContextAwareIdentifiedObjectStoreDelegate extends IdentifiedObjectStore<T> {

    private final RequestContext requestContext;

    ContextAwareIdentifiedObjectStoreDelegate(RequestContext requestContext) {
      this(requestContext, null);
    }

    ContextAwareIdentifiedObjectStoreDelegate(
        RequestContext requestContext, ConfigChangeEventGenerator configChangeEventGenerator) {
      super(
          configServiceBlockingStub,
          resourceNamespace,
          resourceName,
          configChangeEventGenerator,
          clientConfig);
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
    protected Value buildValueForChangeEvent(T data) {
      return ContextuallyIdentifiedObjectStore.this.buildValueForChangeEvent(data);
    }

    @Override
    protected String buildClassNameForChangeEvent(T data) {
      return ContextuallyIdentifiedObjectStore.this.buildClassNameForChangeEvent(data);
    }

    @Override
    protected String getContextFromData(T data) {
      return ContextuallyIdentifiedObjectStore.this.getConfigContextFromRequestContext(
          requestContext);
    }
  }
}
