package ai.traceable.genai.config.service.v1.manager;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.genai.config.service.v1.GenAiConfig;
import ai.traceable.genai.config.service.v1.GenAiScope;
import ai.traceable.genai.config.service.v1.store.GenAiConfigConverter;
import ai.traceable.genai.config.service.v1.store.GenAiConfigIdGenerator;
import ai.traceable.genai.config.service.v1.store.GenAiConfigStore;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.config.objectstore.ConfigObject;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;

class MockGenAiConfigStore extends GenAiConfigStore {

  private final Map<String, Map<String, ContextualConfigObject<GenAiConfig>>> tenantConfigsMap =
      new HashMap<>();
  private final GenAiConfigIdGenerator configIdGenerator;

  MockGenAiConfigStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator,
      GenAiConfigConverter configConverter,
      GenAiConfigIdGenerator configIdGenerator) {
    super(
        configServiceBlockingStub, configChangeEventGenerator, configConverter, configIdGenerator);
    this.configIdGenerator = configIdGenerator;
  }

  @Override
  public Optional<GenAiConfig> getData(RequestContext requestContext, String id) {
    Optional<String> tenantId = requestContext.getTenantId();
    return tenantId.flatMap(
        s ->
            Optional.ofNullable(tenantConfigsMap.get(s))
                .map(valuesMap -> valuesMap.get(id))
                .map(ConfigObject::getData));
  }

  @SuppressWarnings("unchecked")
  @Override
  public ContextualConfigObject<GenAiConfig> upsertObject(
      RequestContext requestContext, GenAiConfig genAiConfig) {
    Optional<String> tenantId = requestContext.getTenantId();
    if (tenantId.isEmpty()) {
      throw new IllegalStateException("Tenant ID is required");
    }

    String tenantIdStr = tenantId.get();
    if (!tenantConfigsMap.containsKey(tenantIdStr)) {
      tenantConfigsMap.put(tenantIdStr, new HashMap<>());
    }

    String id = configIdGenerator.generateId(genAiConfig.getScope());
    ContextualConfigObject<GenAiConfig> configObject = mock(ContextualConfigObject.class);
    when(configObject.getData()).thenReturn(genAiConfig);

    tenantConfigsMap.get(tenantIdStr).put(id, configObject);
    return configObject;
  }

  @Override
  public Optional<GenAiConfig> getGenAiConfigFromStore(
      RequestContext requestContext, GenAiScope scope) {
    return getData(requestContext, configIdGenerator.generateId(scope));
  }
}
