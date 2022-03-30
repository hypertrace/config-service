package ai.traceable.api.attribute.override.service.handlers;

import ai.traceable.api.attribute.override.service.ApiAttributeOverridesConfigStore;
import ai.traceable.api.attribute.override.service.v1.ApiAttributeOverrides;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.hypertrace.config.objectstore.ConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;

class ConfigServiceHandler {
  private final ApiAttributeOverridesConfigStore attributeOverridesConfigStore;

  @Inject
  ConfigServiceHandler(ApiAttributeOverridesConfigStore attributeOverridesConfigStore) {
    this.attributeOverridesConfigStore = attributeOverridesConfigStore;
  }

  public Optional<ApiAttributeOverrides> getApiAttributeOverridesByApiId(
      RequestContext requestContext, String apiId) {
    return this.attributeOverridesConfigStore.getData(requestContext, apiId);
  }

  public Map<String, ApiAttributeOverrides> getAllApiAttributeOverrides(
      RequestContext requestContext) {
    return this.attributeOverridesConfigStore.getAllObjects(requestContext).stream()
        .map(ConfigObject::getData)
        .collect(Collectors.toMap(ApiAttributeOverrides::getApiId, Function.identity()));
  }

  public ApiAttributeOverrides upsertApiAttributeOverrides(
      RequestContext requestContext, ApiAttributeOverrides apiAttributeOverrides) {
    return this.attributeOverridesConfigStore
        .upsertObject(requestContext, apiAttributeOverrides)
        .getData();
  }
}
