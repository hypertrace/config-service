package ai.traceable.api.attribute.override.service.handlers;

import ai.traceable.api.attribute.override.service.v1.ApiAttributeOverrides;
import ai.traceable.api.attribute.override.service.v1.AttributeOverride;
import ai.traceable.api.attribute.override.service.v1.AttributeOverride.AttributeOverrideCase;
import ai.traceable.api.attribute.override.service.v1.AttributeOverrideIdentifier;
import ai.traceable.api.attribute.override.service.v1.AttributeOverrideIdentifier.AttributeOverrideIdentifierCase;
import ai.traceable.api.attribute.override.service.v1.RemoveApiAttributeOverridesRequest;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class RemoveApiAttributeOverridesHandler {
  private final ConfigServiceHandler configServiceHandler;

  @Inject
  public RemoveApiAttributeOverridesHandler(ConfigServiceHandler configServiceHandler) {
    this.configServiceHandler = configServiceHandler;
  }

  public void removeAttributeOverrides(
      RequestContext requestContext, RemoveApiAttributeOverridesRequest request) {
    Optional<ApiAttributeOverrides> existingAttributeOverrides =
        configServiceHandler.getApiAttributeOverridesByApiId(requestContext, request.getApiId());

    if (existingAttributeOverrides.isEmpty()) {
      throw new IllegalStateException(
          String.format("No existing override for api id [%s]", request.getApiId()));
    }

    List<AttributeOverride> filteredAttributeOverrides =
        removeAttributeOverrides(
            existingAttributeOverrides.get().getAttributeOverridesList(),
            request.getAttributeOverrideIdentifiersList());

    ApiAttributeOverrides filteredApiAttributeOverrides =
        ApiAttributeOverrides.newBuilder()
            .setApiId(existingAttributeOverrides.get().getApiId())
            .addAllAttributeOverrides(filteredAttributeOverrides)
            .build();
    configServiceHandler.upsertApiAttributeOverrides(requestContext, filteredApiAttributeOverrides);
  }

  private List<AttributeOverride> removeAttributeOverrides(
      List<AttributeOverride> existingAttributeOverrides,
      List<AttributeOverrideIdentifier> attributeOverridesToRemove) {
    List<AttributeOverride> paramTypeFilteredOverrides =
        removeParamTypeOverrides(existingAttributeOverrides, attributeOverridesToRemove);
    // insert deletion of other override types on top of paramTypeFilteredOverrides here.
    return paramTypeFilteredOverrides;
  }

  private List<AttributeOverride> removeParamTypeOverrides(
      List<AttributeOverride> existingAttributeOverrides,
      List<AttributeOverrideIdentifier> attributeOverridesToRemove) {
    Set<String> toBeRemovedParamNames =
        attributeOverridesToRemove.stream()
            .filter(
                removeOverrideIdentifier ->
                    removeOverrideIdentifier.getAttributeOverrideIdentifierCase()
                        == AttributeOverrideIdentifierCase.PARAM_TYPE_OVERRIDE_IDENTIFIER)
            .map(
                removeOverrideIdentifier ->
                    removeOverrideIdentifier.getParamTypeOverrideIdentifier().getParamName())
            .collect(Collectors.toSet());

    // remove all the param type attribute overrides which matches the identifiers
    return existingAttributeOverrides.stream()
        .filter(
            existingOverride ->
                existingOverride.getAttributeOverrideCase()
                        != AttributeOverrideCase.PARAM_TYPE_OVERRIDE
                    || !toBeRemovedParamNames.contains(
                        existingOverride.getParamTypeOverride().getParamName()))
        .collect(Collectors.toUnmodifiableList());
  }
}
