package ai.traceable.api.attribute.override.service.handlers;

import ai.traceable.api.attribute.override.service.v1.ApiAttributeOverrides;
import ai.traceable.api.attribute.override.service.v1.AttributeOverride;
import ai.traceable.api.attribute.override.service.v1.AttributeOverride.AttributeOverrideCase;
import ai.traceable.api.attribute.override.service.v1.BooleanOverride;
import ai.traceable.api.attribute.override.service.v1.UpsertApiAttributeOverridesRequest;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class UpsertApiAttributeOverridesHandler {
  private final ConfigServiceHandler configServiceHandler;

  @Inject
  public UpsertApiAttributeOverridesHandler(ConfigServiceHandler configServiceHandler) {
    this.configServiceHandler = configServiceHandler;
  }

  public ApiAttributeOverrides upsertAttributeOverrides(
      RequestContext requestContext,
      UpsertApiAttributeOverridesRequest upsertApiAttributeOverridesRequest) {
    Optional<ApiAttributeOverrides> existingAttributeOverrides =
        configServiceHandler.getApiAttributeOverridesByApiId(
            requestContext, upsertApiAttributeOverridesRequest.getApiId());

    if (existingAttributeOverrides.isEmpty()) {
      return configServiceHandler.upsertApiAttributeOverrides(
          requestContext,
          ApiAttributeOverrides.newBuilder()
              .addAllAttributeOverrides(
                  upsertApiAttributeOverridesRequest.getAttributeOverridesList())
              .setApiId(upsertApiAttributeOverridesRequest.getApiId())
              .build());
    }

    List<AttributeOverride> mergedAttributeOverrides =
        mergeAttributeOverrides(
            existingAttributeOverrides.get().getAttributeOverridesList(),
            upsertApiAttributeOverridesRequest.getAttributeOverridesList());
    ApiAttributeOverrides mergedApiAttributeOverrides =
        ApiAttributeOverrides.newBuilder()
            .setApiId(existingAttributeOverrides.get().getApiId())
            .addAllAttributeOverrides(mergedAttributeOverrides)
            .build();
    return configServiceHandler.upsertApiAttributeOverrides(
        requestContext, mergedApiAttributeOverrides);
  }

  private List<AttributeOverride> mergeAttributeOverrides(
      List<AttributeOverride> existingAttributeOverrides,
      List<AttributeOverride> updatedAttributeOverrides) {
    List<AttributeOverride> mergedParamTypeOverrides =
        mergeParamTypeOverride(existingAttributeOverrides, updatedAttributeOverrides);
    mergedParamTypeOverrides =
        mergeApiTypeOverride(mergedParamTypeOverrides, updatedAttributeOverrides);
    // merge different type of overrides on top of mergedParamTypeOverrides here.
    return mergedParamTypeOverrides;
  }

  private List<AttributeOverride> mergeParamTypeOverride(
      List<AttributeOverride> existingAttributeOverrides,
      List<AttributeOverride> updatedAttributeOverrides) {
    List<AttributeOverride> toBeUpsertedParamTypeOverrides =
        updatedAttributeOverrides.stream()
            .filter(
                updatedOverride ->
                    updatedOverride.getAttributeOverrideCase()
                        == AttributeOverrideCase.PARAM_TYPE_OVERRIDE)
            .collect(Collectors.toList());

    Set<String> toBeUpsertedParamNames =
        toBeUpsertedParamTypeOverrides.stream()
            .map(updatedOverride -> updatedOverride.getParamTypeOverride().getParamName())
            .collect(Collectors.toUnmodifiableSet());

    // From the existing attribute overrides, remove the matching param type overrides.
    List<AttributeOverride> filteredAttributeOverrides =
        existingAttributeOverrides.stream()
            .filter(
                existingOverride ->
                    existingOverride.getAttributeOverrideCase()
                            != AttributeOverrideCase.PARAM_TYPE_OVERRIDE
                        || !toBeUpsertedParamNames.contains(
                            existingOverride.getParamTypeOverride().getParamName()))
            .collect(Collectors.toList());
    // add the updated overrides.
    filteredAttributeOverrides.addAll(toBeUpsertedParamTypeOverrides);
    return filteredAttributeOverrides;
  }

  private List<AttributeOverride> mergeApiTypeOverride(
      List<AttributeOverride> existingAttributeOverrides,
      List<AttributeOverride> updatedAttributeOverrides) {
    List<AttributeOverride> toBeUpsertedApiOverrides =
        updatedAttributeOverrides.stream()
            .filter(
                updatedOverride ->
                    updatedOverride.getAttributeOverrideCase()
                        == AttributeOverrideCase.API_EXTERNAL_OVERRIDE)
            .collect(Collectors.toList());

    Set<BooleanOverride> toBeUpsertedAttributesOverrides =
        toBeUpsertedApiOverrides.stream()
            .map(AttributeOverride::getApiExternalOverride)
            .collect(Collectors.toUnmodifiableSet());

    // From the existing attribute overrides, remove the matching param type overrides.
    List<AttributeOverride> filteredAttributeOverrides =
        existingAttributeOverrides.stream()
            .filter(
                existingOverride ->
                    existingOverride.getAttributeOverrideCase()
                        != AttributeOverrideCase.API_EXTERNAL_OVERRIDE)
            .collect(Collectors.toList());
    // add the updated overrides.
    filteredAttributeOverrides.addAll(toBeUpsertedApiOverrides);
    return filteredAttributeOverrides;
  }
}
