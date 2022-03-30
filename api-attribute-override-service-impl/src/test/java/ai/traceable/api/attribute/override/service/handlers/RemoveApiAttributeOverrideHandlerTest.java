package ai.traceable.api.attribute.override.service.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.api.attribute.override.service.v1.ApiAttributeOverrides;
import ai.traceable.api.attribute.override.service.v1.AttributeOverride;
import ai.traceable.api.attribute.override.service.v1.AttributeOverrideIdentifier;
import ai.traceable.api.attribute.override.service.v1.ParamTypeAction;
import ai.traceable.api.attribute.override.service.v1.ParamTypeOverride;
import ai.traceable.api.attribute.override.service.v1.ParamTypeOverrideIdentifier;
import ai.traceable.api.attribute.override.service.v1.RemoveApiAttributeOverridesRequest;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class RemoveApiAttributeOverrideHandlerTest {
  private RemoveApiAttributeOverridesHandler removeApiAttributeOverridesHandler;
  private ConfigServiceHandler configServiceHandler;

  @BeforeEach
  void setup() {
    configServiceHandler = Mockito.mock(ConfigServiceHandler.class);
    removeApiAttributeOverridesHandler =
        new RemoveApiAttributeOverridesHandler(configServiceHandler);
  }

  @Test
  void removeMatchingOverride() {
    List<AttributeOverride> attributeOverrides =
        List.of(
            AttributeOverride.newBuilder()
                .setParamTypeOverride(
                    ParamTypeOverride.newBuilder()
                        .setParamName("param1")
                        .setParamTypeAction(ParamTypeAction.PARAM_TYPE_ACTION_COLLAPSE))
                .build(),
            AttributeOverride.newBuilder()
                .setParamTypeOverride(
                    ParamTypeOverride.newBuilder()
                        .setParamName("param2")
                        .setParamTypeAction(ParamTypeAction.PARAM_TYPE_ACTION_EXCLUDE))
                .build());
    when(configServiceHandler.getApiAttributeOverridesByApiId(any(), any()))
        .thenReturn(
            Optional.of(
                ApiAttributeOverrides.newBuilder()
                    .addAllAttributeOverrides(attributeOverrides)
                    .setApiId("api1")
                    .build()));

    List<AttributeOverrideIdentifier> toRemoveIdentifiers =
        List.of(
            AttributeOverrideIdentifier.newBuilder()
                .setParamTypeOverrideIdentifier(
                    ParamTypeOverrideIdentifier.newBuilder().setParamName("param1").build())
                .build());
    RemoveApiAttributeOverridesRequest removeRequest =
        RemoveApiAttributeOverridesRequest.newBuilder()
            .addAllAttributeOverrideIdentifiers(toRemoveIdentifiers)
            .setApiId("api1")
            .build();
    removeApiAttributeOverridesHandler.removeAttributeOverrides(
        RequestContext.forTenantId("tenantId"), removeRequest);
    ArgumentCaptor<ApiAttributeOverrides> apiAttributeOverridesCaptor =
        ArgumentCaptor.forClass(ApiAttributeOverrides.class);
    verify(configServiceHandler)
        .upsertApiAttributeOverrides(any(), apiAttributeOverridesCaptor.capture());
    ApiAttributeOverrides upsertedOverride = apiAttributeOverridesCaptor.getValue();
    assertEquals(1, upsertedOverride.getAttributeOverridesList().size());
    Assertions.assertEquals(
        ParamTypeAction.PARAM_TYPE_ACTION_EXCLUDE,
        upsertedOverride
            .getAttributeOverridesList()
            .get(0)
            .getParamTypeOverride()
            .getParamTypeAction());
    Assertions.assertEquals(
        "param2",
        upsertedOverride.getAttributeOverridesList().get(0).getParamTypeOverride().getParamName());
  }

  @Test
  void removeNonMatchingOverride() {
    List<AttributeOverride> attributeOverrides =
        List.of(
            AttributeOverride.newBuilder()
                .setParamTypeOverride(
                    ParamTypeOverride.newBuilder()
                        .setParamName("param1")
                        .setParamTypeAction(ParamTypeAction.PARAM_TYPE_ACTION_COLLAPSE))
                .build());
    when(configServiceHandler.getApiAttributeOverridesByApiId(any(), any()))
        .thenReturn(
            Optional.of(
                ApiAttributeOverrides.newBuilder()
                    .addAllAttributeOverrides(attributeOverrides)
                    .setApiId("api1")
                    .build()));

    List<AttributeOverrideIdentifier> toRemoveIdentifiers =
        List.of(
            AttributeOverrideIdentifier.newBuilder()
                .setParamTypeOverrideIdentifier(
                    ParamTypeOverrideIdentifier.newBuilder().setParamName("param2").build())
                .build());
    RemoveApiAttributeOverridesRequest removeRequest =
        RemoveApiAttributeOverridesRequest.newBuilder()
            .addAllAttributeOverrideIdentifiers(toRemoveIdentifiers)
            .setApiId("api1")
            .build();
    removeApiAttributeOverridesHandler.removeAttributeOverrides(
        RequestContext.forTenantId("tenantId"), removeRequest);
    ArgumentCaptor<ApiAttributeOverrides> apiAttributeOverridesCaptor =
        ArgumentCaptor.forClass(ApiAttributeOverrides.class);
    verify(configServiceHandler)
        .upsertApiAttributeOverrides(any(), apiAttributeOverridesCaptor.capture());
    ApiAttributeOverrides upsertedOverride = apiAttributeOverridesCaptor.getValue();
    assertEquals(1, upsertedOverride.getAttributeOverridesList().size());
    Assertions.assertEquals(
        ParamTypeAction.PARAM_TYPE_ACTION_COLLAPSE,
        upsertedOverride
            .getAttributeOverridesList()
            .get(0)
            .getParamTypeOverride()
            .getParamTypeAction());
    Assertions.assertEquals(
        "param1",
        upsertedOverride.getAttributeOverridesList().get(0).getParamTypeOverride().getParamName());
  }
}
