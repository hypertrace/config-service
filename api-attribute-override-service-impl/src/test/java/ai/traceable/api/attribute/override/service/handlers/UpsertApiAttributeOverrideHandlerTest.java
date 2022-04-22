package ai.traceable.api.attribute.override.service.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.api.attribute.override.service.v1.ApiAttributeOverrides;
import ai.traceable.api.attribute.override.service.v1.AttributeOverride;
import ai.traceable.api.attribute.override.service.v1.BooleanOverride;
import ai.traceable.api.attribute.override.service.v1.ParamTypeAction;
import ai.traceable.api.attribute.override.service.v1.ParamTypeOverride;
import ai.traceable.api.attribute.override.service.v1.UpsertApiAttributeOverridesRequest;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class UpsertApiAttributeOverrideHandlerTest {
  private UpsertApiAttributeOverridesHandler upsertApiAttributeOverridesHandler;
  private ConfigServiceHandler configServiceHandler = Mockito.mock(ConfigServiceHandler.class);

  @BeforeEach
  void setup() {
    upsertApiAttributeOverridesHandler =
        new UpsertApiAttributeOverridesHandler(configServiceHandler);
  }

  @Test
  void apiAttributeOverrideNotPresent_param_type() {
    when(configServiceHandler.getApiAttributeOverridesByApiId(any(), any()))
        .thenReturn(Optional.empty());

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

    UpsertApiAttributeOverridesRequest upsertRequest =
        UpsertApiAttributeOverridesRequest.newBuilder()
            .setApiId("api1")
            .addAllAttributeOverrides(attributeOverrides)
            .build();

    upsertApiAttributeOverridesHandler.upsertAttributeOverrides(
        RequestContext.forTenantId("tenant1"), upsertRequest);

    ArgumentCaptor<ApiAttributeOverrides> apiAttributeOverridesCaptor =
        ArgumentCaptor.forClass(ApiAttributeOverrides.class);
    verify(configServiceHandler)
        .upsertApiAttributeOverrides(any(), apiAttributeOverridesCaptor.capture());
    ApiAttributeOverrides upsertedOverride = apiAttributeOverridesCaptor.getValue();
    Assertions.assertEquals(
        ParamTypeAction.PARAM_TYPE_ACTION_COLLAPSE,
        upsertedOverride
            .getAttributeOverridesList()
            .get(0)
            .getParamTypeOverride()
            .getParamTypeAction());
    Assertions.assertEquals(
        ParamTypeAction.PARAM_TYPE_ACTION_EXCLUDE,
        upsertedOverride
            .getAttributeOverridesList()
            .get(1)
            .getParamTypeOverride()
            .getParamTypeAction());
    Assertions.assertEquals(
        "param1",
        upsertedOverride.getAttributeOverridesList().get(0).getParamTypeOverride().getParamName());
    Assertions.assertEquals(
        "param2",
        upsertedOverride.getAttributeOverridesList().get(1).getParamTypeOverride().getParamName());
    assertEquals(2, upsertedOverride.getAttributeOverridesList().size());
  }

  @Test
  void apiAttributeOverrideAlreadyPresent_param_type() {
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

    List<AttributeOverride> attributeOverridesUpdate =
        List.of(
            AttributeOverride.newBuilder()
                .setParamTypeOverride(
                    ParamTypeOverride.newBuilder()
                        .setParamName("param1")
                        .setParamTypeAction(ParamTypeAction.PARAM_TYPE_ACTION_EXCLUDE))
                .build());

    UpsertApiAttributeOverridesRequest upsertRequest =
        UpsertApiAttributeOverridesRequest.newBuilder()
            .setApiId("api1")
            .addAllAttributeOverrides(attributeOverridesUpdate)
            .build();

    upsertApiAttributeOverridesHandler.upsertAttributeOverrides(
        RequestContext.forTenantId("tenantId"), upsertRequest);
    ArgumentCaptor<ApiAttributeOverrides> apiAttributeOverridesCaptor =
        ArgumentCaptor.forClass(ApiAttributeOverrides.class);
    verify(configServiceHandler)
        .upsertApiAttributeOverrides(any(), apiAttributeOverridesCaptor.capture());
    ApiAttributeOverrides upsertedOverride = apiAttributeOverridesCaptor.getValue();
    assertEquals(2, upsertedOverride.getAttributeOverridesList().size());
    Assertions.assertEquals(
        ParamTypeAction.PARAM_TYPE_ACTION_EXCLUDE,
        upsertedOverride
            .getAttributeOverridesList()
            .get(0)
            .getParamTypeOverride()
            .getParamTypeAction());
    Assertions.assertEquals(
        ParamTypeAction.PARAM_TYPE_ACTION_EXCLUDE,
        upsertedOverride
            .getAttributeOverridesList()
            .get(1)
            .getParamTypeOverride()
            .getParamTypeAction());
    Assertions.assertEquals(
        "param2",
        upsertedOverride.getAttributeOverridesList().get(0).getParamTypeOverride().getParamName());
    Assertions.assertEquals(
        "param1",
        upsertedOverride.getAttributeOverridesList().get(1).getParamTypeOverride().getParamName());
  }

  @Test
  void apiAttributeOverrideNotPresent_api_type() {
    when(configServiceHandler.getApiAttributeOverridesByApiId(any(), any()))
        .thenReturn(Optional.empty());

    List<AttributeOverride> attributeOverrides =
        List.of(
            AttributeOverride.newBuilder()
                .setApiExternalOverride(BooleanOverride.newBuilder().setValue(true).build())
                .build());

    UpsertApiAttributeOverridesRequest upsertRequest =
        UpsertApiAttributeOverridesRequest.newBuilder()
            .setApiId("api1")
            .addAllAttributeOverrides(attributeOverrides)
            .build();

    upsertApiAttributeOverridesHandler.upsertAttributeOverrides(
        RequestContext.forTenantId("tenant1"), upsertRequest);

    ArgumentCaptor<ApiAttributeOverrides> apiAttributeOverridesCaptor =
        ArgumentCaptor.forClass(ApiAttributeOverrides.class);
    verify(configServiceHandler)
        .upsertApiAttributeOverrides(any(), apiAttributeOverridesCaptor.capture());
    ApiAttributeOverrides upsertedOverride = apiAttributeOverridesCaptor.getValue();
    Assertions.assertTrue(
        upsertedOverride.getAttributeOverridesList().get(0).getApiExternalOverride().getValue());
    assertEquals(1, upsertedOverride.getAttributeOverridesList().size());
  }

  @Test
  void apiAttributeOverrideAlreadyPresent_api_type() {
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

    List<AttributeOverride> attributeOverridesUpdate =
        List.of(
            AttributeOverride.newBuilder()
                .setApiExternalOverride(BooleanOverride.newBuilder().setValue(true).build())
                .build());

    UpsertApiAttributeOverridesRequest upsertRequest =
        UpsertApiAttributeOverridesRequest.newBuilder()
            .setApiId("api1")
            .addAllAttributeOverrides(attributeOverridesUpdate)
            .build();

    upsertApiAttributeOverridesHandler.upsertAttributeOverrides(
        RequestContext.forTenantId("tenantId"), upsertRequest);
    ArgumentCaptor<ApiAttributeOverrides> apiAttributeOverridesCaptor =
        ArgumentCaptor.forClass(ApiAttributeOverrides.class);
    verify(configServiceHandler)
        .upsertApiAttributeOverrides(any(), apiAttributeOverridesCaptor.capture());
    ApiAttributeOverrides upsertedOverride = apiAttributeOverridesCaptor.getValue();
    assertEquals(3, upsertedOverride.getAttributeOverridesList().size());
    Assertions.assertEquals(
        ParamTypeAction.PARAM_TYPE_ACTION_COLLAPSE,
        upsertedOverride
            .getAttributeOverridesList()
            .get(0)
            .getParamTypeOverride()
            .getParamTypeAction());
    Assertions.assertEquals(
        ParamTypeAction.PARAM_TYPE_ACTION_EXCLUDE,
        upsertedOverride
            .getAttributeOverridesList()
            .get(1)
            .getParamTypeOverride()
            .getParamTypeAction());
    Assertions.assertTrue(
        upsertedOverride.getAttributeOverridesList().get(2).getApiExternalOverride().getValue());
    Assertions.assertEquals(
        "param1",
        upsertedOverride.getAttributeOverridesList().get(0).getParamTypeOverride().getParamName());
    Assertions.assertEquals(
        "param2",
        upsertedOverride.getAttributeOverridesList().get(1).getParamTypeOverride().getParamName());
  }

  @Test
  void apiAttributeOverrideAlreadyPresent_api_type_override() {
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
                .build(),
            AttributeOverride.newBuilder()
                .setApiExternalOverride(BooleanOverride.newBuilder().setValue(false).build())
                .build());
    when(configServiceHandler.getApiAttributeOverridesByApiId(any(), any()))
        .thenReturn(
            Optional.of(
                ApiAttributeOverrides.newBuilder()
                    .addAllAttributeOverrides(attributeOverrides)
                    .setApiId("api1")
                    .build()));

    List<AttributeOverride> attributeOverridesUpdate =
        List.of(
            AttributeOverride.newBuilder()
                .setApiExternalOverride(BooleanOverride.newBuilder().setValue(true).build())
                .build());

    UpsertApiAttributeOverridesRequest upsertRequest =
        UpsertApiAttributeOverridesRequest.newBuilder()
            .setApiId("api1")
            .addAllAttributeOverrides(attributeOverridesUpdate)
            .build();

    upsertApiAttributeOverridesHandler.upsertAttributeOverrides(
        RequestContext.forTenantId("tenantId"), upsertRequest);
    ArgumentCaptor<ApiAttributeOverrides> apiAttributeOverridesCaptor =
        ArgumentCaptor.forClass(ApiAttributeOverrides.class);
    verify(configServiceHandler)
        .upsertApiAttributeOverrides(any(), apiAttributeOverridesCaptor.capture());
    ApiAttributeOverrides upsertedOverride = apiAttributeOverridesCaptor.getValue();
    assertEquals(3, upsertedOverride.getAttributeOverridesList().size());
    Assertions.assertEquals(
        ParamTypeAction.PARAM_TYPE_ACTION_COLLAPSE,
        upsertedOverride
            .getAttributeOverridesList()
            .get(0)
            .getParamTypeOverride()
            .getParamTypeAction());
    Assertions.assertEquals(
        ParamTypeAction.PARAM_TYPE_ACTION_EXCLUDE,
        upsertedOverride
            .getAttributeOverridesList()
            .get(1)
            .getParamTypeOverride()
            .getParamTypeAction());
    Assertions.assertTrue(
        upsertedOverride.getAttributeOverridesList().get(2).getApiExternalOverride().getValue());
    Assertions.assertEquals(
        "param1",
        upsertedOverride.getAttributeOverridesList().get(0).getParamTypeOverride().getParamName());
    Assertions.assertEquals(
        "param2",
        upsertedOverride.getAttributeOverridesList().get(1).getParamTypeOverride().getParamName());
  }
}
