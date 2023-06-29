package ai.traceable.config.service;

import ai.traceable.api.attribute.override.service.v1.ApiAttributeOverrideServiceGrpc;
import ai.traceable.api.attribute.override.service.v1.ApiAttributeOverrideServiceGrpc.ApiAttributeOverrideServiceBlockingStub;
import ai.traceable.api.attribute.override.service.v1.AttributeOverride;
import ai.traceable.api.attribute.override.service.v1.AttributeOverrideIdentifier;
import ai.traceable.api.attribute.override.service.v1.Filter;
import ai.traceable.api.attribute.override.service.v1.GetAllApiAttributeOverridesRequest;
import ai.traceable.api.attribute.override.service.v1.GetAllApiAttributeOverridesResponse;
import ai.traceable.api.attribute.override.service.v1.ParamTypeAction;
import ai.traceable.api.attribute.override.service.v1.ParamTypeOverride;
import ai.traceable.api.attribute.override.service.v1.ParamTypeOverrideIdentifier;
import ai.traceable.api.attribute.override.service.v1.RemoveApiAttributeOverridesRequest;
import ai.traceable.api.attribute.override.service.v1.UpsertApiAttributeOverridesRequest;
import java.util.List;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ApiAttributeOverrideConfigServiceIntegrationTest
    extends TraceableConfigServiceIntegrationTestBase {
  private static ApiAttributeOverrideServiceBlockingStub overrideServiceStub;

  @BeforeAll
  static void init() {
    overrideServiceStub =
        ApiAttributeOverrideServiceGrpc.newBlockingStub(channelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Test
  public void testApiAttributeOverrides() {
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

    // upsert the overrides
    RequestContext.forTenantId(TENANT_ID)
        .call(
            () ->
                overrideServiceStub.upsertApiAttributeOverrides(
                    UpsertApiAttributeOverridesRequest.newBuilder()
                        .addAllAttributeOverrides(attributeOverrides)
                        .setApiId("api1")
                        .build()));

    // get all upserted overrides
    GetAllApiAttributeOverridesResponse response =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    overrideServiceStub.getAllApiAttributeOverrides(
                        GetAllApiAttributeOverridesRequest.newBuilder().build()));

    Assertions.assertEquals(1, response.getApiAttributeOverridesCount());

    List<AttributeOverride> api1OverrideList =
        response.getApiAttributeOverridesMap().get("api1").getAttributeOverridesList();
    Assertions.assertEquals(
        ParamTypeAction.PARAM_TYPE_ACTION_COLLAPSE,
        api1OverrideList.get(0).getParamTypeOverride().getParamTypeAction());
    Assertions.assertEquals(
        ParamTypeAction.PARAM_TYPE_ACTION_EXCLUDE,
        api1OverrideList.get(1).getParamTypeOverride().getParamTypeAction());

    // update the param1 type override
    List<AttributeOverride> attributeOverridesUpdates =
        List.of(
            AttributeOverride.newBuilder()
                .setParamTypeOverride(
                    ParamTypeOverride.newBuilder()
                        .setParamName("param1")
                        .setParamTypeAction(ParamTypeAction.PARAM_TYPE_ACTION_EXCLUDE))
                .build());

    // upsert the update
    RequestContext.forTenantId(TENANT_ID)
        .call(
            () ->
                overrideServiceStub.upsertApiAttributeOverrides(
                    UpsertApiAttributeOverridesRequest.newBuilder()
                        .addAllAttributeOverrides(attributeOverridesUpdates)
                        .setApiId("api1")
                        .build()));

    response =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    overrideServiceStub.getAllApiAttributeOverrides(
                        GetAllApiAttributeOverridesRequest.newBuilder().build()));

    api1OverrideList =
        response.getApiAttributeOverridesMap().get("api1").getAttributeOverridesList();
    Assertions.assertEquals(
        ParamTypeAction.PARAM_TYPE_ACTION_EXCLUDE,
        api1OverrideList.get(0).getParamTypeOverride().getParamTypeAction());
    Assertions.assertEquals(
        ParamTypeAction.PARAM_TYPE_ACTION_EXCLUDE,
        api1OverrideList.get(1).getParamTypeOverride().getParamTypeAction());

    // Remove param1 override
    RemoveApiAttributeOverridesRequest removeAttributeRequest =
        RemoveApiAttributeOverridesRequest.newBuilder()
            .setApiId("api1")
            .addAllAttributeOverrideIdentifiers(
                List.of(
                    AttributeOverrideIdentifier.newBuilder()
                        .setParamTypeOverrideIdentifier(
                            ParamTypeOverrideIdentifier.newBuilder().setParamName("param1"))
                        .build()))
            .build();

    RequestContext.forTenantId(TENANT_ID)
        .call(() -> overrideServiceStub.removeApiAttributeOverrides(removeAttributeRequest));

    response =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    overrideServiceStub.getAllApiAttributeOverrides(
                        GetAllApiAttributeOverridesRequest.newBuilder()
                            .setFilter(Filter.newBuilder().addAllApiIds(List.of("api1")).build())
                            .build()));

    api1OverrideList =
        response.getApiAttributeOverridesMap().get("api1").getAttributeOverridesList();
    Assertions.assertEquals(1, api1OverrideList.size());
    Assertions.assertEquals(
        ParamTypeAction.PARAM_TYPE_ACTION_EXCLUDE,
        api1OverrideList.get(0).getParamTypeOverride().getParamTypeAction());
  }
}
