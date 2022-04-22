package ai.traceable.api.attribute.override.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.api.attribute.override.service.v1.AttributeOverride;
import ai.traceable.api.attribute.override.service.v1.AttributeOverrideIdentifier;
import ai.traceable.api.attribute.override.service.v1.BooleanOverride;
import ai.traceable.api.attribute.override.service.v1.EmptyOverrideIdentifier;
import ai.traceable.api.attribute.override.service.v1.Filter;
import ai.traceable.api.attribute.override.service.v1.GetAllApiAttributeOverridesRequest;
import ai.traceable.api.attribute.override.service.v1.ParamTypeAction;
import ai.traceable.api.attribute.override.service.v1.ParamTypeOverride;
import ai.traceable.api.attribute.override.service.v1.ParamTypeOverrideIdentifier;
import ai.traceable.api.attribute.override.service.v1.RemoveApiAttributeOverridesRequest;
import ai.traceable.api.attribute.override.service.v1.UpsertApiAttributeOverridesRequest;
import io.grpc.Status;
import java.util.List;
import org.junit.jupiter.api.Test;

public class ApiAttributeOverrideRequestValidatorTest {
  ApiAttributeOverrideRequestValidator requestValidator =
      new ApiAttributeOverrideRequestValidator();

  @Test
  void testGetAllOverridesRequest() {
    GetAllApiAttributeOverridesRequest request =
        GetAllApiAttributeOverridesRequest.newBuilder()
            .setFilter(Filter.newBuilder().addAllApiIds(List.of("")).build())
            .build();
    assertEquals(Status.INVALID_ARGUMENT.getCode(), requestValidator.validate(request).getCode());

    request =
        GetAllApiAttributeOverridesRequest.newBuilder()
            .setFilter(Filter.newBuilder().addAllApiIds(List.of("apiId")).build())
            .build();
    assertEquals(Status.OK.getCode(), requestValidator.validate(request).getCode());

    request = GetAllApiAttributeOverridesRequest.newBuilder().build();
    assertEquals(Status.OK.getCode(), requestValidator.validate(request).getCode());
  }

  @Test
  void testUpsertRequestValidation() {
    UpsertApiAttributeOverridesRequest request =
        UpsertApiAttributeOverridesRequest.newBuilder().build();
    assertEquals(Status.INVALID_ARGUMENT.getCode(), requestValidator.validate(request).getCode());

    request = UpsertApiAttributeOverridesRequest.newBuilder().setApiId("apiId").build();
    assertEquals(Status.INVALID_ARGUMENT.getCode(), requestValidator.validate(request).getCode());

    request =
        UpsertApiAttributeOverridesRequest.newBuilder()
            .setApiId("apiId")
            .addAllAttributeOverrides(List.of(AttributeOverride.getDefaultInstance()))
            .build();
    assertEquals(Status.INVALID_ARGUMENT.getCode(), requestValidator.validate(request).getCode());

    request =
        UpsertApiAttributeOverridesRequest.newBuilder()
            .setApiId("apiId")
            .addAllAttributeOverrides(
                List.of(
                    AttributeOverride.newBuilder()
                        .setParamTypeOverride(ParamTypeOverride.newBuilder().build())
                        .build()))
            .build();
    assertEquals(Status.INVALID_ARGUMENT.getCode(), requestValidator.validate(request).getCode());

    request =
        UpsertApiAttributeOverridesRequest.newBuilder()
            .setApiId("apiId")
            .addAllAttributeOverrides(
                List.of(
                    AttributeOverride.newBuilder()
                        .setParamTypeOverride(
                            ParamTypeOverride.newBuilder()
                                .setParamName("param1")
                                .setParamTypeAction(ParamTypeAction.PARAM_TYPE_ACTION_EXCLUDE)
                                .build())
                        .build()))
            .build();
    assertEquals(Status.OK.getCode(), requestValidator.validate(request).getCode());

    request =
        UpsertApiAttributeOverridesRequest.newBuilder()
            .setApiId("apiId")
            .addAllAttributeOverrides(
                List.of(
                    AttributeOverride.newBuilder()
                        .setApiExternalOverride(BooleanOverride.newBuilder().build())
                        .build()))
            .build();
    assertEquals(Status.OK.getCode(), requestValidator.validate(request).getCode());
  }

  @Test
  void testRemoveAttributeRequestValidation() {
    RemoveApiAttributeOverridesRequest request =
        RemoveApiAttributeOverridesRequest.newBuilder().build();
    assertEquals(Status.INVALID_ARGUMENT.getCode(), requestValidator.validate(request).getCode());

    request = RemoveApiAttributeOverridesRequest.newBuilder().setApiId("apiId").build();
    assertEquals(Status.INVALID_ARGUMENT.getCode(), requestValidator.validate(request).getCode());

    request =
        RemoveApiAttributeOverridesRequest.newBuilder()
            .setApiId("apiId")
            .addAllAttributeOverrideIdentifiers(
                List.of(
                    AttributeOverrideIdentifier.newBuilder()
                        .setParamTypeOverrideIdentifier(
                            ParamTypeOverrideIdentifier.newBuilder().build())
                        .build()))
            .build();
    assertEquals(Status.INVALID_ARGUMENT.getCode(), requestValidator.validate(request).getCode());

    request =
        RemoveApiAttributeOverridesRequest.newBuilder()
            .setApiId("apiId")
            .addAllAttributeOverrideIdentifiers(
                List.of(
                    AttributeOverrideIdentifier.newBuilder()
                        .setParamTypeOverrideIdentifier(
                            ParamTypeOverrideIdentifier.newBuilder().setParamName("param").build())
                        .build()))
            .build();
    assertEquals(Status.OK.getCode(), requestValidator.validate(request).getCode());

    request =
        RemoveApiAttributeOverridesRequest.newBuilder()
            .setApiId("apiId")
            .addAllAttributeOverrideIdentifiers(
                List.of(
                    AttributeOverrideIdentifier.newBuilder()
                        .setApiExternalOverrideIdentifier(
                            EmptyOverrideIdentifier.newBuilder().build())
                        .build()))
            .build();
    assertEquals(Status.OK.getCode(), requestValidator.validate(request).getCode());
  }
}
