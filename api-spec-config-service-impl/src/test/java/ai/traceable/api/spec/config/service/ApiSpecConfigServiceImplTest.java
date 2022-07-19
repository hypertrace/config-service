package ai.traceable.api.spec.config.service;

import static ai.traceable.api.spec.config.service.v1.ApiSpecStatus.API_SPEC_STATUS_COMPLETED;
import static ai.traceable.api.spec.config.service.v1.ApiSpecStatus.API_SPEC_STATUS_IN_PROGRESS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.api.spec.config.service.store.ApiSpecConfigStore;
import ai.traceable.api.spec.config.service.v1.ApiSpec;
import ai.traceable.api.spec.config.service.v1.ApiSpecConfigServiceGrpc;
import ai.traceable.api.spec.config.service.v1.CreateApiSpec;
import ai.traceable.api.spec.config.service.v1.CreateApiSpecRequest;
import ai.traceable.api.spec.config.service.v1.DeleteApiSpecRequest;
import ai.traceable.api.spec.config.service.v1.GetApiSpecRequest;
import ai.traceable.api.spec.config.service.v1.GetApiSpecsRequest;
import ai.traceable.api.spec.config.service.v1.UpdateApiSpec;
import ai.traceable.api.spec.config.service.v1.UpdateApiSpecRequest;
import ai.traceable.api.spec.config.service.validation.ApiSpecConfigRequestValidator;
import ai.traceable.config.utils.TimestampConverter;
import com.google.protobuf.Timestamp;
import java.util.List;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ApiSpecConfigServiceImplTest {

  private ApiSpecConfigServiceGrpc.ApiSpecConfigServiceBlockingStub
      apiSpecConfigServiceBlockingStub;

  @BeforeEach
  void beforeEach() {
    MockGenericConfigService mockGenericConfigService =
        new MockGenericConfigService()
            .mockUpsert()
            .mockGet()
            .mockGetAll()
            .mockDelete()
            .mockUpsertAll();

    ConfigServiceGrpc.ConfigServiceBlockingStub genericStub =
        ConfigServiceGrpc.newBlockingStub(mockGenericConfigService.channel());
    TimestampConverter timestampConverter = mock(TimestampConverter.class);

    ApiSpecConfigStore apiSpecConfigStore = new ApiSpecConfigStore(genericStub, timestampConverter);

    mockGenericConfigService
        .addService(
            new ApiSpecConfigServiceImpl(
                new ApiSpecConfigRequestValidator(), apiSpecConfigStore, timestampConverter))
        .start();

    this.apiSpecConfigServiceBlockingStub =
        ApiSpecConfigServiceGrpc.newBlockingStub(mockGenericConfigService.channel());

    when(timestampConverter.convert(any()))
        .thenReturn(Timestamp.newBuilder().setSeconds(100).build());
  }

  @Test
  void testApiSpecCrud() {
    ApiSpec firstCreatedApiSpec =
        this.apiSpecConfigServiceBlockingStub
            .createApiSpec(
                CreateApiSpecRequest.newBuilder()
                    .setCreateApiSpec(
                        CreateApiSpec.newBuilder()
                            .setName("spec1")
                            .setApiNamingEnabled(true)
                            .setStatus(API_SPEC_STATUS_IN_PROGRESS))
                    .build())
            .getApiSpec();
    Timestamp expectedTimestamp = Timestamp.newBuilder().setSeconds(100).build();
    assertEquals(expectedTimestamp, firstCreatedApiSpec.getCreationTimestamp());
    assertEquals(expectedTimestamp, firstCreatedApiSpec.getLastUpdatedTimestamp());

    ApiSpec secondCreatedApiSpec =
        this.apiSpecConfigServiceBlockingStub
            .createApiSpec(
                CreateApiSpecRequest.newBuilder()
                    .setCreateApiSpec(
                        CreateApiSpec.newBuilder()
                            .setName("spec2")
                            .setApiNamingEnabled(true)
                            .setStatus(API_SPEC_STATUS_COMPLETED))
                    .build())
            .getApiSpec();

    List<ApiSpec> apiSpecs =
        this.apiSpecConfigServiceBlockingStub
            .getApiSpecs(GetApiSpecsRequest.newBuilder().build())
            .getApiSpecsList();
    assertEquals(2, apiSpecs.size());
    assertTrue(apiSpecs.contains(firstCreatedApiSpec));
    assertTrue(apiSpecs.contains(secondCreatedApiSpec));
    assertEquals(
        ApiSpec.newBuilder(
                this.apiSpecConfigServiceBlockingStub
                    .getApiSpec(
                        GetApiSpecRequest.newBuilder()
                            .setId(firstCreatedApiSpec.getSpecId())
                            .build())
                    .getApiSpec())
            .setCreationTimestamp(expectedTimestamp)
            .setLastUpdatedTimestamp(expectedTimestamp)
            .build(),
        firstCreatedApiSpec);
    assertEquals(
        ApiSpec.newBuilder(
                this.apiSpecConfigServiceBlockingStub
                    .getApiSpec(
                        GetApiSpecRequest.newBuilder()
                            .setId(secondCreatedApiSpec.getSpecId())
                            .build())
                    .getApiSpec())
            .setCreationTimestamp(expectedTimestamp)
            .setLastUpdatedTimestamp(expectedTimestamp)
            .build(),
        secondCreatedApiSpec);

    ApiSpec updatedFirstApiSpec =
        this.apiSpecConfigServiceBlockingStub
            .updateApiSpec(
                UpdateApiSpecRequest.newBuilder()
                    .setApiSpec(
                        UpdateApiSpec.newBuilder()
                            .setSpecId(firstCreatedApiSpec.getSpecId())
                            .setName("updatedSpec1")
                            .setApiNamingEnabled(false)
                            .setStatus(API_SPEC_STATUS_COMPLETED))
                    .build())
            .getApiSpec();
    assertEquals("updatedSpec1", updatedFirstApiSpec.getName());
    assertFalse(updatedFirstApiSpec.getApiNamingEnabled());
    assertEquals(API_SPEC_STATUS_COMPLETED, updatedFirstApiSpec.getStatus());
    assertEquals(
        ApiSpec.newBuilder(
                this.apiSpecConfigServiceBlockingStub
                    .getApiSpec(
                        GetApiSpecRequest.newBuilder()
                            .setId(firstCreatedApiSpec.getSpecId())
                            .build())
                    .getApiSpec())
            .setCreationTimestamp(expectedTimestamp)
            .setLastUpdatedTimestamp(expectedTimestamp)
            .build(),
        updatedFirstApiSpec);

    apiSpecs =
        this.apiSpecConfigServiceBlockingStub
            .getApiSpecs(GetApiSpecsRequest.newBuilder().build())
            .getApiSpecsList();
    assertEquals(2, apiSpecs.size());
    assertTrue(apiSpecs.contains(updatedFirstApiSpec));

    this.apiSpecConfigServiceBlockingStub.deleteApiSpec(
        DeleteApiSpecRequest.newBuilder().setSpecId(firstCreatedApiSpec.getSpecId()).build());

    apiSpecs =
        this.apiSpecConfigServiceBlockingStub
            .getApiSpecs(GetApiSpecsRequest.newBuilder().build())
            .getApiSpecsList();
    assertEquals(1, apiSpecs.size());
    assertEquals(secondCreatedApiSpec, apiSpecs.get(0));
  }
}
