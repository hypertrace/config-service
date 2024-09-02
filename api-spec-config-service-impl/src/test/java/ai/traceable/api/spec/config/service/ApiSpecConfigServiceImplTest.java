package ai.traceable.api.spec.config.service;

import static ai.traceable.api.spec.config.service.v1.ApiSpecStatus.API_SPEC_STATUS_COMPLETED;
import static ai.traceable.api.spec.config.service.v1.ApiSpecStatus.API_SPEC_STATUS_IN_PROGRESS;
import static ai.traceable.api.spec.config.service.v1.ApiSpecStatus.API_SPEC_STATUS_UPLOAD_COMPLETED;
import static ai.traceable.api.spec.config.service.v1.ApiSpecStatus.API_SPEC_STATUS_UPLOAD_IN_PROGRESS;
import static ai.traceable.api.spec.config.service.v1.OpenApiSpecResolutionState.OPEN_API_SPEC_RESOLUTION_STATE_INCOMPLETE;
import static ai.traceable.api.spec.config.service.v1.SpecType.SPEC_TYPE_OPEN_API_SPEC;
import static ai.traceable.api.spec.config.service.v1.SpecType.SPEC_TYPE_POSTMAN_COLLECTION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.api.spec.config.service.converter.ApiSpecStatusConverter;
import ai.traceable.api.spec.config.service.store.ApiSpecConfigStore;
import ai.traceable.api.spec.config.service.v1.ApiSpec;
import ai.traceable.api.spec.config.service.v1.ApiSpecConfigServiceGrpc;
import ai.traceable.api.spec.config.service.v1.ApiSpecFilter;
import ai.traceable.api.spec.config.service.v1.ApiSpecMetadata;
import ai.traceable.api.spec.config.service.v1.ApiSpecStatusFilter;
import ai.traceable.api.spec.config.service.v1.ApiSpecUpdate;
import ai.traceable.api.spec.config.service.v1.BulkUpdateApiSpecsRequest;
import ai.traceable.api.spec.config.service.v1.CreateApiSpec;
import ai.traceable.api.spec.config.service.v1.CreateApiSpecRequest;
import ai.traceable.api.spec.config.service.v1.DeleteApiSpecRequest;
import ai.traceable.api.spec.config.service.v1.DeleteApiSpecsRequest;
import ai.traceable.api.spec.config.service.v1.FileContentSha256Filter;
import ai.traceable.api.spec.config.service.v1.GetApiSpecRequest;
import ai.traceable.api.spec.config.service.v1.GetApiSpecsRequest;
import ai.traceable.api.spec.config.service.v1.IncompleteOpenApiSpecReference;
import ai.traceable.api.spec.config.service.v1.MissingOpenApiSpecReference;
import ai.traceable.api.spec.config.service.v1.OpenApiSpecMetadata;
import ai.traceable.api.spec.config.service.v1.OpenApiSpecReference;
import ai.traceable.api.spec.config.service.v1.ReferenceApiSpec;
import ai.traceable.api.spec.config.service.v1.ReferenceApiSpecFilter;
import ai.traceable.api.spec.config.service.v1.ReferenceType;
import ai.traceable.api.spec.config.service.v1.ReferenceTypeFilter;
import ai.traceable.api.spec.config.service.v1.SpecResolutionStateFilter;
import ai.traceable.api.spec.config.service.v1.SpecType;
import ai.traceable.api.spec.config.service.v1.SpecTypeFilter;
import ai.traceable.api.spec.config.service.v1.StringList;
import ai.traceable.api.spec.config.service.v1.UpdateApiSpec;
import ai.traceable.api.spec.config.service.v1.UpdateApiSpecRequest;
import ai.traceable.api.spec.config.service.v1.UpdateApiSpecsRequest;
import ai.traceable.api.spec.config.service.v1.UpdatedApiSpecField;
import ai.traceable.api.spec.config.service.validation.ApiSpecConfigRequestValidator;
import ai.traceable.config.utils.TimestampConverter;
import com.google.protobuf.Timestamp;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ApiSpecConfigServiceImplTest {

  private ApiSpecConfigServiceGrpc.ApiSpecConfigServiceBlockingStub
      apiSpecConfigServiceBlockingStub;
  private final int TEST_MAX_ALLOWED_SPECS_PER_TENANT = 7;

  @BeforeEach
  void beforeEach() {
    MockGenericConfigService mockGenericConfigService =
        new MockGenericConfigService()
            .mockUpsert()
            .mockGet()
            .mockGetAll()
            .mockDelete()
            .mockDeleteAll()
            .mockUpsertAll();

    ConfigServiceGrpc.ConfigServiceBlockingStub genericStub =
        ConfigServiceGrpc.newBlockingStub(mockGenericConfigService.channel());
    TimestampConverter timestampConverter = mock(TimestampConverter.class);
    ConfigChangeEventGenerator configChangeEventGenerator = mock(ConfigChangeEventGenerator.class);

    ApiSpecStatusConverter apiSpecStatusConverter = new ApiSpecStatusConverter();
    ApiSpecConfigStore apiSpecConfigStore =
        new ApiSpecConfigStore(
            genericStub, timestampConverter, configChangeEventGenerator, apiSpecStatusConverter);
    Config testConfig =
        ConfigFactory.parseMap(
            Map.of("maxAllowedSpecsPerTenant", TEST_MAX_ALLOWED_SPECS_PER_TENANT));
    mockGenericConfigService
        .addService(
            new ApiSpecConfigServiceImpl(
                new ApiSpecConfigRequestValidator(),
                apiSpecConfigStore,
                timestampConverter,
                new ApiSpecConfig(testConfig),
                apiSpecStatusConverter))
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
                            .setStatus(API_SPEC_STATUS_IN_PROGRESS)
                            .setSpecPath("/test/spec1.json"))
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
                            .setStatus(API_SPEC_STATUS_COMPLETED)
                            .setSpecPath("/test/spec2.json"))
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
                            .setApiNamingEnabled(false))
                    .build())
            .getApiSpec();
    assertEquals("updatedSpec1", updatedFirstApiSpec.getName());
    assertFalse(updatedFirstApiSpec.getApiNamingEnabled());
    assertEquals(API_SPEC_STATUS_UPLOAD_IN_PROGRESS, updatedFirstApiSpec.getStatus());

    updatedFirstApiSpec =
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
    assertEquals(API_SPEC_STATUS_UPLOAD_COMPLETED, updatedFirstApiSpec.getStatus());

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

  @Test
  void testCreateApiSpecWithFileHash() {
    ApiSpec createdApiSpec =
        this.apiSpecConfigServiceBlockingStub
            .createApiSpec(
                CreateApiSpecRequest.newBuilder()
                    .setCreateApiSpec(
                        CreateApiSpec.newBuilder()
                            .setName("spec1")
                            .setApiNamingEnabled(true)
                            .setStatus(API_SPEC_STATUS_IN_PROGRESS)
                            .setSpecPath("/test/spec1.json")
                            .setFileContentSha256(
                                "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"))
                    .build())
            .getApiSpec();
    Timestamp expectedTimestamp = Timestamp.newBuilder().setSeconds(100).build();
    assertEquals(expectedTimestamp, createdApiSpec.getCreationTimestamp());
    assertEquals(expectedTimestamp, createdApiSpec.getLastUpdatedTimestamp());
    assertEquals(
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        createdApiSpec.getFileContentSha256());

    // Check if spec-config with same file hash is uploaded, exception is thrown.
    assertThrows(
        StatusRuntimeException.class,
        () ->
            this.apiSpecConfigServiceBlockingStub.createApiSpec(
                CreateApiSpecRequest.newBuilder()
                    .setCreateApiSpec(
                        CreateApiSpec.newBuilder()
                            .setName("spec2")
                            .setApiNamingEnabled(true)
                            .setStatus(API_SPEC_STATUS_IN_PROGRESS)
                            .setSpecPath("/test/spec2.json")
                            .setFileContentSha256(
                                "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"))
                    .build()));

    // Check if hash is not SHA256, error is thrown.
    assertThrows(
        StatusRuntimeException.class,
        () ->
            this.apiSpecConfigServiceBlockingStub.createApiSpec(
                CreateApiSpecRequest.newBuilder()
                    .setCreateApiSpec(
                        CreateApiSpec.newBuilder()
                            .setName("spec3")
                            .setApiNamingEnabled(true)
                            .setStatus(API_SPEC_STATUS_IN_PROGRESS)
                            .setSpecPath("/test/spec3.json")
                            .setFileContentSha256("filehash1"))
                    .build()));

    ApiSpec updatedApiSpec =
        this.apiSpecConfigServiceBlockingStub
            .updateApiSpec(
                UpdateApiSpecRequest.newBuilder()
                    .setApiSpec(
                        UpdateApiSpec.newBuilder()
                            .setSpecId(createdApiSpec.getSpecId())
                            .setName("updatedSpec1")
                            .setApiNamingEnabled(false))
                    .build())
            .getApiSpec();
    assertEquals("updatedSpec1", updatedApiSpec.getName());
    assertFalse(updatedApiSpec.getApiNamingEnabled());
    assertEquals(
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        updatedApiSpec.getFileContentSha256());
    assertEquals(API_SPEC_STATUS_UPLOAD_IN_PROGRESS, updatedApiSpec.getStatus());
  }

  @Test
  void testCreateApiSpecWithSameSpecPath() {
    ApiSpec createdApiSpec =
        this.apiSpecConfigServiceBlockingStub
            .createApiSpec(
                CreateApiSpecRequest.newBuilder()
                    .setCreateApiSpec(
                        CreateApiSpec.newBuilder()
                            .setName("spec1")
                            .setApiNamingEnabled(true)
                            .setStatus(API_SPEC_STATUS_IN_PROGRESS)
                            .setSpecPath("/test/spec1.json"))
                    .build())
            .getApiSpec();
    Timestamp expectedTimestamp = Timestamp.newBuilder().setSeconds(100).build();
    assertEquals(expectedTimestamp, createdApiSpec.getCreationTimestamp());
    assertEquals(expectedTimestamp, createdApiSpec.getLastUpdatedTimestamp());
    assertEquals("/test/spec1.json", createdApiSpec.getSpecPath());

    // Check if spec-config with same spec path is uploaded, exception is thrown.
    assertThrows(
        StatusRuntimeException.class,
        () ->
            this.apiSpecConfigServiceBlockingStub.createApiSpec(
                CreateApiSpecRequest.newBuilder()
                    .setCreateApiSpec(
                        CreateApiSpec.newBuilder()
                            .setName("spec2")
                            .setApiNamingEnabled(true)
                            .setStatus(API_SPEC_STATUS_IN_PROGRESS)
                            .setSpecPath("/test/spec1.json"))
                    .build()));

    ApiSpec updatedApiSpec =
        this.apiSpecConfigServiceBlockingStub
            .updateApiSpec(
                UpdateApiSpecRequest.newBuilder()
                    .setApiSpec(
                        UpdateApiSpec.newBuilder()
                            .setSpecId(createdApiSpec.getSpecId())
                            .setName("updatedSpec1")
                            .setApiNamingEnabled(false))
                    .build())
            .getApiSpec();
    assertEquals("updatedSpec1", updatedApiSpec.getName());
    assertFalse(updatedApiSpec.getApiNamingEnabled());
    assertEquals("/test/spec1.json", updatedApiSpec.getSpecPath());
    assertEquals(API_SPEC_STATUS_UPLOAD_IN_PROGRESS, updatedApiSpec.getStatus());
  }

  @Test
  void testGetApiSpecsWithSpecPathFilter() {
    assertTrue(
        this.apiSpecConfigServiceBlockingStub
                .getApiSpecs(
                    GetApiSpecsRequest.newBuilder()
                        .setApiSpecFilter(
                            ApiSpecFilter.newBuilder()
                                .setSpecPaths(
                                    StringList.newBuilder()
                                        .addAllValues(
                                            List.of("/test/spec1.json", "/test/spec2.json"))))
                        .build())
                .getApiSpecsCount()
            == 0);
    ApiSpec createdApiSpec =
        this.apiSpecConfigServiceBlockingStub
            .createApiSpec(
                CreateApiSpecRequest.newBuilder()
                    .setCreateApiSpec(
                        CreateApiSpec.newBuilder()
                            .setName("spec1")
                            .setApiNamingEnabled(true)
                            .setStatus(API_SPEC_STATUS_IN_PROGRESS)
                            .setSpecPath("/test/spec1.json")
                            .setFileContentSha256(
                                "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"))
                    .build())
            .getApiSpec();
    assertEquals(
        createdApiSpec,
        this.apiSpecConfigServiceBlockingStub
            .getApiSpecs(
                GetApiSpecsRequest.newBuilder()
                    .setApiSpecFilter(
                        ApiSpecFilter.newBuilder()
                            .setSpecPaths(
                                StringList.newBuilder()
                                    .addAllValues(List.of("/test/spec1.json", "/test/spec2.json"))))
                    .build())
            .getApiSpecs(0));
    assertTrue(
        this.apiSpecConfigServiceBlockingStub
                .getApiSpecs(
                    GetApiSpecsRequest.newBuilder()
                        .setApiSpecFilter(
                            ApiSpecFilter.newBuilder()
                                .setSpecPaths(
                                    StringList.newBuilder()
                                        .addAllValues(
                                            List.of("/test/spec2.json", "/test/spec3.json"))))
                        .build())
                .getApiSpecsCount()
            == 0);
  }

  @Test
  void testGetApiSpecsWithFilter() {
    assertTrue(
        this.apiSpecConfigServiceBlockingStub
                .getApiSpecs(
                    GetApiSpecsRequest.newBuilder()
                        .setApiSpecFilter(
                            ApiSpecFilter.newBuilder()
                                .setFileContentSha256(
                                    FileContentSha256Filter.newBuilder()
                                        .addFileContentSha256(
                                            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")))
                        .build())
                .getApiSpecsCount()
            == 0);
    ApiSpec firstCreatedApiSpec =
        this.apiSpecConfigServiceBlockingStub
            .createApiSpec(
                CreateApiSpecRequest.newBuilder()
                    .setCreateApiSpec(
                        CreateApiSpec.newBuilder()
                            .setName("spec1")
                            .setApiNamingEnabled(true)
                            .setStatus(API_SPEC_STATUS_IN_PROGRESS)
                            .setSpecPath("/test/spec1.json")
                            .setFileContentSha256(
                                "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
                            .setSpecType(SPEC_TYPE_OPEN_API_SPEC))
                    .build())
            .getApiSpec();
    // Filter by file hash
    assertEquals(
        firstCreatedApiSpec,
        this.apiSpecConfigServiceBlockingStub
            .getApiSpecs(
                GetApiSpecsRequest.newBuilder()
                    .setApiSpecFilter(
                        ApiSpecFilter.newBuilder()
                            .setFileContentSha256(
                                FileContentSha256Filter.newBuilder()
                                    .addFileContentSha256(
                                        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")))
                    .build())
            .getApiSpecs(0));

    ApiSpec secondCreatedApiSpec =
        this.apiSpecConfigServiceBlockingStub
            .createApiSpec(
                CreateApiSpecRequest.newBuilder()
                    .setCreateApiSpec(
                        CreateApiSpec.newBuilder()
                            .setName("spec2")
                            .setApiNamingEnabled(true)
                            .setStatus(API_SPEC_STATUS_COMPLETED)
                            .setSpecPath("/test/spec2.json")
                            .setFileContentSha256(
                                "5e50280084181a7a3e44c8093b367d4b79e6b6d19e575bfc956a7714b58196ab")
                            .setApiInspectorDisabled(!firstCreatedApiSpec.getApiInspectorDisabled())
                            .setSpecType(SPEC_TYPE_OPEN_API_SPEC))
                    .build())
            .getApiSpec();
    secondCreatedApiSpec =
        this.apiSpecConfigServiceBlockingStub
            .bulkUpdateApiSpecs(
                BulkUpdateApiSpecsRequest.newBuilder()
                    .addAllApiSpecs(
                        List.of(
                            ApiSpecUpdate.newBuilder()
                                .setSpecId(secondCreatedApiSpec.getSpecId())
                                .addAllUpdatedApiSpecFields(
                                    List.of(
                                        UpdatedApiSpecField.newBuilder()
                                            .setApiSpecMetadata(
                                                ApiSpecMetadata.newBuilder()
                                                    .setOpenApiSpecMetadata(
                                                        OpenApiSpecMetadata.newBuilder()
                                                            .setOpenApiSpecResolutionState(
                                                                OPEN_API_SPEC_RESOLUTION_STATE_INCOMPLETE)
                                                            .addAllOpenApiSpecReferences(
                                                                List.of(
                                                                    OpenApiSpecReference
                                                                        .newBuilder()
                                                                        .setResolvedSpecPath(
                                                                            firstCreatedApiSpec
                                                                                .getSpecPath())
                                                                        .setMissingOpenApiSpecReference(
                                                                            MissingOpenApiSpecReference
                                                                                .getDefaultInstance())
                                                                        .build()))))
                                            .build(),
                                        UpdatedApiSpecField.newBuilder()
                                            .setReferenceType(
                                                ReferenceType.REFERENCE_TYPE_REFERENCE)
                                            .build(),
                                        UpdatedApiSpecField.newBuilder()
                                            .setApiInspectorDisabled(
                                                !firstCreatedApiSpec.getApiInspectorDisabled())
                                            .build()))
                                .build()))
                    .build())
            .getApiSpecs(0);

    // filter by reference
    assertEquals(
        secondCreatedApiSpec,
        this.apiSpecConfigServiceBlockingStub
            .getApiSpecs(
                GetApiSpecsRequest.newBuilder()
                    .setApiSpecFilter(
                        ApiSpecFilter.newBuilder()
                            .setReferenceApiSpec(
                                ReferenceApiSpecFilter.newBuilder()
                                    .addAllReferenceApiSpecs(
                                        List.of(
                                            ReferenceApiSpec.newBuilder()
                                                .setSpecPath(firstCreatedApiSpec.getSpecPath())
                                                .build()))))
                    .build())
            .getApiSpecs(0));
    // Filter on reference type
    List<ApiSpec> getApiSpecs =
        this.apiSpecConfigServiceBlockingStub
            .getApiSpecs(
                GetApiSpecsRequest.newBuilder()
                    .setApiSpecFilter(
                        ApiSpecFilter.newBuilder()
                            .setReferenceType(
                                ReferenceTypeFilter.newBuilder()
                                    .addAllReferenceTypes(
                                        List.of(
                                            ReferenceType.REFERENCE_TYPE_UNSPECIFIED,
                                            ReferenceType.REFERENCE_TYPE_MAIN))))
                    .build())
            .getApiSpecsList();
    assertTrue(getApiSpecs.contains(firstCreatedApiSpec));
    assertFalse(getApiSpecs.contains(secondCreatedApiSpec));

    // Filter based on inspector flag
    getApiSpecs =
        this.apiSpecConfigServiceBlockingStub
            .getApiSpecs(
                GetApiSpecsRequest.newBuilder()
                    .setApiSpecFilter(
                        ApiSpecFilter.newBuilder()
                            .setApiInspectorDisabled(firstCreatedApiSpec.getApiInspectorDisabled()))
                    .build())
            .getApiSpecsList();
    assertTrue(getApiSpecs.contains(firstCreatedApiSpec));
    assertFalse(getApiSpecs.contains(secondCreatedApiSpec));

    // Filter based on status
    getApiSpecs =
        this.apiSpecConfigServiceBlockingStub
            .getApiSpecs(
                GetApiSpecsRequest.newBuilder()
                    .setApiSpecFilter(
                        ApiSpecFilter.newBuilder()
                            .setStatusFilter(
                                ApiSpecStatusFilter.newBuilder()
                                    .addAllStatuses(List.of(API_SPEC_STATUS_IN_PROGRESS))))
                    .build())
            .getApiSpecsList();
    assertTrue(getApiSpecs.contains(firstCreatedApiSpec));
    assertFalse(getApiSpecs.contains(secondCreatedApiSpec));

    // Filter based on spec name filter
    getApiSpecs =
        this.apiSpecConfigServiceBlockingStub
            .getApiSpecs(
                GetApiSpecsRequest.newBuilder()
                    .setApiSpecFilter(
                        ApiSpecFilter.newBuilder()
                            .setNames(
                                StringList.newBuilder()
                                    .addAllValues(List.of(firstCreatedApiSpec.getName()))))
                    .build())
            .getApiSpecsList();
    assertTrue(getApiSpecs.contains(firstCreatedApiSpec));
    assertFalse(getApiSpecs.contains(secondCreatedApiSpec));

    // Filter based on spec resolution state filter
    getApiSpecs =
        this.apiSpecConfigServiceBlockingStub
            .getApiSpecs(
                GetApiSpecsRequest.newBuilder()
                    .setApiSpecFilter(
                        ApiSpecFilter.newBuilder()
                            .setSpecResolutionStateFilter(
                                SpecResolutionStateFilter.newBuilder()
                                    .addAllSpecResolutionStates(
                                        List.of(OPEN_API_SPEC_RESOLUTION_STATE_INCOMPLETE))))
                    .build())
            .getApiSpecsList();
    assertFalse(getApiSpecs.contains(firstCreatedApiSpec));
    assertTrue(getApiSpecs.contains(secondCreatedApiSpec));
  }

  @Test
  void testGetApiSpecsWithSpecTypeFilter() {
    ApiSpec createdApiSpec1 =
        this.apiSpecConfigServiceBlockingStub
            .createApiSpec(
                CreateApiSpecRequest.newBuilder()
                    .setCreateApiSpec(
                        CreateApiSpec.newBuilder()
                            .setName("spec1")
                            .setApiNamingEnabled(true)
                            .setStatus(API_SPEC_STATUS_IN_PROGRESS)
                            .setSpecPath("/test/spec1.json")
                            .setSpecType(SPEC_TYPE_OPEN_API_SPEC))
                    .build())
            .getApiSpec();
    ApiSpec createdApiSpec2 =
        this.apiSpecConfigServiceBlockingStub
            .createApiSpec(
                CreateApiSpecRequest.newBuilder()
                    .setCreateApiSpec(
                        CreateApiSpec.newBuilder()
                            .setName("spec2")
                            .setApiNamingEnabled(true)
                            .setStatus(API_SPEC_STATUS_IN_PROGRESS)
                            .setSpecPath("/test/spec2.json")
                            .setSpecType(SPEC_TYPE_POSTMAN_COLLECTION)
                            .setApiInspectorDisabled(true))
                    .build())
            .getApiSpec();
    ApiSpec createdApiSpec3 =
        this.apiSpecConfigServiceBlockingStub
            .createApiSpec(
                CreateApiSpecRequest.newBuilder()
                    .setCreateApiSpec(
                        CreateApiSpec.newBuilder()
                            .setName("spec3")
                            .setApiNamingEnabled(true)
                            .setStatus(API_SPEC_STATUS_IN_PROGRESS)
                            .setSpecPath("/test/spec3.json")
                            .setSpecType(SPEC_TYPE_OPEN_API_SPEC)
                            .setApiInspectorDisabled(true))
                    .build())
            .getApiSpec();

    List<ApiSpec> apiSpecs =
        this.apiSpecConfigServiceBlockingStub
            .getApiSpecs(
                GetApiSpecsRequest.newBuilder()
                    .setApiSpecFilter(
                        ApiSpecFilter.newBuilder()
                            .setSpecTypeFilter(
                                SpecTypeFilter.newBuilder().addSpecTypes(SPEC_TYPE_OPEN_API_SPEC)))
                    .build())
            .getApiSpecsList();
    assertEquals(List.of(createdApiSpec3, createdApiSpec1), apiSpecs);

    apiSpecs =
        this.apiSpecConfigServiceBlockingStub
            .getApiSpecs(
                GetApiSpecsRequest.newBuilder()
                    .setApiSpecFilter(
                        ApiSpecFilter.newBuilder()
                            .setSpecTypeFilter(
                                SpecTypeFilter.newBuilder().addSpecTypes(SPEC_TYPE_OPEN_API_SPEC))
                            .setApiInspectorDisabled(false))
                    .build())
            .getApiSpecsList();
    assertEquals(List.of(createdApiSpec1), apiSpecs);

    apiSpecs =
        this.apiSpecConfigServiceBlockingStub
            .getApiSpecs(
                GetApiSpecsRequest.newBuilder()
                    .setApiSpecFilter(
                        ApiSpecFilter.newBuilder()
                            .setSpecTypeFilter(
                                SpecTypeFilter.newBuilder().addSpecTypes(SPEC_TYPE_OPEN_API_SPEC))
                            .setApiInspectorDisabled(true))
                    .build())
            .getApiSpecsList();
    assertEquals(List.of(createdApiSpec3), apiSpecs);
  }

  @Test
  void testLimitSpecConfigs() {
    // Create max_allowed number of spec configs.
    for (int i = 0; i < TEST_MAX_ALLOWED_SPECS_PER_TENANT; i++) {
      this.apiSpecConfigServiceBlockingStub.createApiSpec(
          CreateApiSpecRequest.newBuilder()
              .setCreateApiSpec(
                  CreateApiSpec.newBuilder()
                      .setName(String.format("spec %d", i))
                      .setApiNamingEnabled(true)
                      .setStatus(API_SPEC_STATUS_IN_PROGRESS)
                      .setSpecPath(String.format("/test/spec%d.json", i))
                      .setFileContentSha256(
                          String.format(
                              "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f2001%03d",
                              i)))
              .build());
    }
    // Check if in second exception is thrown if more spec configs are created.
    assertThrows(
        StatusRuntimeException.class,
        () ->
            this.apiSpecConfigServiceBlockingStub.createApiSpec(
                CreateApiSpecRequest.newBuilder()
                    .setCreateApiSpec(
                        CreateApiSpec.newBuilder()
                            .setName("spec2")
                            .setApiNamingEnabled(true)
                            .setStatus(API_SPEC_STATUS_IN_PROGRESS)
                            .setSpecPath("/test/spec2.json")
                            .setFileContentSha256(
                                "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015az"))
                    .build()));
  }

  @Test
  void testBulkApiSpecUpdateAndDelete() {
    ApiSpec firstCreatedApiSpec =
        this.apiSpecConfigServiceBlockingStub
            .createApiSpec(
                CreateApiSpecRequest.newBuilder()
                    .setCreateApiSpec(
                        CreateApiSpec.newBuilder()
                            .setName("spec1")
                            .setApiNamingEnabled(true)
                            .setStatus(API_SPEC_STATUS_IN_PROGRESS)
                            .setSpecPath("/test/spec1.json"))
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
                            .setStatus(API_SPEC_STATUS_COMPLETED)
                            .setSpecPath("/test/spec2.json"))
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

    List<ApiSpec> updatedApiSpecs =
        this.apiSpecConfigServiceBlockingStub
            .updateApiSpecs(
                UpdateApiSpecsRequest.newBuilder()
                    .addAllApiSpecs(
                        List.of(
                            UpdateApiSpec.newBuilder()
                                .setSpecId(firstCreatedApiSpec.getSpecId())
                                .setName("updatedSpec1")
                                .setApiNamingEnabled(false)
                                .setStatus(API_SPEC_STATUS_COMPLETED)
                                .build(),
                            UpdateApiSpec.newBuilder()
                                .setSpecId(secondCreatedApiSpec.getSpecId())
                                .setName("updatedSpec2")
                                .setApiNamingEnabled(false)
                                .setStatus(API_SPEC_STATUS_COMPLETED)
                                .build()))
                    .build())
            .getApiSpecsList();
    assertEquals(2, updatedApiSpecs.size());
    ApiSpec updatedFirstApiSpec =
        ApiSpec.newBuilder()
            .setSpecId(firstCreatedApiSpec.getSpecId())
            .setName("updatedSpec1")
            .setApiNamingEnabled(false)
            .setStatus(API_SPEC_STATUS_UPLOAD_COMPLETED)
            .setCreationTimestamp(Timestamp.newBuilder().setSeconds(100))
            .setLastUpdatedTimestamp(Timestamp.newBuilder().setSeconds(100))
            .setSpecPath("/test/spec1.json")
            .setSpecType(SpecType.SPEC_TYPE_OPEN_API_SPEC)
            .setReferenceType(ReferenceType.REFERENCE_TYPE_UNSPECIFIED)
            .setApiSpecMetadata(ApiSpecMetadata.getDefaultInstance())
            .build();
    ApiSpec updatedSecondApiSpec =
        ApiSpec.newBuilder()
            .setSpecId(secondCreatedApiSpec.getSpecId())
            .setName("updatedSpec2")
            .setApiNamingEnabled(false)
            .setStatus(API_SPEC_STATUS_UPLOAD_COMPLETED)
            .setCreationTimestamp(Timestamp.newBuilder().setSeconds(100))
            .setLastUpdatedTimestamp(Timestamp.newBuilder().setSeconds(100))
            .setSpecPath("/test/spec2.json")
            .setSpecType(SpecType.SPEC_TYPE_OPEN_API_SPEC)
            .setReferenceType(ReferenceType.REFERENCE_TYPE_UNSPECIFIED)
            .setApiSpecMetadata(ApiSpecMetadata.getDefaultInstance())
            .build();

    assertTrue(updatedApiSpecs.contains(updatedFirstApiSpec));
    assertTrue(updatedApiSpecs.contains(updatedSecondApiSpec));

    apiSpecs =
        this.apiSpecConfigServiceBlockingStub
            .getApiSpecs(GetApiSpecsRequest.newBuilder().build())
            .getApiSpecsList();
    assertEquals(2, apiSpecs.size());
    assertTrue(apiSpecs.contains(updatedFirstApiSpec));
    assertTrue(apiSpecs.contains(updatedSecondApiSpec));

    this.apiSpecConfigServiceBlockingStub.deleteApiSpecs(
        DeleteApiSpecsRequest.newBuilder()
            .addAllSpecIds(
                List.of(firstCreatedApiSpec.getSpecId(), secondCreatedApiSpec.getSpecId()))
            .build());

    apiSpecs =
        this.apiSpecConfigServiceBlockingStub
            .getApiSpecs(GetApiSpecsRequest.newBuilder().build())
            .getApiSpecsList();
    assertEquals(0, apiSpecs.size());
  }

  @Test
  void testBulkApiSpecPartialUpdateAndDelete() {
    ApiSpec firstCreatedApiSpec =
        this.apiSpecConfigServiceBlockingStub
            .createApiSpec(
                CreateApiSpecRequest.newBuilder()
                    .setCreateApiSpec(
                        CreateApiSpec.newBuilder()
                            .setName("spec1")
                            .setApiNamingEnabled(true)
                            .setStatus(API_SPEC_STATUS_IN_PROGRESS)
                            .setSpecPath("/test/spec1.json"))
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
                            .setStatus(API_SPEC_STATUS_COMPLETED)
                            .setSpecPath("/test/spec2.json"))
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

    UpdatedApiSpecField.Builder updatedApiSpecFieldBuilder = UpdatedApiSpecField.newBuilder();

    List<ApiSpec> updatedApiSpecs =
        this.apiSpecConfigServiceBlockingStub
            .bulkUpdateApiSpecs(
                BulkUpdateApiSpecsRequest.newBuilder()
                    .addAllApiSpecs(
                        List.of(
                            ApiSpecUpdate.newBuilder()
                                .setSpecId(firstCreatedApiSpec.getSpecId())
                                .addAllUpdatedApiSpecFields(
                                    List.of(
                                        updatedApiSpecFieldBuilder.setName("updatedSpec1").build(),
                                        updatedApiSpecFieldBuilder
                                            .setApiNamingEnabled(false)
                                            .build(),
                                        updatedApiSpecFieldBuilder
                                            .setStatus(API_SPEC_STATUS_COMPLETED)
                                            .build(),
                                        updatedApiSpecFieldBuilder
                                            .setApiDiscoveryEnabled(false)
                                            .build(),
                                        updatedApiSpecFieldBuilder
                                            .setApiInspectorDisabled(true)
                                            .build()))
                                .build(),
                            ApiSpecUpdate.newBuilder()
                                .setSpecId(secondCreatedApiSpec.getSpecId())
                                .addAllUpdatedApiSpecFields(
                                    List.of(
                                        updatedApiSpecFieldBuilder.setName("updatedSpec2").build(),
                                        updatedApiSpecFieldBuilder
                                            .setApiDiscoveryEnabled(true)
                                            .build(),
                                        updatedApiSpecFieldBuilder
                                            .setApiInspectorDisabled(false)
                                            .build(),
                                        updatedApiSpecFieldBuilder
                                            .setReferenceType(
                                                ReferenceType.REFERENCE_TYPE_REFERENCE)
                                            .build(),
                                        updatedApiSpecFieldBuilder
                                            .setApiSpecMetadata(
                                                ApiSpecMetadata.newBuilder()
                                                    .setOpenApiSpecMetadata(
                                                        OpenApiSpecMetadata.newBuilder()
                                                            .addAllOpenApiSpecReferences(
                                                                List.of(
                                                                    OpenApiSpecReference
                                                                        .newBuilder()
                                                                        .setResolvedSpecPath(
                                                                            "/test/spec1.json")
                                                                        .setIncompleteOpenApiSpecReference(
                                                                            IncompleteOpenApiSpecReference
                                                                                .newBuilder()
                                                                                .setSpecId(
                                                                                    firstCreatedApiSpec
                                                                                        .getSpecId()))
                                                                        .build()))))
                                            .build()))
                                .build()))
                    .build())
            .getApiSpecsList();
    assertEquals(2, updatedApiSpecs.size());
    ApiSpec updatedFirstApiSpec =
        ApiSpec.newBuilder()
            .setSpecId(firstCreatedApiSpec.getSpecId())
            .setName("updatedSpec1")
            .setApiNamingEnabled(false)
            .setStatus(API_SPEC_STATUS_UPLOAD_COMPLETED)
            .setCreationTimestamp(Timestamp.newBuilder().setSeconds(100).build())
            .setLastUpdatedTimestamp(Timestamp.newBuilder().setSeconds(100).build())
            .setSpecPath("/test/spec1.json")
            .setSpecType(SPEC_TYPE_OPEN_API_SPEC)
            .setApiDiscoveryEnabled(false)
            .setApiInspectorDisabled(true)
            .setReferenceType(ReferenceType.REFERENCE_TYPE_UNSPECIFIED)
            .setApiSpecMetadata(ApiSpecMetadata.getDefaultInstance())
            .build();
    ApiSpec updatedSecondApiSpec =
        ApiSpec.newBuilder()
            .setSpecId(secondCreatedApiSpec.getSpecId())
            .setName("updatedSpec2")
            .setApiNamingEnabled(true)
            .setStatus(API_SPEC_STATUS_UPLOAD_COMPLETED)
            .setCreationTimestamp(Timestamp.newBuilder().setSeconds(100).build())
            .setLastUpdatedTimestamp(Timestamp.newBuilder().setSeconds(100).build())
            .setSpecPath("/test/spec2.json")
            .setSpecType(SPEC_TYPE_OPEN_API_SPEC)
            .setApiDiscoveryEnabled(true)
            .setApiInspectorDisabled(false)
            .setReferenceType(ReferenceType.REFERENCE_TYPE_REFERENCE)
            .setApiSpecMetadata(
                ApiSpecMetadata.newBuilder()
                    .setOpenApiSpecMetadata(
                        OpenApiSpecMetadata.newBuilder()
                            .addAllOpenApiSpecReferences(
                                List.of(
                                    OpenApiSpecReference.newBuilder()
                                        .setResolvedSpecPath("/test/spec1.json")
                                        .setIncompleteOpenApiSpecReference(
                                            IncompleteOpenApiSpecReference.newBuilder()
                                                .setSpecId(firstCreatedApiSpec.getSpecId()))
                                        .build()))))
            .build();

    assertTrue(updatedApiSpecs.contains(updatedFirstApiSpec));
    assertTrue(updatedApiSpecs.contains(updatedSecondApiSpec));

    apiSpecs =
        this.apiSpecConfigServiceBlockingStub
            .getApiSpecs(GetApiSpecsRequest.newBuilder().build())
            .getApiSpecsList();
    assertEquals(2, apiSpecs.size());
    assertTrue(apiSpecs.contains(updatedFirstApiSpec));
    assertTrue(apiSpecs.contains(updatedSecondApiSpec));

    this.apiSpecConfigServiceBlockingStub.deleteApiSpecs(
        DeleteApiSpecsRequest.newBuilder()
            .addAllSpecIds(
                List.of(firstCreatedApiSpec.getSpecId(), secondCreatedApiSpec.getSpecId()))
            .build());

    apiSpecs =
        this.apiSpecConfigServiceBlockingStub
            .getApiSpecs(GetApiSpecsRequest.newBuilder().build())
            .getApiSpecsList();
    assertEquals(0, apiSpecs.size());
  }

  @Test
  void testBulkUpdate_throwsError() {
    ApiSpec apiSpec =
        this.apiSpecConfigServiceBlockingStub
            .createApiSpec(
                CreateApiSpecRequest.newBuilder()
                    .setCreateApiSpec(
                        CreateApiSpec.newBuilder()
                            .setName("spec1")
                            .setApiNamingEnabled(true)
                            .setStatus(API_SPEC_STATUS_IN_PROGRESS)
                            .setSpecPath("/test/spec1.json"))
                    .build())
            .getApiSpec();
    Timestamp expectedTimestamp = Timestamp.newBuilder().setSeconds(100).build();
    assertEquals(expectedTimestamp, apiSpec.getCreationTimestamp());
    assertEquals(expectedTimestamp, apiSpec.getLastUpdatedTimestamp());

    assertThrows(
        StatusRuntimeException.class,
        () ->
            this.apiSpecConfigServiceBlockingStub.updateApiSpecs(
                UpdateApiSpecsRequest.newBuilder()
                    .addAllApiSpecs(
                        List.of(
                            UpdateApiSpec.newBuilder()
                                .setSpecId(apiSpec.getSpecId())
                                .setName("updatedSpec1")
                                .setApiNamingEnabled(false)
                                .setStatus(API_SPEC_STATUS_COMPLETED)
                                .build(),
                            UpdateApiSpec.newBuilder()
                                .setSpecId(UUID.randomUUID().toString())
                                .setName("updatedSpec2")
                                .setApiNamingEnabled(false)
                                .setStatus(API_SPEC_STATUS_COMPLETED)
                                .build()))
                    .build()));
  }
}
