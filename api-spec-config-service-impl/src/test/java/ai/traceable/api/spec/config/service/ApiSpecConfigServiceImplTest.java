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
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.hypertrace.config.objectstore.ClientConfig;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ContextSpecificConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ApiSpecConfigServiceImplTest {

  private ApiSpecConfigServiceGrpc.ApiSpecConfigServiceBlockingStub
      apiSpecConfigServiceBlockingStub;
  private final int TEST_MAX_ALLOWED_SPECS_PER_TENANT = 7;
  private MockGenericConfigService mockGenericConfigService;

  @BeforeEach
  void beforeEach() {
    mockGenericConfigService =
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
            genericStub,
            timestampConverter,
            configChangeEventGenerator,
            apiSpecStatusConverter,
            ClientConfig.DEFAULT);
    Config testConfig =
        ConfigFactory.parseMap(
            Map.of("max.allowed.specs.per.tenant", TEST_MAX_ALLOWED_SPECS_PER_TENANT));
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
  void testApiSpecCrud() throws InvalidProtocolBufferException {
    List<ApiSpec> currentSpecs = new ArrayList<>();

    // Step 1: Create first spec
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
    currentSpecs.add(firstCreatedApiSpec);

    Timestamp expectedTimestamp = Timestamp.newBuilder().setSeconds(100).build();
    assertEquals(expectedTimestamp, firstCreatedApiSpec.getCreationTimestamp());
    assertEquals(expectedTimestamp, firstCreatedApiSpec.getLastUpdatedTimestamp());

    // Step 2: Create second spec
    // Duplicate specs will be verified based on specPath and
    String specPath2 = "/test/spec2.json";
    mockGenericConfigService.mockGetAllWithFilter(
        buildPredicate(
            ApiSpecFilter.newBuilder()
                .setSpecPaths(StringList.newBuilder().addValues(specPath2).build())
                .build()));
    ApiSpec secondCreatedApiSpec =
        this.apiSpecConfigServiceBlockingStub
            .createApiSpec(
                CreateApiSpecRequest.newBuilder()
                    .setCreateApiSpec(
                        CreateApiSpec.newBuilder()
                            .setName("spec2")
                            .setApiNamingEnabled(true)
                            .setStatus(API_SPEC_STATUS_COMPLETED)
                            .setSpecPath(specPath2))
                    .build())
            .getApiSpec();
    currentSpecs.add(secondCreatedApiSpec);

    // Mock state after second create
    mockGenericConfigService.mockGetAllWithFilter(
        buildPredicate(ApiSpecFilter.getDefaultInstance()));

    // Step 3: Validate both specs returned
    List<ApiSpec> apiSpecs =
        this.apiSpecConfigServiceBlockingStub
            .getApiSpecs(GetApiSpecsRequest.newBuilder().build())
            .getApiSpecsList();
    assertEquals(2, apiSpecs.size());
    assertTrue(apiSpecs.contains(firstCreatedApiSpec));
    assertTrue(apiSpecs.contains(secondCreatedApiSpec));

    // Step 4: Validate get by ID
    mockGenericConfigService.mockGetAllWithFilter(
        buildPredicate(
            ApiSpecFilter.newBuilder()
                .setIds(StringList.newBuilder().addValues(firstCreatedApiSpec.getSpecId()).build())
                .build()));
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
    mockGenericConfigService.mockGetAllWithFilter(
        buildPredicate(
            ApiSpecFilter.newBuilder()
                .setIds(StringList.newBuilder().addValues(secondCreatedApiSpec.getSpecId()).build())
                .build()));
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
    mockGenericConfigService.mockGetAllWithFilter(
        buildPredicate(
            ApiSpecFilter.newBuilder()
                .setIds(StringList.newBuilder().addValues(firstCreatedApiSpec.getSpecId()).build())
                .build()));
    // Step 5: Update first spec name + apiNamingEnabled
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

    // Step 6: Update status to COMPLETED
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

    // Replace in current specs and mock
    currentSpecs.set(0, updatedFirstApiSpec);
    mockGenericConfigService.mockGetAllWithFilter(
        buildPredicate(
            ApiSpecFilter.newBuilder()
                .setSpecPaths(StringList.newBuilder().addValues("/test/spec1.json").build())
                .build()));

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

    mockGenericConfigService.mockGetAllWithFilter(
        buildPredicate(ApiSpecFilter.getDefaultInstance()));

    apiSpecs =
        this.apiSpecConfigServiceBlockingStub
            .getApiSpecs(GetApiSpecsRequest.newBuilder().build())
            .getApiSpecsList();
    assertEquals(2, apiSpecs.size());
    assertTrue(apiSpecs.contains(updatedFirstApiSpec));

    // Step 7: Delete first spec
    this.apiSpecConfigServiceBlockingStub.deleteApiSpec(
        DeleteApiSpecRequest.newBuilder().setSpecId(firstCreatedApiSpec.getSpecId()).build());
    currentSpecs.remove(updatedFirstApiSpec);

    // Mock state after delete
    mockGenericConfigService.mockGetAllWithFilter(
        buildPredicate(ApiSpecFilter.getDefaultInstance()));

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
  void testGetApiSpecsWithSpecPathFilter() throws InvalidProtocolBufferException {
    // 1. Initial specPaths filter (before any ApiSpec is created)
    ApiSpecFilter initialFilter =
        ApiSpecFilter.newBuilder()
            .setSpecPaths(
                StringList.newBuilder()
                    .addAllValues(List.of("/test/spec1.json", "/test/spec2.json")))
            .build();

    GetApiSpecsRequest initialRequest =
        GetApiSpecsRequest.newBuilder().setApiSpecFilter(initialFilter).build();

    // Expect no ApiSpecs initially
    assertEquals(
        0,
        apiSpecConfigServiceBlockingStub.getApiSpecs(initialRequest).getApiSpecsCount(),
        "Expected no ApiSpecs before creation");

    // 2. Create an ApiSpec
    ApiSpec createdApiSpec =
        apiSpecConfigServiceBlockingStub
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

    // 3. Mock server-side filter for initialFilter
    mockGenericConfigService.mockGetAllWithFilter(buildPredicate(initialFilter));

    // 4. Expect created spec to match initial filter
    GetApiSpecsRequest matchingRequest =
        GetApiSpecsRequest.newBuilder().setApiSpecFilter(initialFilter).build();

    assertEquals(
        createdApiSpec,
        apiSpecConfigServiceBlockingStub.getApiSpecs(matchingRequest).getApiSpecs(0),
        "Expected the created ApiSpec to be returned for matching filter");

    // 5. Reconfigure mock for a non-matching filter
    ApiSpecFilter nonMatchingFilter =
        ApiSpecFilter.newBuilder()
            .setSpecPaths(
                StringList.newBuilder()
                    .addAllValues(List.of("/test/spec2.json", "/test/spec3.json")))
            .build();

    mockGenericConfigService.mockGetAllWithFilter(buildPredicate(nonMatchingFilter));

    GetApiSpecsRequest nonMatchingRequest =
        GetApiSpecsRequest.newBuilder().setApiSpecFilter(nonMatchingFilter).build();

    // 6. Expect no specs for non-matching filter
    assertEquals(
        0,
        apiSpecConfigServiceBlockingStub.getApiSpecs(nonMatchingRequest).getApiSpecsCount(),
        "Expected no results with non-matching specPaths");
  }

  protected Optional<ApiSpec> buildDataFromValue(Value value)
      throws InvalidProtocolBufferException {
    ApiSpec.Builder configBuilder = ApiSpec.newBuilder();
    ConfigProtoConverter.mergeFromValue(value, configBuilder);
    configBuilder.setStatus(new ApiSpecStatusConverter().convert(configBuilder.getStatus()));
    return Optional.of(configBuilder.build());
  }

  @Test
  void testGetApiSpecsWithFilter() throws InvalidProtocolBufferException {
    // Step 1: Ensure empty state returns nothing
    ApiSpecFilter fileHashFilter =
        ApiSpecFilter.newBuilder()
            .setFileContentSha256(
                FileContentSha256Filter.newBuilder()
                    .addFileContentSha256(
                        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"))
            .build();
    mockGenericConfigService.mockGetAllWithFilter(buildPredicate(fileHashFilter));

    assertEquals(
        0,
        apiSpecConfigServiceBlockingStub
            .getApiSpecs(GetApiSpecsRequest.newBuilder().setApiSpecFilter(fileHashFilter).build())
            .getApiSpecsCount());

    // Step 2: Create first ApiSpec
    ApiSpec firstCreatedApiSpec =
        apiSpecConfigServiceBlockingStub
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

    // Step 3: Verify filter by file hash
    mockGenericConfigService.mockGetAllWithFilter(buildPredicate(fileHashFilter));

    assertEquals(
        firstCreatedApiSpec,
        apiSpecConfigServiceBlockingStub
            .getApiSpecs(GetApiSpecsRequest.newBuilder().setApiSpecFilter(fileHashFilter).build())
            .getApiSpecs(0));

    // Step 4: Verify filter by file hash
    ApiSpecFilter fileHashFilter2 =
        ApiSpecFilter.newBuilder()
            .setFileContentSha256(
                FileContentSha256Filter.newBuilder()
                    .addFileContentSha256(
                        "5e50280084181a7a3e44c8093b367d4b79e6b6d19e575bfc956a7714b58196ab"))
            .build();
    mockGenericConfigService.mockGetAllWithFilter(buildPredicate(fileHashFilter2));

    // Step 5: Create second ApiSpec with reference to first

    ApiSpec secondCreatedApiSpec =
        apiSpecConfigServiceBlockingStub
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
        apiSpecConfigServiceBlockingStub
            .bulkUpdateApiSpecs(
                BulkUpdateApiSpecsRequest.newBuilder()
                    .addApiSpecs(
                        ApiSpecUpdate.newBuilder()
                            .setSpecId(secondCreatedApiSpec.getSpecId())
                            .addUpdatedApiSpecFields(
                                UpdatedApiSpecField.newBuilder()
                                    .setApiSpecMetadata(
                                        ApiSpecMetadata.newBuilder()
                                            .setOpenApiSpecMetadata(
                                                OpenApiSpecMetadata.newBuilder()
                                                    .setOpenApiSpecResolutionState(
                                                        OPEN_API_SPEC_RESOLUTION_STATE_INCOMPLETE)
                                                    .addOpenApiSpecReferences(
                                                        OpenApiSpecReference.newBuilder()
                                                            .setResolvedSpecPath(
                                                                firstCreatedApiSpec.getSpecPath())
                                                            .setMissingOpenApiSpecReference(
                                                                MissingOpenApiSpecReference
                                                                    .getDefaultInstance())
                                                            .build()))))
                            .addUpdatedApiSpecFields(
                                UpdatedApiSpecField.newBuilder()
                                    .setReferenceType(ReferenceType.REFERENCE_TYPE_REFERENCE))
                            .addUpdatedApiSpecFields(
                                UpdatedApiSpecField.newBuilder()
                                    .setApiInspectorDisabled(
                                        !firstCreatedApiSpec.getApiInspectorDisabled())))
                    .build())
            .getApiSpecs(0);

    // Reference filter
    ApiSpecFilter referenceFilter =
        ApiSpecFilter.newBuilder()
            .setReferenceApiSpec(
                ReferenceApiSpecFilter.newBuilder()
                    .addReferenceApiSpecs(
                        ReferenceApiSpec.newBuilder()
                            .setSpecPath(firstCreatedApiSpec.getSpecPath())))
            .build();
    mockGenericConfigService.mockGetAllWithFilter(buildPredicate(referenceFilter));
    assertEquals(
        secondCreatedApiSpec,
        apiSpecConfigServiceBlockingStub
            .getApiSpecs(GetApiSpecsRequest.newBuilder().setApiSpecFilter(referenceFilter).build())
            .getApiSpecs(0));

    // Reference type filter
    ApiSpecFilter refTypeFilter =
        ApiSpecFilter.newBuilder()
            .setReferenceType(
                ReferenceTypeFilter.newBuilder()
                    .addAllReferenceTypes(
                        List.of(
                            ReferenceType.REFERENCE_TYPE_MAIN,
                            ReferenceType.REFERENCE_TYPE_UNSPECIFIED)))
            .build();
    mockGenericConfigService.mockGetAllWithFilter(buildPredicate(refTypeFilter));
    List<ApiSpec> results =
        apiSpecConfigServiceBlockingStub
            .getApiSpecs(GetApiSpecsRequest.newBuilder().setApiSpecFilter(refTypeFilter).build())
            .getApiSpecsList();
    assertTrue(results.contains(firstCreatedApiSpec));
    assertFalse(results.contains(secondCreatedApiSpec));

    // Inspector disabled filter
    ApiSpecFilter inspectorDisabledFilter =
        ApiSpecFilter.newBuilder()
            .setApiInspectorDisabled(firstCreatedApiSpec.getApiInspectorDisabled())
            .build();
    mockGenericConfigService.mockGetAllWithFilter(buildPredicate(inspectorDisabledFilter));
    results =
        apiSpecConfigServiceBlockingStub
            .getApiSpecs(
                GetApiSpecsRequest.newBuilder().setApiSpecFilter(inspectorDisabledFilter).build())
            .getApiSpecsList();
    assertTrue(results.contains(firstCreatedApiSpec));
    assertFalse(results.contains(secondCreatedApiSpec));

    // Status filter
    ApiSpecFilter statusFilter =
        ApiSpecFilter.newBuilder()
            .setStatusFilter(
                ApiSpecStatusFilter.newBuilder().addStatuses(API_SPEC_STATUS_UPLOAD_IN_PROGRESS))
            .build();
    mockGenericConfigService.mockGetAllWithFilter(buildPredicate(statusFilter));
    results =
        apiSpecConfigServiceBlockingStub
            .getApiSpecs(GetApiSpecsRequest.newBuilder().setApiSpecFilter(statusFilter).build())
            .getApiSpecsList();
    assertTrue(results.contains(firstCreatedApiSpec));
    assertFalse(results.contains(secondCreatedApiSpec));

    // Name filter
    ApiSpecFilter nameFilter =
        ApiSpecFilter.newBuilder()
            .setNames(StringList.newBuilder().addValues(firstCreatedApiSpec.getName()))
            .build();
    mockGenericConfigService.mockGetAllWithFilter(buildPredicate(nameFilter));
    results =
        apiSpecConfigServiceBlockingStub
            .getApiSpecs(GetApiSpecsRequest.newBuilder().setApiSpecFilter(nameFilter).build())
            .getApiSpecsList();
    assertTrue(results.contains(firstCreatedApiSpec));
    assertFalse(results.contains(secondCreatedApiSpec));

    // Resolution state filter
    ApiSpecFilter resStateFilter =
        ApiSpecFilter.newBuilder()
            .setSpecResolutionStateFilter(
                SpecResolutionStateFilter.newBuilder()
                    .addSpecResolutionStates(OPEN_API_SPEC_RESOLUTION_STATE_INCOMPLETE))
            .build();
    mockGenericConfigService.mockGetAllWithFilter(buildPredicate(resStateFilter));
    results =
        apiSpecConfigServiceBlockingStub
            .getApiSpecs(GetApiSpecsRequest.newBuilder().setApiSpecFilter(resStateFilter).build())
            .getApiSpecsList();
    assertFalse(results.contains(firstCreatedApiSpec));
    assertTrue(results.contains(secondCreatedApiSpec));
  }

  private Predicate<ContextSpecificConfig> buildPredicate(ApiSpecFilter filter) {
    return config -> {
      if (!config.hasConfig()) return false;

      try {
        Optional<ApiSpec> maybe = buildDataFromValue(config.getConfig());
        if (maybe.isEmpty()) return false;
        ApiSpec spec = maybe.get();

        if (filter.hasFileContentSha256()) {
          if (!filter
              .getFileContentSha256()
              .getFileContentSha256List()
              .contains(spec.getFileContentSha256())) return false;
        }

        if (filter.hasSpecPaths()) {
          List<String> specPaths = filter.getSpecPaths().getValuesList();
          return specPaths.contains(spec.getSpecPath());
        }

        if (filter.hasReferenceApiSpec()) {
          List<String> referencedPaths =
              filter.getReferenceApiSpec().getReferenceApiSpecsList().stream()
                  .map(ReferenceApiSpec::getSpecPath)
                  .collect(Collectors.toList());
          boolean matches =
              spec
                  .getApiSpecMetadata()
                  .getOpenApiSpecMetadata()
                  .getOpenApiSpecReferencesList()
                  .stream()
                  .anyMatch(ref -> referencedPaths.contains(ref.getResolvedSpecPath()));
          if (!matches) return false;
        }

        if (filter.hasReferenceType()) {
          if (!filter.getReferenceType().getReferenceTypesList().contains(spec.getReferenceType()))
            return false;
        }

        if (filter.hasStatusFilter()) {
          if (!filter.getStatusFilter().getStatusesList().contains(spec.getStatus())) return false;
        }

        if (filter.hasApiInspectorDisabled()) {
          if (spec.getApiInspectorDisabled() != filter.getApiInspectorDisabled()) return false;
        }

        if (filter.hasNames()) {
          if (!filter.getNames().getValuesList().contains(spec.getName())) return false;
        }

        if (filter.hasSpecResolutionStateFilter()) {
          if (!filter
              .getSpecResolutionStateFilter()
              .getSpecResolutionStatesList()
              .contains(
                  spec.getApiSpecMetadata()
                      .getOpenApiSpecMetadata()
                      .getOpenApiSpecResolutionState())) return false;
        }

        if (filter.hasSpecTypeFilter()) {
          return filter.getSpecTypeFilter().getSpecTypesList().contains(spec.getSpecType());
        }

        if (filter.hasIds()) {
          return filter.getIds().getValuesList().contains(spec.getSpecId());
        }

        return true;
      } catch (InvalidProtocolBufferException e) {
        return false;
      }
    };
  }

  @Test
  void testGetApiSpecsWithSpecTypeFilter() throws InvalidProtocolBufferException {
    // Step 1: Create three ApiSpecs with different spec types and inspector flags
    ApiSpec createdApiSpec1 =
        apiSpecConfigServiceBlockingStub
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

    ApiSpecFilter specPathFilter =
        ApiSpecFilter.newBuilder()
            .setSpecPaths(StringList.newBuilder().addValues("/test/spec2.json").build())
            .build();
    mockGenericConfigService.mockGetAllWithFilter(buildPredicate(specPathFilter));

    ApiSpec createdApiSpec2 =
        apiSpecConfigServiceBlockingStub
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

    specPathFilter =
        ApiSpecFilter.newBuilder()
            .setSpecPaths(StringList.newBuilder().addValues("/test/spec3.json").build())
            .build();
    mockGenericConfigService.mockGetAllWithFilter(buildPredicate(specPathFilter));

    ApiSpec createdApiSpec3 =
        apiSpecConfigServiceBlockingStub
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

    // Step 2: Filter by SpecType = OPEN_API_SPEC
    ApiSpecFilter openApiFilter =
        ApiSpecFilter.newBuilder()
            .setSpecTypeFilter(SpecTypeFilter.newBuilder().addSpecTypes(SPEC_TYPE_OPEN_API_SPEC))
            .build();
    mockGenericConfigService.mockGetAllWithFilter(buildPredicate(openApiFilter));
    List<ApiSpec> apiSpecs =
        apiSpecConfigServiceBlockingStub
            .getApiSpecs(GetApiSpecsRequest.newBuilder().setApiSpecFilter(openApiFilter).build())
            .getApiSpecsList();
    assertEquals(List.of(createdApiSpec1, createdApiSpec3), apiSpecs);

    // Step 3: Add ApiInspectorDisabled = false to the filter
    ApiSpecFilter openApiWithInspectorEnabled =
        ApiSpecFilter.newBuilder()
            .setSpecTypeFilter(SpecTypeFilter.newBuilder().addSpecTypes(SPEC_TYPE_OPEN_API_SPEC))
            .setApiInspectorDisabled(false)
            .build();
    mockGenericConfigService.mockGetAllWithFilter(buildPredicate(openApiWithInspectorEnabled));
    apiSpecs =
        apiSpecConfigServiceBlockingStub
            .getApiSpecs(
                GetApiSpecsRequest.newBuilder()
                    .setApiSpecFilter(openApiWithInspectorEnabled)
                    .build())
            .getApiSpecsList();
    assertEquals(List.of(createdApiSpec1), apiSpecs);

    // Step 4: Add ApiInspectorDisabled = true to the filter
    ApiSpecFilter openApiWithInspectorDisabled =
        ApiSpecFilter.newBuilder()
            .setSpecTypeFilter(SpecTypeFilter.newBuilder().addSpecTypes(SPEC_TYPE_OPEN_API_SPEC))
            .setApiInspectorDisabled(true)
            .build();
    mockGenericConfigService.mockGetAllWithFilter(buildPredicate(openApiWithInspectorDisabled));
    apiSpecs =
        apiSpecConfigServiceBlockingStub
            .getApiSpecs(
                GetApiSpecsRequest.newBuilder()
                    .setApiSpecFilter(openApiWithInspectorDisabled)
                    .build())
            .getApiSpecsList();
    assertEquals(List.of(createdApiSpec3), apiSpecs);
  }

  @Test
  void testLimitSpecConfigs() {
    // Step 1: Create max allowed number of spec configs
    for (int i = 0; i < TEST_MAX_ALLOWED_SPECS_PER_TENANT; i++) {
      mockGenericConfigService.mockGetAllWithFilter(
          buildPredicate(
              ApiSpecFilter.newBuilder()
                  .setSpecPaths(
                      StringList.newBuilder()
                          .addValues(String.format("/test/spec%d.json", i))
                          .build())
                  .build()));
      ApiSpec spec =
          this.apiSpecConfigServiceBlockingStub
              .createApiSpec(
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
                      .build())
              .getApiSpec();
    }

    // Step 2: Try to create one more and expect rejection
    assertThrows(
        StatusRuntimeException.class,
        () -> {
          // This mock should still return TEST_MAX_ALLOWED_SPECS_PER_TENANT configs
          mockGenericConfigService.mockGetAllWithFilter(
              buildPredicate(ApiSpecFilter.getDefaultInstance()));

          this.apiSpecConfigServiceBlockingStub.createApiSpec(
              CreateApiSpecRequest.newBuilder()
                  .setCreateApiSpec(
                      CreateApiSpec.newBuilder()
                          .setName("spec overflow")
                          .setApiNamingEnabled(true)
                          .setStatus(API_SPEC_STATUS_IN_PROGRESS)
                          .setSpecPath("/test/spec-overflow.json")
                          .setFileContentSha256(
                              "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f2001overflow"))
                  .build());
        });
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

    String specPath2 = "/test/spec2.json";
    mockGenericConfigService.mockGetAllWithFilter(
        buildPredicate(
            ApiSpecFilter.newBuilder()
                .setSpecPaths(StringList.newBuilder().addValues(specPath2).build())
                .build()));
    ApiSpec secondCreatedApiSpec =
        this.apiSpecConfigServiceBlockingStub
            .createApiSpec(
                CreateApiSpecRequest.newBuilder()
                    .setCreateApiSpec(
                        CreateApiSpec.newBuilder()
                            .setName("spec2")
                            .setApiNamingEnabled(true)
                            .setStatus(API_SPEC_STATUS_COMPLETED)
                            .setSpecPath(specPath2))
                    .build())
            .getApiSpec();

    mockGenericConfigService.mockGetAllWithFilter(
        buildPredicate(ApiSpecFilter.getDefaultInstance()));
    List<ApiSpec> apiSpecs =
        this.apiSpecConfigServiceBlockingStub
            .getApiSpecs(GetApiSpecsRequest.newBuilder().build())
            .getApiSpecsList();
    assertEquals(2, apiSpecs.size());
    assertTrue(apiSpecs.contains(firstCreatedApiSpec));
    assertTrue(apiSpecs.contains(secondCreatedApiSpec));
    mockGenericConfigService.mockGetAllWithFilter(
        buildPredicate(
            ApiSpecFilter.newBuilder()
                .setIds(StringList.newBuilder().addValues(firstCreatedApiSpec.getSpecId()).build())
                .build()));
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

    mockGenericConfigService.mockGetAllWithFilter(
        buildPredicate(
            ApiSpecFilter.newBuilder()
                .setIds(StringList.newBuilder().addValues(secondCreatedApiSpec.getSpecId()).build())
                .build()));
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

    mockGenericConfigService.mockGetAllWithFilter(
        buildPredicate(ApiSpecFilter.getDefaultInstance()));

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
            .setSpecPath(specPath2)
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

    String specPath2 = "/test/spec2.json";
    mockGenericConfigService.mockGetAllWithFilter(
        buildPredicate(
            ApiSpecFilter.newBuilder()
                .setSpecPaths(StringList.newBuilder().addValues(specPath2).build())
                .build()));
    ApiSpec secondCreatedApiSpec =
        this.apiSpecConfigServiceBlockingStub
            .createApiSpec(
                CreateApiSpecRequest.newBuilder()
                    .setCreateApiSpec(
                        CreateApiSpec.newBuilder()
                            .setName("spec2")
                            .setApiNamingEnabled(true)
                            .setStatus(API_SPEC_STATUS_COMPLETED)
                            .setSpecPath(specPath2))
                    .build())
            .getApiSpec();

    mockGenericConfigService.mockGetAllWithFilter(
        buildPredicate(ApiSpecFilter.getDefaultInstance()));
    List<ApiSpec> apiSpecs =
        this.apiSpecConfigServiceBlockingStub
            .getApiSpecs(GetApiSpecsRequest.newBuilder().build())
            .getApiSpecsList();
    assertEquals(2, apiSpecs.size());
    assertTrue(apiSpecs.contains(firstCreatedApiSpec));
    assertTrue(apiSpecs.contains(secondCreatedApiSpec));
    mockGenericConfigService.mockGetAllWithFilter(
        buildPredicate(
            ApiSpecFilter.newBuilder()
                .setIds(StringList.newBuilder().addValues(firstCreatedApiSpec.getSpecId()).build())
                .build()));
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
    mockGenericConfigService.mockGetAllWithFilter(
        buildPredicate(
            ApiSpecFilter.newBuilder()
                .setIds(StringList.newBuilder().addValues(secondCreatedApiSpec.getSpecId()).build())
                .build()));
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

    mockGenericConfigService.mockGetAllWithFilter(
        buildPredicate(ApiSpecFilter.getDefaultInstance()));

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
                                            .build(),
                                        updatedApiSpecFieldBuilder
                                            .setApiDiscoveryEnabledV2(true)
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
                                            .build(),
                                        updatedApiSpecFieldBuilder
                                            .setApiDiscoveryEnabledV2(false)
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
            .setApiDiscoveryEnabledV2(true)
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
            .setApiDiscoveryEnabledV2(false)
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
