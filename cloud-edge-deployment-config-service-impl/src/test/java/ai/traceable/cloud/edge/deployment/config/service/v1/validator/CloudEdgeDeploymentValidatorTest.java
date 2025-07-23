package ai.traceable.cloud.edge.deployment.config.service.v1.validator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import ai.traceable.cloud.edge.deployment.config.service.v1.*;
import ai.traceable.cloud.edge.deployment.config.service.v1.shared.config.SharedConfigMetadataRegistry;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.grpc.Status;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CloudEdgeDeploymentValidatorTest {
  @Mock private SharedConfigMetadataRegistry sharedConfigMetadataRegistry;

  private CloudEdgeDeploymentValidator validator;

  @BeforeEach
  void setUp() {
    validator = new CloudEdgeDeploymentValidator(sharedConfigMetadataRegistry);
  }

  @Test
  void testValidateCreateRequest_Valid() {
    // Create a request with no advanced config keys
    CreateCloudEdgeDeploymentConfigRequest request =
        CreateCloudEdgeDeploymentConfigRequest.newBuilder()
            .setCloudEdgeDeploymentInputConfig(
                CloudEdgeDeploymentInputConfig.newBuilder()
                    .setClusterConfig(
                        ClusterConfig.newBuilder().setClusterName("test-cluster").build())
                    .addServiceConfigs(
                        ServiceConfig.newBuilder().setServiceName("test-service").build())
                    .build())
            .setConfigPermission(
                ConfigPermission.newBuilder()
                    .setRead(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
                    .setWrite(ConfigAccessType.CONFIG_ACCESS_TYPE_TRACEABLE)
                    .build())
            .build();

    // Return empty metadata - no validation will happen since there are no keys in the request
    when(sharedConfigMetadataRegistry.getSharedConfigMetadataWithWritePermission(
            ConfigAccessType.CONFIG_ACCESS_TYPE_TRACEABLE))
        .thenReturn(SharedConfigMetadata.getDefaultInstance());

    Status status = validator.validate(request);
    assertTrue(status.isOk());
  }

  @Test
  void testValidateCreateRequest_MissingPermission() {
    CreateCloudEdgeDeploymentConfigRequest request =
        CreateCloudEdgeDeploymentConfigRequest.newBuilder()
            .setCloudEdgeDeploymentInputConfig(
                CloudEdgeDeploymentInputConfig.newBuilder()
                    .setClusterConfig(
                        ClusterConfig.newBuilder().setClusterName("test-cluster").build())
                    .addServiceConfigs(
                        ServiceConfig.newBuilder().setServiceName("test-service").build())
                    .build())
            .build();

    Status status = validator.validate(request);
    assertFalse(status.isOk());
    assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    assertEquals("Config permission is required", status.getDescription());
  }

  @Test
  void testValidateUpdateRequest_Valid() {
    // Create a request with no advanced config keys
    UpdateCloudEdgeDeploymentConfigRequest request =
        UpdateCloudEdgeDeploymentConfigRequest.newBuilder()
            .setId("test-id")
            .setCloudEdgeDeploymentInputConfig(
                CloudEdgeDeploymentInputConfig.newBuilder()
                    .setClusterConfig(
                        ClusterConfig.newBuilder().setClusterName("test-cluster").build())
                    .addServiceConfigs(
                        ServiceConfig.newBuilder().setServiceName("test-service").build())
                    .build())
            .setConfigPermission(
                ConfigPermission.newBuilder()
                    .setRead(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
                    .setWrite(ConfigAccessType.CONFIG_ACCESS_TYPE_TRACEABLE)
                    .build())
            .build();

    // Return empty metadata - no validation will happen since there are no keys in the request
    when(sharedConfigMetadataRegistry.getSharedConfigMetadataWithWritePermission(
            ConfigAccessType.CONFIG_ACCESS_TYPE_TRACEABLE))
        .thenReturn(SharedConfigMetadata.getDefaultInstance());

    Status status = validator.validate(request);
    assertTrue(status.isOk());
  }

  @Test
  void testValidateUpdateRequest_MissingId() {
    UpdateCloudEdgeDeploymentConfigRequest request =
        UpdateCloudEdgeDeploymentConfigRequest.newBuilder()
            .setCloudEdgeDeploymentInputConfig(
                CloudEdgeDeploymentInputConfig.newBuilder()
                    .setClusterConfig(
                        ClusterConfig.newBuilder().setClusterName("test-cluster").build())
                    .addServiceConfigs(
                        ServiceConfig.newBuilder().setServiceName("test-service").build())
                    .build())
            .setConfigPermission(
                ConfigPermission.newBuilder()
                    .setRead(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
                    .setWrite(ConfigAccessType.CONFIG_ACCESS_TYPE_TRACEABLE)
                    .build())
            .build();

    Status status = validator.validate(request);
    assertFalse(status.isOk());
    assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    assertEquals("Cloud edge deployment config ID cannot be empty", status.getDescription());
  }

  @Test
  void testValidateDeleteRequest() {
    // Test case 1: Valid delete request
    DeleteCloudEdgeDeploymentConfigRequest validRequest =
        DeleteCloudEdgeDeploymentConfigRequest.newBuilder().setId("test-id").build();

    Status validStatus = validator.validate(validRequest);
    assertTrue(validStatus.isOk());
    assertEquals(Status.Code.OK, validStatus.getCode());

    // Test case 2: Invalid delete request with empty ID
    DeleteCloudEdgeDeploymentConfigRequest invalidRequest =
        DeleteCloudEdgeDeploymentConfigRequest.newBuilder().setId("").build();

    Status invalidStatus = validator.validate(invalidRequest);
    assertFalse(invalidStatus.isOk());
    assertEquals(Status.Code.INVALID_ARGUMENT, invalidStatus.getCode());
    assertEquals("Cloud edge deployment config ID cannot be empty", invalidStatus.getDescription());
  }

  @Test
  void testValidateCreateRequest_WithInvalidClusterConfigKey() {
    // Setup mock data - empty map means no keys are allowed
    SharedConfigMetadata metadata = SharedConfigMetadata.newBuilder().build();

    when(sharedConfigMetadataRegistry.getSharedConfigMetadataWithWritePermission(
            ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL))
        .thenReturn(metadata);

    // Create a request with a key that's not in the allowed list
    CreateCloudEdgeDeploymentConfigRequest request =
        CreateCloudEdgeDeploymentConfigRequest.newBuilder()
            .setCloudEdgeDeploymentInputConfig(
                CloudEdgeDeploymentInputConfig.newBuilder()
                    .setClusterConfig(
                        ClusterConfig.newBuilder()
                            .setClusterName("test-cluster")
                            .setAdvancedConfig(
                                ClusterAdvancedConfig.newBuilder()
                                    .setGenericConfig(
                                        Struct.newBuilder()
                                            .putFields(
                                                "nonExistentKey",
                                                Value.newBuilder().setStringValue("value").build())
                                            .build())
                                    .build())
                            .build())
                    .build())
            .setConfigPermission(
                ConfigPermission.newBuilder()
                    .setRead(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
                    .setWrite(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
                    .build())
            .build();

    // Validate and verify
    Status status = validator.validate(request);
    assertFalse(status.isOk());
    assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    assertTrue(status.getDescription().contains("can't update cluster advanced config key"));
  }

  @Test
  void testValidateCreateRequest_WithInvalidServiceConfigKey() {
    // Setup mock data - empty map means no keys are allowed
    SharedConfigMetadata metadata = SharedConfigMetadata.newBuilder().build();

    when(sharedConfigMetadataRegistry.getSharedConfigMetadataWithWritePermission(
            ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL))
        .thenReturn(metadata);

    // Create a request with a key that's not in the allowed list
    CreateCloudEdgeDeploymentConfigRequest request =
        CreateCloudEdgeDeploymentConfigRequest.newBuilder()
            .setCloudEdgeDeploymentInputConfig(
                CloudEdgeDeploymentInputConfig.newBuilder()
                    .setClusterConfig(
                        ClusterConfig.newBuilder().setClusterName("test-cluster").build())
                    .addServiceConfigs(
                        ServiceConfig.newBuilder()
                            .setServiceName("test-service")
                            .setAdvancedConfig(
                                ServiceAdvancedConfig.newBuilder()
                                    .setGenericConfig(
                                        Struct.newBuilder()
                                            .putFields(
                                                "nonExistentKey",
                                                Value.newBuilder().setStringValue("value").build())
                                            .build())
                                    .build())
                            .build())
                    .build())
            .setConfigPermission(
                ConfigPermission.newBuilder()
                    .setRead(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
                    .setWrite(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
                    .build())
            .build();

    // Validate and verify
    Status status = validator.validate(request);
    assertFalse(status.isOk());
    assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    assertTrue(status.getDescription().contains("can't update service advanced config key"));
  }

  @Test
  void testValidateUpdateRequest_WithInvalidClusterConfigKey() {
    // Setup mock data - empty map means no keys are allowed
    SharedConfigMetadata metadata = SharedConfigMetadata.newBuilder().build();

    when(sharedConfigMetadataRegistry.getSharedConfigMetadataWithWritePermission(
            ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL))
        .thenReturn(metadata);

    // Create an update request with a key that's not in the allowed list
    UpdateCloudEdgeDeploymentConfigRequest request =
        UpdateCloudEdgeDeploymentConfigRequest.newBuilder()
            .setId("test-id")
            .setCloudEdgeDeploymentInputConfig(
                CloudEdgeDeploymentInputConfig.newBuilder()
                    .setClusterConfig(
                        ClusterConfig.newBuilder()
                            .setClusterName("test-cluster")
                            .setAdvancedConfig(
                                ClusterAdvancedConfig.newBuilder()
                                    .setGenericConfig(
                                        Struct.newBuilder()
                                            .putFields(
                                                "nonExistentKey",
                                                Value.newBuilder().setStringValue("value").build())
                                            .build())
                                    .build())
                            .build())
                    .build())
            .setConfigPermission(
                ConfigPermission.newBuilder()
                    .setRead(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
                    .setWrite(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
                    .build())
            .build();

    // Validate and verify
    Status status = validator.validate(request);
    assertFalse(status.isOk());
    assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    assertTrue(status.getDescription().contains("can't update cluster advanced config key"));
  }

  @Test
  void testValidateCreateRequest_WithValidClusterConfigKey() {
    // Setup mock data - add the key to the map to make it valid
    Map<String, ConfigValueDescriptor> clusterConfigMap = new HashMap<>();
    clusterConfigMap.put("validKey", ConfigValueDescriptor.getDefaultInstance());

    SharedConfigMetadata metadata =
        SharedConfigMetadata.newBuilder().putAllClusterConfigDetails(clusterConfigMap).build();

    when(sharedConfigMetadataRegistry.getSharedConfigMetadataWithWritePermission(
            ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL))
        .thenReturn(metadata);

    // Create a request with a valid key that exists in the map
    CreateCloudEdgeDeploymentConfigRequest request =
        CreateCloudEdgeDeploymentConfigRequest.newBuilder()
            .setCloudEdgeDeploymentInputConfig(
                CloudEdgeDeploymentInputConfig.newBuilder()
                    .setClusterConfig(
                        ClusterConfig.newBuilder()
                            .setClusterName("test-cluster")
                            .setAdvancedConfig(
                                ClusterAdvancedConfig.newBuilder()
                                    .setGenericConfig(
                                        Struct.newBuilder()
                                            .putFields(
                                                "validKey",
                                                Value.newBuilder().setStringValue("value").build())
                                            .build())
                                    .build())
                            .build())
                    .build())
            .setConfigPermission(
                ConfigPermission.newBuilder()
                    .setRead(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
                    .setWrite(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
                    .build())
            .build();

    // Validate and verify - should be OK since the key is in the map
    Status status = validator.validate(request);
    assertTrue(status.isOk());
  }

  @Test
  void testValidateCreateRequest_WithValidServiceConfigKey() {
    // Setup mock data - add the key to the map to make it valid
    Map<String, ConfigValueDescriptor> serviceConfigMap = new HashMap<>();
    serviceConfigMap.put("validKey", ConfigValueDescriptor.getDefaultInstance());

    SharedConfigMetadata metadata =
        SharedConfigMetadata.newBuilder().putAllServiceConfigDetails(serviceConfigMap).build();

    when(sharedConfigMetadataRegistry.getSharedConfigMetadataWithWritePermission(
            ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL))
        .thenReturn(metadata);

    // Create a request with a valid key that exists in the map
    CreateCloudEdgeDeploymentConfigRequest request =
        CreateCloudEdgeDeploymentConfigRequest.newBuilder()
            .setCloudEdgeDeploymentInputConfig(
                CloudEdgeDeploymentInputConfig.newBuilder()
                    .setClusterConfig(
                        ClusterConfig.newBuilder().setClusterName("test-cluster").build())
                    .addServiceConfigs(
                        ServiceConfig.newBuilder()
                            .setServiceName("test-service")
                            .setAdvancedConfig(
                                ServiceAdvancedConfig.newBuilder()
                                    .setGenericConfig(
                                        Struct.newBuilder()
                                            .putFields(
                                                "validKey",
                                                Value.newBuilder().setStringValue("value").build())
                                            .build())
                                    .build())
                            .build())
                    .build())
            .setConfigPermission(
                ConfigPermission.newBuilder()
                    .setRead(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
                    .setWrite(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
                    .build())
            .build();

    // Validate and verify - should be OK since the key is in the map
    Status status = validator.validate(request);
    assertTrue(status.isOk());
  }

  @Test
  void testValidateUpdateRequest_WithGlobalPermissionAndOutputConfig() {
    // Create an update request with global permission and output config
    UpdateCloudEdgeDeploymentConfigRequest request =
        UpdateCloudEdgeDeploymentConfigRequest.newBuilder()
            .setId("test-id")
            .setCloudEdgeDeployedOutputConfig(CloudEdgeDeploymentOutputConfig.getDefaultInstance())
            .setConfigPermission(
                ConfigPermission.newBuilder()
                    .setRead(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
                    .setWrite(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
                    .build())
            .build();

    // Validate and verify
    Status status = validator.validate(request);
    assertFalse(status.isOk());
    assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    assertTrue(status.getDescription().contains("User with permission"));
    assertTrue(status.getDescription().contains("can't update out config"));
  }

  @Test
  void testValidateUpdateRequest_WithTraceablePermissionAndOutputConfig() {
    // Create an update request with traceable permission and output config
    UpdateCloudEdgeDeploymentConfigRequest request =
        UpdateCloudEdgeDeploymentConfigRequest.newBuilder()
            .setId("test-id")
            .setCloudEdgeDeployedOutputConfig(CloudEdgeDeploymentOutputConfig.getDefaultInstance())
            .setConfigPermission(
                ConfigPermission.newBuilder()
                    .setRead(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
                    .setWrite(ConfigAccessType.CONFIG_ACCESS_TYPE_TRACEABLE)
                    .build())
            .build();

    // Validate and verify - should be OK since traceable permission can update output config
    Status status = validator.validate(request);
    assertTrue(status.isOk());
  }

  @Test
  void testValidateInputConfig_WithDuplicateServiceNames() {
    // Create a request with duplicate service names
    CreateCloudEdgeDeploymentConfigRequest request =
        CreateCloudEdgeDeploymentConfigRequest.newBuilder()
            .setCloudEdgeDeploymentInputConfig(
                CloudEdgeDeploymentInputConfig.newBuilder()
                    .setClusterConfig(
                        ClusterConfig.newBuilder().setClusterName("test-cluster").build())
                    .addServiceConfigs(
                        ServiceConfig.newBuilder().setServiceName("duplicate-service").build())
                    .addServiceConfigs(
                        ServiceConfig.newBuilder().setServiceName("duplicate-service").build())
                    .build())
            .setConfigPermission(
                ConfigPermission.newBuilder()
                    .setRead(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
                    .setWrite(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
                    .build())
            .build();

    // Validate and verify
    Status status = validator.validate(request);
    assertFalse(status.isOk());
    assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
    assertEquals(
        "Duplicate service name found: duplicate-service. Service names must be unique.",
        status.getDescription());
  }

  @Test
  void testValidateInputConfig_WithUniqueServiceNames() {
    // Setup mock data
    SharedConfigMetadata metadata = SharedConfigMetadata.newBuilder().build();
    when(sharedConfigMetadataRegistry.getSharedConfigMetadataWithWritePermission(
            ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL))
        .thenReturn(metadata);

    // Create a request with unique service names
    CreateCloudEdgeDeploymentConfigRequest request =
        CreateCloudEdgeDeploymentConfigRequest.newBuilder()
            .setCloudEdgeDeploymentInputConfig(
                CloudEdgeDeploymentInputConfig.newBuilder()
                    .setClusterConfig(
                        ClusterConfig.newBuilder().setClusterName("test-cluster").build())
                    .addServiceConfigs(
                        ServiceConfig.newBuilder().setServiceName("service-1").build())
                    .addServiceConfigs(
                        ServiceConfig.newBuilder().setServiceName("service-2").build())
                    .addServiceConfigs(
                        ServiceConfig.newBuilder().setServiceName("service-3").build())
                    .build())
            .setConfigPermission(
                ConfigPermission.newBuilder()
                    .setRead(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
                    .setWrite(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
                    .build())
            .build();

    // Validate and verify - should be OK since all service names are unique
    Status status = validator.validate(request);
    assertTrue(status.isOk());
  }

  @Test
  void testValidateInputConfig_HttpsProtocolRequiresCertificateId() {
    // Setup mock data
    SharedConfigMetadata metadata = SharedConfigMetadata.newBuilder().build();
    when(sharedConfigMetadataRegistry.getSharedConfigMetadataWithWritePermission(
            ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL))
        .thenReturn(metadata);

    // Test case 1: HTTPS protocol without certificate ID (invalid)
    CreateCloudEdgeDeploymentConfigRequest invalidRequest =
        CreateCloudEdgeDeploymentConfigRequest.newBuilder()
            .setCloudEdgeDeploymentInputConfig(
                CloudEdgeDeploymentInputConfig.newBuilder()
                    .setClusterConfig(
                        ClusterConfig.newBuilder().setClusterName("test-cluster").build())
                    .addServiceConfigs(
                        ServiceConfig.newBuilder()
                            .setServiceName("test-service")
                            .addDomainConfigs(
                                DomainConfig.newBuilder()
                                    .setDomainName("example.com")
                                    .setProtocol(Protocol.PROTOCOL_HTTPS)
                                    .setCertificateId("") // Empty certificate ID
                                    .build())
                            .build())
                    .build())
            .setConfigPermission(
                ConfigPermission.newBuilder()
                    .setRead(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
                    .setWrite(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
                    .build())
            .build();

    // Validate and verify
    Status invalidStatus = validator.validate(invalidRequest);
    assertFalse(invalidStatus.isOk());
    assertEquals(Status.Code.INVALID_ARGUMENT, invalidStatus.getCode());
    assertEquals(
        "HTTPS protocol requires a certificate ID for domain example.com in service test-service",
        invalidStatus.getDescription());

    // Test case 2: HTTPS protocol with certificate ID (valid)
    CreateCloudEdgeDeploymentConfigRequest validRequest =
        CreateCloudEdgeDeploymentConfigRequest.newBuilder()
            .setCloudEdgeDeploymentInputConfig(
                CloudEdgeDeploymentInputConfig.newBuilder()
                    .setClusterConfig(
                        ClusterConfig.newBuilder().setClusterName("test-cluster").build())
                    .addServiceConfigs(
                        ServiceConfig.newBuilder()
                            .setServiceName("test-service")
                            .addDomainConfigs(
                                DomainConfig.newBuilder()
                                    .setDomainName("example.com")
                                    .setProtocol(Protocol.PROTOCOL_HTTPS)
                                    .setCertificateId("cert-123") // Valid certificate ID
                                    .build())
                            .build())
                    .build())
            .setConfigPermission(
                ConfigPermission.newBuilder()
                    .setRead(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
                    .setWrite(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
                    .build())
            .build();

    // Validate and verify
    Status validStatus = validator.validate(validRequest);
    assertTrue(validStatus.isOk());

    // Test case 3: HTTP protocol without certificate ID (valid)
    CreateCloudEdgeDeploymentConfigRequest httpRequest =
        CreateCloudEdgeDeploymentConfigRequest.newBuilder()
            .setCloudEdgeDeploymentInputConfig(
                CloudEdgeDeploymentInputConfig.newBuilder()
                    .setClusterConfig(
                        ClusterConfig.newBuilder().setClusterName("test-cluster").build())
                    .addServiceConfigs(
                        ServiceConfig.newBuilder()
                            .setServiceName("test-service")
                            .addDomainConfigs(
                                DomainConfig.newBuilder()
                                    .setDomainName("example.com")
                                    .setProtocol(Protocol.PROTOCOL_HTTP)
                                    .setCertificateId("") // Empty certificate ID is fine for HTTP
                                    .build())
                            .build())
                    .build())
            .setConfigPermission(
                ConfigPermission.newBuilder()
                    .setRead(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
                    .setWrite(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
                    .build())
            .build();

    // Validate and verify
    Status httpStatus = validator.validate(httpRequest);
    assertTrue(httpStatus.isOk());
  }

  @Test
  void testValidateInputConfig_WithDuplicateDomainNames() {
    // Setup mock data
    SharedConfigMetadata metadata = SharedConfigMetadata.newBuilder().build();
    when(sharedConfigMetadataRegistry.getSharedConfigMetadataWithWritePermission(
            ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL))
        .thenReturn(metadata);

    // Test case 1: Duplicate domain names across different services (invalid)
    CreateCloudEdgeDeploymentConfigRequest invalidRequest =
        CreateCloudEdgeDeploymentConfigRequest.newBuilder()
            .setCloudEdgeDeploymentInputConfig(
                CloudEdgeDeploymentInputConfig.newBuilder()
                    .setClusterConfig(
                        ClusterConfig.newBuilder().setClusterName("test-cluster").build())
                    .addServiceConfigs(
                        ServiceConfig.newBuilder()
                            .setServiceName("service-1")
                            .addDomainConfigs(
                                DomainConfig.newBuilder()
                                    .setDomainName("duplicate-domain.com")
                                    .setProtocol(Protocol.PROTOCOL_HTTP)
                                    .build())
                            .build())
                    .addServiceConfigs(
                        ServiceConfig.newBuilder()
                            .setServiceName("service-2")
                            .addDomainConfigs(
                                DomainConfig.newBuilder()
                                    .setDomainName(
                                        "duplicate-domain.com") // Same domain name as in service-1
                                    .setProtocol(Protocol.PROTOCOL_HTTP)
                                    .build())
                            .build())
                    .build())
            .setConfigPermission(
                ConfigPermission.newBuilder()
                    .setRead(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
                    .setWrite(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
                    .build())
            .build();

    // Validate and verify
    Status invalidStatus = validator.validate(invalidRequest);
    assertFalse(invalidStatus.isOk());
    assertEquals(Status.Code.INVALID_ARGUMENT, invalidStatus.getCode());
    assertEquals(
        "Duplicate domain name found: duplicate-domain.com. Domain names must be unique across all service configurations.",
        invalidStatus.getDescription());

    // Test case 2: Unique domain names across different services (valid)
    CreateCloudEdgeDeploymentConfigRequest validRequest =
        CreateCloudEdgeDeploymentConfigRequest.newBuilder()
            .setCloudEdgeDeploymentInputConfig(
                CloudEdgeDeploymentInputConfig.newBuilder()
                    .setClusterConfig(
                        ClusterConfig.newBuilder().setClusterName("test-cluster").build())
                    .addServiceConfigs(
                        ServiceConfig.newBuilder()
                            .setServiceName("service-1")
                            .addDomainConfigs(
                                DomainConfig.newBuilder()
                                    .setDomainName("service1-domain.com")
                                    .setProtocol(Protocol.PROTOCOL_HTTP)
                                    .build())
                            .build())
                    .addServiceConfigs(
                        ServiceConfig.newBuilder()
                            .setServiceName("service-2")
                            .addDomainConfigs(
                                DomainConfig.newBuilder()
                                    .setDomainName("service2-domain.com") // Different domain name
                                    .setProtocol(Protocol.PROTOCOL_HTTP)
                                    .build())
                            .build())
                    .build())
            .setConfigPermission(
                ConfigPermission.newBuilder()
                    .setRead(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
                    .setWrite(ConfigAccessType.CONFIG_ACCESS_TYPE_GLOBAL)
                    .build())
            .build();

    // Validate and verify
    Status validStatus = validator.validate(validRequest);
    assertTrue(validStatus.isOk());
  }
}
