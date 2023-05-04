package ai.traceable.api.gateway.config.service.delegate;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.api.gateway.config.service.store.MetadataConfigStore;
import ai.traceable.api.gateway.config.service.v1.ConfigMetadata;
import ai.traceable.api.gateway.config.service.v1.CreateMetadataRequest;
import ai.traceable.api.gateway.config.service.v1.CreateMetadataResponse;
import ai.traceable.api.gateway.config.service.v1.RequestInfo;
import ai.traceable.api.gateway.config.service.v1.SourceInfo;
import ai.traceable.api.gateway.config.service.v1.StorageInfo;
import java.util.UUID;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ConfigMetadataCreatorImplTest {

  @SuppressWarnings("unused")
  @Mock
  private MetadataConfigStore mockMetadataConfigStore;

  @InjectMocks private ConfigMetadataCreatorImpl configMetadataCreatorImpl;

  @Test
  void testCreate() {
    final String uuid = UUID.randomUUID().toString();
    final String orgId = UUID.randomUUID().toString();

    final ConfigMetadata metadata =
        ConfigMetadata.newBuilder()
            .setOrgId(orgId)
            .addSourceInfo(
                SourceInfo.newBuilder()
                    .setRequestInfo(
                        RequestInfo.newBuilder().setUrl("/hello/Mars").putEnvVars("key1", "value1"))
                    .setStorageInfo(StorageInfo.newBuilder().setDirName(uuid)))
            .build();
    final RequestContext requestContext = new RequestContext();

    final CreateMetadataRequest request =
        CreateMetadataRequest.newBuilder().setConfigMetadata(metadata).build();
    final CreateMetadataResponse result = configMetadataCreatorImpl.create(request, requestContext);

    assertEquals(CreateMetadataResponse.newBuilder().build(), result);
  }
}
