package ai.traceable.api.gateway.config.service.delegate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import ai.traceable.api.gateway.config.service.store.MetadataConfigStore;
import ai.traceable.api.gateway.config.service.v1.ConfigMetadata;
import ai.traceable.api.gateway.config.service.v1.GetMetadataRequest;
import ai.traceable.api.gateway.config.service.v1.GetMetadataResponse;
import ai.traceable.api.gateway.config.service.v1.MetadataFilter;
import ai.traceable.api.gateway.config.service.v1.RequestInfo;
import ai.traceable.api.gateway.config.service.v1.SourceInfo;
import ai.traceable.api.gateway.config.service.v1.StorageInfo;
import java.util.List;
import java.util.UUID;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ConfigMetadataGetterImplTest {

  @Mock private MetadataConfigStore mockMetadataConfigStore;

  @InjectMocks private ConfigMetadataGetterImpl configMetadataGetterImpl;

  @Test
  void testGet() {
    final MetadataFilter filter = MetadataFilter.newBuilder().build();
    final GetMetadataRequest request =
        GetMetadataRequest.newBuilder().setMetadataFilter(filter).build();
    final RequestContext requestContext = new RequestContext();
    final String uuid = UUID.randomUUID().toString();
    final String orgId = UUID.randomUUID().toString();

    final ConfigMetadata metadata =
        ConfigMetadata.newBuilder()
            .setOrgId(orgId)
            .addSourceInfo(
                SourceInfo.newBuilder()
                    .setRequestInfo(
                        RequestInfo.newBuilder().setUrl("/hello/Mars").putHeaders("key1", "value1"))
                    .setStorageInfo(StorageInfo.newBuilder().setDirName(uuid)))
            .build();
    final GetMetadataResponse expectedResult =
        GetMetadataResponse.newBuilder().addConfigMetadata(metadata).build();
    when(mockMetadataConfigStore.getAllConfigData(requestContext, filter))
        .thenReturn(List.of(metadata));

    final GetMetadataResponse result = configMetadataGetterImpl.get(request, requestContext);

    assertEquals(expectedResult, result);
  }
}
