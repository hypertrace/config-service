package ai.traceable.api.gateway.config.service.delegate;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.api.gateway.config.service.store.MetadataConfigStore;
import ai.traceable.api.gateway.config.service.v1.ConfigMetadata;
import ai.traceable.api.gateway.config.service.v1.DeleteMetadataRequest;
import ai.traceable.api.gateway.config.service.v1.DeleteMetadataResponse;
import ai.traceable.api.gateway.config.service.v1.MetadataFilter;
import ai.traceable.api.gateway.config.service.v1.OrgIds;
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
class ConfigMetadataDeleterImplTest {

  @Mock private MetadataConfigStore mockMetadataConfigStore;

  @InjectMocks private ConfigMetadataDeleterImpl configMetadataDeleterImpl;

  @Test
  void testDelete() {
    final RequestContext requestContext = new RequestContext();
    final String uuid = UUID.randomUUID().toString();
    final String orgId = UUID.randomUUID().toString();
    final MetadataFilter filter =
        MetadataFilter.newBuilder().setOrgIds(OrgIds.newBuilder().addOrgId(orgId)).build();
    final DeleteMetadataRequest request =
        DeleteMetadataRequest.newBuilder().setMetadataFilter(filter).build();

    final ConfigMetadata metadata =
        ConfigMetadata.newBuilder()
            .setOrgId(orgId)
            .addSourceInfo(
                SourceInfo.newBuilder()
                    .setRequestInfo(
                        RequestInfo.newBuilder().setUrl("/hello/Mars").putHeaders("key1", "value1"))
                    .setStorageInfo(StorageInfo.newBuilder().setDirName(uuid)))
            .build();
    final List<ConfigMetadata> metadataList = List.of(metadata);
    when(mockMetadataConfigStore.getAllConfigData(requestContext, filter)).thenReturn(metadataList);

    when(mockMetadataConfigStore.deleteObjects(requestContext, List.of(orgId)))
        .thenReturn(List.of());

    final DeleteMetadataResponse result = configMetadataDeleterImpl.delete(request, requestContext);

    assertEquals(DeleteMetadataResponse.newBuilder().build(), result);
    verify(mockMetadataConfigStore).getAllConfigData(requestContext, filter);
    verify(mockMetadataConfigStore).deleteObjects(requestContext, List.of(orgId));
  }

  @Test
  void testDelete_ApiRoutesConfigStoreGetAllConfigDataReturnsNoItems() {
    final MetadataFilter filter = MetadataFilter.newBuilder().build();
    final DeleteMetadataRequest request =
        DeleteMetadataRequest.newBuilder().setMetadataFilter(filter).build();
    final RequestContext requestContext = new RequestContext();

    when(mockMetadataConfigStore.getAllConfigData(requestContext, filter)).thenReturn(emptyList());

    final DeleteMetadataResponse result = configMetadataDeleterImpl.delete(request, requestContext);

    assertEquals(DeleteMetadataResponse.newBuilder().build(), result);
    verify(mockMetadataConfigStore).getAllConfigData(requestContext, filter);
    verify(mockMetadataConfigStore, never()).deleteObjects(requestContext, emptyList());
  }
}
