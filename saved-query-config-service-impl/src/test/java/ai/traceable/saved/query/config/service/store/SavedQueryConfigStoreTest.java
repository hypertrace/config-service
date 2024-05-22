package ai.traceable.saved.query.config.service.store;

import ai.traceable.saved.query.config.service.v1.GetSavedQueriesFilter;
import ai.traceable.saved.query.config.service.v1.GetSavedQueriesRequest;
import ai.traceable.saved.query.config.service.v1.SavedQuery;
import ai.traceable.saved.query.config.service.v1.User;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class SavedQueryConfigStoreTest {
  @Mock ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub;
  @Mock ConfigChangeEventGenerator configChangeEventGenerator;

  @Test
  void testFilter() {
    GetSavedQueriesRequest scopeFilter =
        GetSavedQueriesRequest.newBuilder()
            .setFilter(GetSavedQueriesFilter.newBuilder().setScope("someScope").build())
            .build();

    SavedQueryConfigStore configStore =
        new SavedQueryConfigStore(configServiceBlockingStub, configChangeEventGenerator);

    Assertions.assertTrue(
        configStore
            .filterConfigData(
                SavedQuery.newBuilder()
                    .setScope("someScope")
                    .setAuthor(User.newBuilder().setId("user1").build())
                    .build(),
                scopeFilter)
            .isPresent());

    Assertions.assertTrue(
        configStore
            .filterConfigData(
                SavedQuery.newBuilder()
                    .setScope("someOtherScope")
                    .setAuthor(User.newBuilder().setId("user1").build())
                    .build(),
                scopeFilter)
            .isEmpty());

    GetSavedQueriesRequest userFilter =
        GetSavedQueriesRequest.newBuilder()
            .setFilter(GetSavedQueriesFilter.newBuilder().addUserIds("user1").build())
            .build();

    Assertions.assertTrue(
        configStore
            .filterConfigData(
                SavedQuery.newBuilder()
                    .setScope("someOtherScope")
                    .setAuthor(User.newBuilder().setId("user1").build())
                    .build(),
                userFilter)
            .isPresent());

    Assertions.assertTrue(
        configStore
            .filterConfigData(
                SavedQuery.newBuilder()
                    .setScope("someOtherScope")
                    .setAuthor(User.newBuilder().setId("user2").build())
                    .build(),
                userFilter)
            .isEmpty());
  }
}
