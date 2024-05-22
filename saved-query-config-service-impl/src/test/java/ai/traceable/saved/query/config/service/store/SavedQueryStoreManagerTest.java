package ai.traceable.saved.query.config.service.store;

import ai.traceable.config.utils.TimestampConverter;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.saved.query.config.service.DefaultSavedQueryConfig;
import ai.traceable.saved.query.config.service.v1.GetAllSavedQueryUsersResponse;
import ai.traceable.saved.query.config.service.v1.SavedQuery;
import ai.traceable.saved.query.config.service.v1.User;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class SavedQueryStoreManagerTest {

  @Mock private DeletedSavedQueryConfigStore deletedSavedQueryConfigStore;
  @Mock private SavedQueryConfigStore savedQueryConfigStore;
  @Mock private DefaultSavedQueryConfig defaultSavedQueryConfig;
  @Mock private TimestampConverter timestampConverter;
  @Mock private UuidGenerator uuidGenerator;

  @Test
  void testGetAllSavedQueryUsers() {
    SavedQueryStoreManager storeManager =
        new SavedQueryStoreManager(
            deletedSavedQueryConfigStore,
            defaultSavedQueryConfig,
            savedQueryConfigStore,
            timestampConverter,
            uuidGenerator);

    RequestContext requestContext = Mockito.mock(RequestContext.class);
    List<SavedQuery> mockQueries =
        List.of(
            SavedQuery.newBuilder()
                .setAuthor(User.newBuilder().setId("user1").setName("user 1"))
                .build(),
            SavedQuery.newBuilder()
                .setAuthor(User.newBuilder().setId("user1").setName("user 1"))
                .build(),
            SavedQuery.newBuilder().setCreatedByUserId("user2").build());

    Mockito.when(savedQueryConfigStore.getAllConfigData(requestContext)).thenReturn(mockQueries);

    GetAllSavedQueryUsersResponse response = storeManager.getAllSavedQueryUsers(requestContext);
    Assertions.assertEquals(2, response.getUsersCount());
    Assertions.assertTrue(
        response.getUsersList().stream().map(User::getId).anyMatch(id -> id.equals("user1")));
    Assertions.assertTrue(
        response.getUsersList().stream().map(User::getId).anyMatch(id -> id.equals("user2")));
  }
}
