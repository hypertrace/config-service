package ai.traceable.ast.hooks.config.service.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import ai.traceable.ast.hooks.config.service.v1.AstHookTest;
import ai.traceable.ast.hooks.config.service.v1.AstHookTestFilter;
import ai.traceable.ast.hooks.config.service.v1.TestIdFilter;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AstHooksTestConfigStoreTest {

  @Mock private RequestContext mockRequestContext;

  @Test
  void testGetAllFilteredAstHookTests_WithIdFilter() {
    // Setup test data
    AstHookTest test1 = AstHookTest.newBuilder().setId("id1").build();
    AstHookTest test2 = AstHookTest.newBuilder().setId("id2").build();
    AstHookTest test3 = AstHookTest.newBuilder().setId("id3").build();

    // Create spy and mock getAllConfigData
    AstHooksTestConfigStore store = spy(new AstHooksTestConfigStore(null, null));
    doReturn(List.of(test1, test2, test3)).when(store).getAllConfigData(mockRequestContext);

    // Create filter for id1 and id3
    TestIdFilter idFilter = TestIdFilter.newBuilder().addIds("id1").addIds("id3").build();
    AstHookTestFilter filter = AstHookTestFilter.newBuilder().setTestIdFilter(idFilter).build();

    // Get filtered results
    List<AstHookTest> results =
        store.getAllFilteredAstHookTests(mockRequestContext, List.of(filter));

    // Verify
    assertEquals(2, results.size());
    assertTrue(results.stream().anyMatch(t -> t.getId().equals("id1")));
    assertTrue(results.stream().anyMatch(t -> t.getId().equals("id3")));
  }

  @Test
  void testGetAllFilteredAstHookTests_NoMatch() {
    // Setup test data
    AstHookTest test1 = AstHookTest.newBuilder().setId("id1").build();

    // Create spy and mock getAllConfigData
    AstHooksTestConfigStore store = spy(new AstHooksTestConfigStore(null, null));
    doReturn(List.of(test1)).when(store).getAllConfigData(mockRequestContext);

    // Create filter with non-matching ID
    TestIdFilter idFilter = TestIdFilter.newBuilder().addIds("id99").build();
    AstHookTestFilter filter = AstHookTestFilter.newBuilder().setTestIdFilter(idFilter).build();

    // Get filtered results
    List<AstHookTest> results =
        store.getAllFilteredAstHookTests(mockRequestContext, List.of(filter));

    // Verify no results
    assertTrue(results.isEmpty());
  }
}
