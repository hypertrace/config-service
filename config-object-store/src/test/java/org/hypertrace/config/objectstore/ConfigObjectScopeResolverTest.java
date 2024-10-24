package org.hypertrace.config.objectstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Builder;
import org.junit.jupiter.api.Test;

public class ConfigObjectScopeResolverTest {

  private static final TestInternalScope GRANDPARENT_1_SCOPE =
      TestInternalScope.builder()
          .type("grandparent")
          .id("gp1")
          .parentScope(Optional.empty())
          .build();
  private static final TestInternalScope GRANDPARENT_2_SCOPE =
      TestInternalScope.builder()
          .type("grandparent")
          .id("gp2")
          .parentScope(Optional.empty())
          .build();
  private static final TestInternalScope PARENT_1_SCOPE =
      TestInternalScope.builder()
          .type("parent")
          .id("p1")
          .parentScope(Optional.of(GRANDPARENT_1_SCOPE))
          .build();
  private static final TestInternalScope PARENT_2_SCOPE =
      TestInternalScope.builder()
          .type("parent")
          .id("p2")
          .parentScope(Optional.of(GRANDPARENT_1_SCOPE))
          .build();
  private static final TestInternalScope CHILD_1_SCOPE =
      TestInternalScope.builder()
          .type("child")
          .id("c1")
          .parentScope(Optional.of(PARENT_1_SCOPE))
          .build();
  private static final TestInternalScope CHILD_2_SCOPE =
      TestInternalScope.builder()
          .type("child")
          .id("c2")
          .parentScope(Optional.of(PARENT_2_SCOPE))
          .build();
  private static final TestInternalScope CHILD_3_SCOPE =
      TestInternalScope.builder()
          .type("child")
          .id("c3")
          .parentScope(Optional.of(PARENT_1_SCOPE))
          .build();

  private static final TestInternalObject DEFAULT_OBJECT =
      TestInternalObject.builder().rank(25).enabled(Optional.of(true)).build();

  private static final TestInternalObject GRANDPARENT_OBJECT_1 =
      TestInternalObject.builder()
          .id("grandparent-1")
          .rank(30)
          .enabled(Optional.empty())
          .scope(GRANDPARENT_1_SCOPE)
          .build();

  private static final TestInternalObject PARENT_OBJECT_1 =
      TestInternalObject.builder()
          .id("parent-1")
          .rank(20)
          .enabled(Optional.of(true))
          .scope(PARENT_1_SCOPE)
          .build();

  private static final TestInternalObject CHILD_OBJECT_1 =
      TestInternalObject.builder()
          .id("child-1")
          .rank(10)
          .enabled(Optional.empty())
          .scope(CHILD_1_SCOPE)
          .build();

  private static final TestInternalObject CHILD_OBJECT_2 =
      TestInternalObject.builder()
          .id("child-2")
          .rank(2)
          .enabled(Optional.of(false))
          .scope(CHILD_2_SCOPE)
          .build();

  List<TestInternalObject> configObjects =
      List.of(CHILD_OBJECT_1, CHILD_OBJECT_2, PARENT_OBJECT_1, GRANDPARENT_OBJECT_1);

  @Test
  public void testResolutionWithoutDefault() {
    TestConfigObjectScopeResolver resolver = new TestConfigObjectScopeResolver();

    TestInternalResolvedConfig grandParent1ResolvedConfig =
        resolver.convertConfig(GRANDPARENT_OBJECT_1);

    assertTrue(resolver.getResolvedData(configObjects, GRANDPARENT_2_SCOPE).isEmpty());

    verifyResolvedConfigs(resolver, grandParent1ResolvedConfig);
  }

  @Test
  public void testResolutionWithDefault() {
    TestConfigObjectScopeResolver resolver = new TestConfigObjectScopeResolver(DEFAULT_OBJECT);

    TestInternalResolvedConfig grandParent1ResolvedConfig =
        resolver.convertConfig(
            GRANDPARENT_OBJECT_1.toBuilder().enabled(DEFAULT_OBJECT.getEnabled()).build());

    TestInternalResolvedConfig grandParent2ResolvedConfig = resolver.convertConfig(DEFAULT_OBJECT);
    assertEquals(
        grandParent2ResolvedConfig,
        resolver.getResolvedData(configObjects, GRANDPARENT_2_SCOPE).get());

    verifyResolvedConfigs(resolver, grandParent1ResolvedConfig);
  }

  private void verifyResolvedConfigs(
      TestConfigObjectScopeResolver resolver,
      TestInternalResolvedConfig grandParent1ResolvedConfig) {
    TestInternalResolvedConfig parent1ResolvedConfig =
        resolver.convertConfig(
            PARENT_OBJECT_1.toBuilder().rank(grandParent1ResolvedConfig.getRank()).build());
    TestInternalResolvedConfig parent2ResolvedConfig = grandParent1ResolvedConfig;
    TestInternalResolvedConfig child1ResolvedConfig =
        resolver.convertConfig(
            CHILD_OBJECT_1.toBuilder()
                .rank(grandParent1ResolvedConfig.getRank())
                .enabled(PARENT_OBJECT_1.getEnabled())
                .build());
    TestInternalResolvedConfig child2ResolvedConfig =
        resolver.convertConfig(
            CHILD_OBJECT_2.toBuilder().rank(grandParent1ResolvedConfig.getRank()).build());
    TestInternalResolvedConfig child3ResolvedConfig = parent1ResolvedConfig;

    List<TestInternalResolvedConfig> resolvedConfigs =
        resolver.getAllResolvedConfigData(configObjects);
    assertEquals(4, resolvedConfigs.size());

    Map<TestInternalScope, TestInternalResolvedConfig> configMap =
        resolvedConfigs.stream()
            .collect(Collectors.toMap(TestInternalResolvedConfig::getScope, Function.identity()));
    assertEquals(4, configMap.size());

    assertEquals(grandParent1ResolvedConfig, configMap.get(GRANDPARENT_1_SCOPE));
    assertFalse(configMap.containsKey(GRANDPARENT_2_SCOPE));
    assertEquals(parent1ResolvedConfig, configMap.get(PARENT_1_SCOPE));
    assertFalse(configMap.containsKey(PARENT_2_SCOPE));
    assertEquals(child1ResolvedConfig, configMap.get(CHILD_1_SCOPE));
    assertEquals(child2ResolvedConfig, configMap.get(CHILD_2_SCOPE));
    assertFalse(configMap.containsKey(CHILD_3_SCOPE));

    assertEquals(
        grandParent1ResolvedConfig,
        resolver.getResolvedData(configObjects, GRANDPARENT_1_SCOPE).get());
    assertEquals(
        parent1ResolvedConfig, resolver.getResolvedData(configObjects, PARENT_1_SCOPE).get());
    assertEquals(
        parent2ResolvedConfig, resolver.getResolvedData(configObjects, PARENT_2_SCOPE).get());
    assertEquals(
        child1ResolvedConfig, resolver.getResolvedData(configObjects, CHILD_1_SCOPE).get());
    assertEquals(
        child2ResolvedConfig, resolver.getResolvedData(configObjects, CHILD_2_SCOPE).get());
    assertEquals(
        child3ResolvedConfig, resolver.getResolvedData(configObjects, CHILD_3_SCOPE).get());
  }

  @lombok.Value
  @Builder
  private static class TestInternalScope {
    String type;
    String id;
    Optional<TestInternalScope> parentScope;
  }

  @lombok.Value
  @Builder(toBuilder = true)
  private static class TestInternalObject {
    TestInternalScope scope;
    String id;
    Optional<Boolean> enabled;
    int rank;
  }

  @lombok.Value
  @Builder
  private static class TestInternalResolvedConfig {
    TestInternalScope scope;
    boolean enabled;
    int rank;
  }

  private static class TestConfigObjectScopeResolver
      extends ConfigObjectScopeResolver<
          TestInternalObject, TestInternalScope, TestInternalResolvedConfig> {

    public TestConfigObjectScopeResolver(TestInternalObject defaultConfig) {
      super(defaultConfig);
    }

    public TestConfigObjectScopeResolver() {
      super();
    }

    @Override
    protected TestInternalScope extractScope(TestInternalObject configData) {
      return configData.getScope();
    }

    @Override
    protected List<TestInternalScope> getResolutionScopesWithIncreasingPriority(
        TestInternalScope scopeObject) {
      List<TestInternalScope> scopes = new ArrayList<>();
      scopes.add(scopeObject);
      Optional<TestInternalScope> parentScope = scopeObject.getParentScope();
      while (parentScope.isPresent()) {
        scopes.add(0, parentScope.get());
        parentScope = parentScope.get().getParentScope();
      }
      return scopes;
    }

    @Override
    protected TestInternalResolvedConfig convertConfig(TestInternalObject configStoreData) {
      return TestInternalResolvedConfig.builder()
          .scope(configStoreData.getScope())
          .enabled(configStoreData.getEnabled().orElse(false))
          .rank(configStoreData.getRank())
          .build();
    }

    @Override
    protected TestInternalObject mergeConfigs(
        TestInternalObject fallbackConfig, TestInternalObject preferredConfig) {
      return TestInternalObject.builder()
          .id(preferredConfig.getId())
          .scope(preferredConfig.getScope())
          .rank(
              preferredConfig.getRank() > fallbackConfig.getRank()
                  ? preferredConfig.getRank()
                  : fallbackConfig.getRank())
          .enabled(
              preferredConfig.getEnabled().isPresent()
                  ? preferredConfig.getEnabled()
                  : fallbackConfig.getEnabled())
          .build();
    }
  }
}
