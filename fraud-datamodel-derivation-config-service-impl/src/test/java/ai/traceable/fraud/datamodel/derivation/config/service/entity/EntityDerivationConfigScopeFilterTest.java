package ai.traceable.fraud.datamodel.derivation.config.service.entity;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfig;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfigData;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationScopeFilter;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityScope;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityType;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EnvironmentScope;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EventDerivationConfigDetails;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.ParentDerivation;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.PrepopulatedSpanAttribute;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.Scope;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.SpanProjection;
import java.util.Map;
import org.junit.jupiter.api.Test;

class EntityDerivationConfigScopeFilterTest {

  @Test
  void prepopulatedEntity_alwaysIncluded() {
    var config = prepopulated("ip");
    var filter = filter("env-1", "api-1");
    assertTrue(applyScopeFilter(config, filter));
  }

  @Test
  void spanProjection_envMatch() {
    var config = spanProjection("e1", "env-1", "api-1");
    assertTrue(applyScopeFilter(config, filter("env-1", null)));
    assertFalse(applyScopeFilter(config, filter("env-2", null)));
  }

  @Test
  void spanProjection_apiMatch() {
    var config = spanProjection("e1", "env-1", "api-1");
    assertTrue(applyScopeFilter(config, filter(null, "api-1")));
    assertFalse(applyScopeFilter(config, filter(null, "api-2")));
  }

  @Test
  void spanProjection_bothMustMatch() {
    var config = spanProjection("e1", "env-1", "api-1");
    assertTrue(applyScopeFilter(config, filter("env-1", "api-1")));
    assertFalse(applyScopeFilter(config, filter("env-1", "api-2")));
    assertFalse(applyScopeFilter(config, filter("env-2", "api-1")));
  }

  @Test
  void spanProjection_emptyScope_matchesAll() {
    var config = spanProjectionEmptyScope("e1");
    assertTrue(applyScopeFilter(config, filter("env-1", "api-1")));
  }

  @Test
  void spanProjection_emptyFilter_matchesAll() {
    var config = spanProjection("e1", "env-1", "api-1");
    assertTrue(applyScopeFilter(config, EntityDerivationScopeFilter.getDefaultInstance()));
  }

  @Test
  void spanProjection_serviceType_ignoredForApiFilter() {
    var config = spanProjectionService("e1", "env-1", "svc-1");
    assertTrue(applyScopeFilter(config, filter(null, "api-1")));
  }

  @Test
  void parentDerivation_resolvesToParent() {
    var parent = spanProjection("parent", "env-1", "api-1");
    var child = parentDerivation("child", "parent");
    var configs = Map.of("parent", parent, "child", child);

    assertTrue(
        EntityDerivationConfigFilterUtil.applyScopeFilter(child, filter("env-1", null), configs));
    assertFalse(
        EntityDerivationConfigFilterUtil.applyScopeFilter(child, filter("env-2", null), configs));
  }

  @Test
  void parentDerivation_missingParent_included() {
    var child = parentDerivation("child", "missing");
    assertTrue(applyScopeFilter(child, filter("env-1", null)));
  }

  @Test
  void noValueSource_included() {
    var config =
        EntityDerivationConfig.newBuilder()
            .setId("no-source")
            .setData(EntityDerivationConfigData.newBuilder().setDisplayName("No Source"))
            .build();
    assertTrue(applyScopeFilter(config, filter("env-1", null)));
  }

  private boolean applyScopeFilter(
      EntityDerivationConfig config, EntityDerivationScopeFilter filter) {
    return EntityDerivationConfigFilterUtil.applyScopeFilter(
        config, filter, Map.of(config.getId(), config));
  }

  private EntityDerivationScopeFilter filter(String envId, String apiId) {
    var builder = EntityDerivationScopeFilter.newBuilder();
    if (envId != null) builder.addEnvironmentIds(envId);
    if (apiId != null) builder.addApiIds(apiId);
    return builder.build();
  }

  private EntityDerivationConfig prepopulated(String id) {
    return EntityDerivationConfig.newBuilder()
        .setId(id)
        .setData(
            EntityDerivationConfigData.newBuilder()
                .setDisplayName(id)
                .setPrepopulatedSpanAttribute(PrepopulatedSpanAttribute.getDefaultInstance()))
        .build();
  }

  private EntityDerivationConfig spanProjection(String id, String envId, String apiId) {
    return EntityDerivationConfig.newBuilder()
        .setId(id)
        .setData(
            EntityDerivationConfigData.newBuilder()
                .setDisplayName(id)
                .setSpanProjection(
                    SpanProjection.newBuilder()
                        .addEventDerivationConfigs(
                            EventDerivationConfigDetails.newBuilder()
                                .setScope(
                                    Scope.newBuilder()
                                        .setEnvironmentScope(
                                            EnvironmentScope.newBuilder().addEnvironments(envId))
                                        .setEntityScope(
                                            EntityScope.newBuilder()
                                                .addEntityIds(apiId)
                                                .setEntityType(EntityType.ENTITY_TYPE_API))))))
        .build();
  }

  private EntityDerivationConfig spanProjectionService(String id, String envId, String svcId) {
    return EntityDerivationConfig.newBuilder()
        .setId(id)
        .setData(
            EntityDerivationConfigData.newBuilder()
                .setDisplayName(id)
                .setSpanProjection(
                    SpanProjection.newBuilder()
                        .addEventDerivationConfigs(
                            EventDerivationConfigDetails.newBuilder()
                                .setScope(
                                    Scope.newBuilder()
                                        .setEnvironmentScope(
                                            EnvironmentScope.newBuilder().addEnvironments(envId))
                                        .setEntityScope(
                                            EntityScope.newBuilder()
                                                .addEntityIds(svcId)
                                                .setEntityType(EntityType.ENTITY_TYPE_SERVICE))))))
        .build();
  }

  private EntityDerivationConfig spanProjectionEmptyScope(String id) {
    return EntityDerivationConfig.newBuilder()
        .setId(id)
        .setData(
            EntityDerivationConfigData.newBuilder()
                .setDisplayName(id)
                .setSpanProjection(
                    SpanProjection.newBuilder()
                        .addEventDerivationConfigs(
                            EventDerivationConfigDetails.newBuilder()
                                .setScope(
                                    Scope.newBuilder()
                                        .setEnvironmentScope(
                                            EnvironmentScope.getDefaultInstance())))))
        .build();
  }

  private EntityDerivationConfig parentDerivation(String id, String parentId) {
    return EntityDerivationConfig.newBuilder()
        .setId(id)
        .setData(
            EntityDerivationConfigData.newBuilder()
                .setDisplayName(id)
                .setParentDerivation(
                    ParentDerivation.newBuilder().setParentEntityDerivationId(parentId)))
        .build();
  }
}
