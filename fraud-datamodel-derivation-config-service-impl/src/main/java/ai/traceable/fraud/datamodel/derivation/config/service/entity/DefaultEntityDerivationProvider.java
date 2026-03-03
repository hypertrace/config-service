package ai.traceable.fraud.datamodel.derivation.config.service.entity;

import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfig;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Singleton
public class DefaultEntityDerivationProvider {

  private static final String MANDATORY_ENTITIES_FILE = "mandatory_user_context_entities.yaml";
  private static final String PREPOPULATED_ATTRIBUTES_FILE = "prepopulated_span_attributes.yaml";

  @Getter private final List<EntityDerivationConfig> defaultEntityDerivations;
  private final Map<String, EntityDerivationConfig> defaultEntityDerivationsById;

  @Inject
  public DefaultEntityDerivationProvider() {
    List<EntityDerivationConfig> allDefaults = new ArrayList<>();
    allDefaults.addAll(loadEntities(MANDATORY_ENTITIES_FILE));
    allDefaults.addAll(loadEntities(PREPOPULATED_ATTRIBUTES_FILE));

    this.defaultEntityDerivations = Collections.unmodifiableList(allDefaults);
    this.defaultEntityDerivationsById =
        defaultEntityDerivations.stream()
            .collect(Collectors.toUnmodifiableMap(EntityDerivationConfig::getId, config -> config));
  }

  public boolean isDefaultEntity(String id) {
    return defaultEntityDerivationsById.containsKey(id);
  }

  public EntityDerivationConfig getDefaultEntity(String id) {
    return defaultEntityDerivationsById.get(id);
  }

  private List<EntityDerivationConfig> loadEntities(String resourceFile) {
    return YamlFileReader.readEntitiesFromYaml(resourceFile);
  }
}
