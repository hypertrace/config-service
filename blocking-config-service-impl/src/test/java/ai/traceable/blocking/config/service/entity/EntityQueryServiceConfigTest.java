package ai.traceable.blocking.config.service.entity;

import static org.junit.jupiter.api.Assertions.*;

import com.typesafe.config.ConfigFactory;
import java.util.Map;
import org.junit.jupiter.api.Test;

class EntityQueryServiceConfigTest {
  EntityQueryServiceConfig entityQueryServiceConfig =
      new EntityQueryServiceConfig(
          ConfigFactory.parseMap(
              Map.of(
                  "entity.service",
                  Map.of(
                      "config",
                      Map.of("host", "localhost", "port", "8000"),
                      "attributeMap",
                      Map.of("environment.id", "envId", "environment.name", "envName")))));

  @Test
  void getEntityServiceHost() {
    assertEquals("localhost", entityQueryServiceConfig.getEntityServiceHost());
  }

  @Test
  void getEntityServicePort() {
    assertEquals(8000, entityQueryServiceConfig.getEntityServicePort());
  }

  @Test
  void getEnvironmentIdColumnName() {
    assertEquals("envId", entityQueryServiceConfig.getEnvironmentIdColumnName());
  }

  @Test
  void getEnvironmentNameColumnName() {
    assertEquals("envName", entityQueryServiceConfig.getEnvironmentNameColumnName());
  }
}
