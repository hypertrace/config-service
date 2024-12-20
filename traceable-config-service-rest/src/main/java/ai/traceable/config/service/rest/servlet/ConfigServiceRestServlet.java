package ai.traceable.config.service.rest.servlet;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.glassfish.jersey.servlet.ServletContainer;

@Singleton
class ConfigServiceRestServlet extends ServletContainer {

  @Inject
  ConfigServiceRestServlet(ConfigServiceRestResourceConfig resourceConfig) {
    super(resourceConfig);
  }
}
