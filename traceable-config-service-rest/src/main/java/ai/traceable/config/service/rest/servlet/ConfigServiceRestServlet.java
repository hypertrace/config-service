package ai.traceable.config.service.rest.servlet;

import javax.inject.Inject;
import javax.inject.Singleton;
import org.glassfish.jersey.servlet.ServletContainer;

@Singleton
class ConfigServiceRestServlet extends ServletContainer {

  @Inject
  ConfigServiceRestServlet(ConfigServiceRestResourceConfig resourceConfig) {
    super(resourceConfig);
  }
}
