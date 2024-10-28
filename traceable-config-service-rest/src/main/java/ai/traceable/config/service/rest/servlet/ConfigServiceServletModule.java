package ai.traceable.config.service.rest.servlet;

import com.google.inject.servlet.ServletModule;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class ConfigServiceServletModule extends ServletModule {

  @Override
  protected void configureServlets() {
    bind(RequestContext.class).toProvider(ContextProvider.class);
    serve("/*").with(ConfigServiceRestServlet.class);
  }
}
