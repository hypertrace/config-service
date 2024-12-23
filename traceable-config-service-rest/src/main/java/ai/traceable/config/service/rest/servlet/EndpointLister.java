package ai.traceable.config.service.rest.servlet;

import jakarta.ws.rs.Path;
import java.util.Set;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.model.Resource;

public class EndpointLister {

  public static void listEndpoints(ResourceConfig resourceConfig) {
    Set<Class<?>> resourceClasses = resourceConfig.getClasses();
    for (Class<?> resourceClass : resourceClasses) {
      if (!resourceClass.isAnnotationPresent(Path.class)) {
        continue;
      }
      System.out.println("Resource Class: " + resourceClass.getName());
      Resource resource = Resource.builder(resourceClass).build();
      printResource(resource, "");
    }
  }

  private static void printResource(Resource resource, String basePath) {
    String resourcePath = "";
    if (resource.getPath() != null) {
      resourcePath = resource.getPath();
    }
    if (!resourcePath.startsWith("/")) {
      resourcePath = "/" + resourcePath;
    }
    String fullPath = basePath + resourcePath;

    resource
        .getResourceMethods()
        .forEach(
            method -> {
              System.out.println(method.getHttpMethod() + " " + fullPath);
            });

    resource
        .getChildResources()
        .forEach(
            childResource -> {
              printResource(childResource, fullPath);
            });
  }
}
