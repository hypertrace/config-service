package ai.traceable.config.service.rest.codegen;

import static ai.traceable.config.service.rest.codegen.GrpcCodegenConstants.SAVE_GENERATED_CLASSES_TO_LOCATION;

import ai.traceable.config.service.rest.codegen.error.JaxRsResourceGenerationException;
import com.google.inject.Provider;
import com.google.protobuf.Message;
import io.grpc.stub.AbstractStub;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.annotation.AnnotationDescription;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.implementation.MethodDelegation;
import org.hypertrace.core.grpcutils.context.RequestContext;

/**
 * Default resource creator that creates a JaxRsResource for a given rpc stub This class uses
 * ByteBuddy to generate a JAX-RS resource. Each method in the rpc definition maps to a REST
 * endpoint.
 *
 * <pre>
 * @Path("SampleGrpc")
 * public class SampleGrpcDynamicResource {
 *     @POST
 *     @Path("sampleRequest")
 *     @Consumes({"application/json"})
 *     @Produces({"application/json"})
 *     public Response sampleRequest(SampleRequest request) {
 *         return (Response)delegate$l5qihi1.intercept(new Object[]{request}, this, cachedValue$eCjSrnRc$mjkh3a0);
 *     }
 * }
 * </pre>
 */
@Slf4j
@Singleton
@AllArgsConstructor(onConstructor_ = {@Inject})
public class DefaultJaxRsResourceCreator implements JaxRsResourceCreator {
  private static final String VALUE = "value";
  private static final String REQUEST_PARAM_NAME = "request";
  private static final String DEFAULT_RESOURCE_CLASSNAME_SUFFIX = "DynamicResource";
  private final Provider<RequestContext> requestContextProvider;

  public Object createJaxRsResource(
      ClassLoader classLoader, Class<? extends AbstractStub> clazz, AbstractStub<?> clientStub)
      throws JaxRsResourceGenerationException {
    try {
      if (clientStub == null) {
        throw new JaxRsResourceGenerationException("No clientStub provided for " + clazz.getName());
      }
      String clientName = clazz.getEnclosingClass().getSimpleName();
      List<Method> overridableMethods = getOverridableMethods(clazz);
      Class<?> controllerType =
          createRuntimeJaxRsResource(
              classLoader, clazz, clientName, overridableMethods, clientStub);
      return controllerType.getConstructor().newInstance();
    } catch (Throwable t) {
      throw new JaxRsResourceGenerationException(t);
    }
  }

  private Class<?> createRuntimeJaxRsResource(
      ClassLoader classLoader,
      Class<?> clazz,
      String clientName,
      List<Method> overridableMethods,
      Object clientStub)
      throws IOException {
    DynamicType.Builder<Object> objectBuilder =
        new ByteBuddy()
            .subclass(Object.class)
            .name(
                getClass().getPackageName() + "." + clientName + DEFAULT_RESOURCE_CLASSNAME_SUFFIX)
            .annotateType(
                AnnotationDescription.Builder.ofType(Path.class)
                    .define(VALUE, clientName) // Ensure the class has a @Path annotation
                    .build());

    for (Method overridableMethod : overridableMethods) {
      Class<?>[] parameterTypes = overridableMethod.getParameterTypes();
      @SuppressWarnings("unchecked")
      Class<? extends Message> parameter = (Class<? extends Message>) parameterTypes[0];
      @SuppressWarnings("unchecked")
      Class<? extends Message> returnType =
          (Class<? extends Message>) overridableMethod.getReturnType();
      SimpleGrpcInterceptor interceptor =
          new SimpleGrpcInterceptor(
              requestContextProvider, clazz, parameter, returnType, clientStub);
      objectBuilder =
          objectBuilder
              .defineMethod(overridableMethod.getName(), Response.class, Modifier.PUBLIC)
              .withParameter(parameter, REQUEST_PARAM_NAME)
              .intercept(MethodDelegation.to(interceptor))
              .annotateMethod(
                  AnnotationDescription.Builder.ofType(POST.class).build(),
                  AnnotationDescription.Builder.ofType(Path.class)
                      .define(VALUE, overridableMethod.getName())
                      .build(),
                  AnnotationDescription.Builder.ofType(Consumes.class)
                      .defineArray(VALUE, new String[] {MediaType.APPLICATION_JSON})
                      .build(),
                  AnnotationDescription.Builder.ofType(Produces.class)
                      .defineArray(VALUE, new String[] {MediaType.APPLICATION_JSON})
                      .build());
    }
    DynamicType.Unloaded<Object> unloadedClass = objectBuilder.make();
    unloadedClass.saveIn(new File(SAVE_GENERATED_CLASSES_TO_LOCATION));
    return objectBuilder.make().load(classLoader).getLoaded();
  }

  private List<Method> getOverridableMethods(Class<?> clazz) {
    return Stream.of(clazz.getMethods())
        .filter(
            c ->
                !Modifier.isFinal(c.getModifiers())
                    && !Modifier.isNative(c.getModifiers())
                    && !Modifier.isStatic(c.getModifiers()))
        .filter(m -> !m.getDeclaringClass().equals(Object.class))
        .filter(m -> m.getParameterTypes().length == 1)
        .collect(Collectors.toUnmodifiableList());
  }
}
