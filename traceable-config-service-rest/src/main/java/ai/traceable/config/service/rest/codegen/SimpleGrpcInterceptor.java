package ai.traceable.config.service.rest.codegen;

import com.google.inject.Provider;
import com.google.protobuf.Message;
import java.lang.reflect.Method;
import javax.ws.rs.core.Response;
import lombok.SneakyThrows;
import net.bytebuddy.implementation.bind.annotation.AllArguments;
import net.bytebuddy.implementation.bind.annotation.Origin;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.This;
import org.hypertrace.core.grpcutils.context.RequestContext;

/**
 * Interceptor provided to the JaxRsResource generated. It is invoked on the REST endpoint
 * invocation
 */
public class SimpleGrpcInterceptor {
  private final Provider<RequestContext> requestContextProvider;
  private final Class<?> clazz;
  private final Object clientStub;
  private final Class<? extends Message> paramTypePTO;
  private final Class<? extends Message> returnTypePTO;

  public SimpleGrpcInterceptor(
      Provider<RequestContext> requestContextProvider,
      Class<?> clazz,
      Class<? extends Message> paramTypePTO,
      Class<? extends Message> returnTypePTO,
      Object clientStub) {
    this.requestContextProvider = requestContextProvider;
    this.clazz = clazz;
    this.paramTypePTO = paramTypePTO;
    this.returnTypePTO = returnTypePTO;
    this.clientStub = clientStub;
  }

  @RuntimeType
  @SneakyThrows
  public Object intercept(
      @AllArguments Object[] allArguments, @This Object thiz, @Origin Method method) {
    Message resultPto =
        requestContextProvider
            .get()
            .call(
                () ->
                    (Message)
                        clazz
                            .getDeclaredMethod(method.getName(), paramTypePTO)
                            .invoke(clientStub, allArguments[0]));
    return Response.ok(resultPto).build();
  }
}
