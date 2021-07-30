package ai.traceable.config.service;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class TestInterceptor implements ServerInterceptor {
  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
    Context ctx =
        Context.current()
            .withValue(
                RequestContext.CURRENT,
                RequestContext.forTenantId(
                    headers.get(Metadata.Key.of("x-tenant-id", Metadata.ASCII_STRING_MARSHALLER))));
    return Contexts.interceptCall(ctx, call, headers, next);
  }
}
