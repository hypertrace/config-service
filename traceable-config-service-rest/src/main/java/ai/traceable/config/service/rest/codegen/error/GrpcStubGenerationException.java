package ai.traceable.config.service.rest.codegen.error;

public class GrpcStubGenerationException extends Throwable {
  public GrpcStubGenerationException(String msg) {
    super(msg);
  }

  public GrpcStubGenerationException(String msg, Throwable t) {
    super(msg, t);
  }

  public GrpcStubGenerationException(Throwable t) {
    super(t);
  }
}
