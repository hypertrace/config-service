package ai.traceable.config.service.rest.codegen.error;

public class JaxRsResourceGenerationException extends Throwable {
  public JaxRsResourceGenerationException(String msg) {
    super(msg);
  }

  public JaxRsResourceGenerationException(String msg, Throwable t) {
    super(msg, t);
  }

  public JaxRsResourceGenerationException(Throwable t) {
    super(t);
  }
}
