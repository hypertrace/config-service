package ai.traceable.edge.decision.config.service.error;

public class JexlParserException extends RuntimeException {
  public JexlParserException(String message, Throwable cause) {
    super(message, cause);
  }

  public JexlParserException(String message) {
    super(message);
  }
}
