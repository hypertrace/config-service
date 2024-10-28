package ai.traceable.config.service.rest.codegen.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class GrpcApiSpec {
  private String grpcClassName;
  private String host;
  private int port;
}
