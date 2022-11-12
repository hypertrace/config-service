package ai.traceable.mock.config.service;

import io.grpc.*;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MockConfigServer {
  private static final int PORT = 50102;
  private Server server;

  public void start() throws IOException {
    log.info("Server has Started");

    server =
        ServerBuilder.forPort(PORT)
            .addService(new PiiFilterConfigServiceImpl())
            .addService(new LocalProcessingConfigServiceImpl())
            .addService(new BlockingConfigServiceImpl())
            .addService(new ExternalUserAttributionConfigServiceImpl())
            .addService(new ExternalDataClassificationConfigServiceImpl())
            .addService(new ExternalAgentAttributeConfigServiceImpl())
            .build()
            .start();
  }

  public void blockUntilShutdown() throws InterruptedException {
    if (server == null) {
      return;
    }
    server.awaitTermination();
  }

  public static void main(String[] args) throws InterruptedException, IOException {
    MockConfigServer server = new MockConfigServer();
    server.start();
    server.blockUntilShutdown();
  }
}
