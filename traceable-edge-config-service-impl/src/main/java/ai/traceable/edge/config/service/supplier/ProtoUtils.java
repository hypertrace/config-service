package ai.traceable.edge.config.service.supplier;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

public abstract class ProtoUtils {
  private static final JsonFormat.Printer jsonPrinter = JsonFormat.printer();

  public static String serialize(Message m) {
    try {
      return jsonPrinter.preservingProtoFieldNames().print(m);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }
}
