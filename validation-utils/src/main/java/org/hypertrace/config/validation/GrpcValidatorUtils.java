package org.hypertrace.config.validation;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.Type;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Status;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class GrpcValidatorUtils {
  private static final JsonFormat.Printer JSON_PRINTER = JsonFormat.printer();

  private GrpcValidatorUtils() {}

  public static void validateRequestContextOrThrow(RequestContext requestContext) {
    if (requestContext.getTenantId().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Missing expected Tenant ID")
          .asRuntimeException();
    }
  }

  public static <T extends Message> void validateNonDefaultPresenceOrThrow(
      T source, int fieldNumber) {
    FieldDescriptor descriptor = source.getDescriptorForType().findFieldByNumber(fieldNumber);

    if (descriptor.isRepeated()) {
      validateNonDefaultPresenceRepeatedOrThrow(source, descriptor);
    } else if (descriptor.getType() == Type.MESSAGE
        && descriptor
            .getMessageType()
            .toProto()
            .getDefaultInstanceForType()
            .equals(source.getField(descriptor))) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Expected field value %s but not present:%n %s",
                  descriptor.getFullName(), printMessage(source)))
          .asRuntimeException();
    } else if (descriptor.getType() != Type.MESSAGE
        && (!source.hasField(descriptor)
            || source.getField(descriptor).equals(descriptor.getDefaultValue()))) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Expected field value %s but not present:%n %s",
                  descriptor.getFullName(), printMessage(source)))
          .asRuntimeException();
    }
  }

  private static <T extends Message> void validateNonDefaultPresenceRepeatedOrThrow(
      T source, FieldDescriptor descriptor) {
    if (source.getRepeatedFieldCount(descriptor) == 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Expected at least 1 value for repeated field %s but not present:%n %s",
                  descriptor.getFullName(), printMessage(source)))
          .asRuntimeException();
    }
  }

  public static String printMessage(Message message) {
    try {
      return JSON_PRINTER.print(message);
    } catch (Exception exception) {
      return message.toString();
    }
  }
}
