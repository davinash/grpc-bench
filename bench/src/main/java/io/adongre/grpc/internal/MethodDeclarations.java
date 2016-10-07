package io.adongre.grpc.internal;

import io.adongre.grpc.generated.ScanFormattedResponse;
import io.adongre.grpc.generated.ScanRequest;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.lite.ProtoLiteUtils;

/**
 * Created by adongre on 10/7/16.
 */
public class MethodDeclarations {

  public static final MethodDescriptor<ScanRequest, ScanFormattedResponse> scanMethod =
      MethodDescriptor.create(MethodDescriptor.MethodType.SERVER_STREAMING,
          "AmpoolCore/scanFlowControlledStreaming",
          ProtoLiteUtils.marshaller(ScanRequest.getDefaultInstance()),
          ProtoLiteUtils.marshaller(ScanFormattedResponse.getDefaultInstance()));
}
