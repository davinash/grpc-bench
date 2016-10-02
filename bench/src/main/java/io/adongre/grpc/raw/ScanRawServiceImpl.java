package io.adongre.grpc.raw;

import com.google.protobuf.ByteString;
import io.adongre.grpc.generated.ScanRawResponse;
import io.adongre.grpc.generated.ScanRawRow;
import io.adongre.grpc.generated.ScanRequest;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

/**
 * Created by adongre on 9/29/16.
 */
public class ScanRawServiceImpl extends io.adongre.grpc.generated.ScanServiceGrpc.ScanServiceImplBase {
  @Override
  public void rawScan(ScanRequest request, StreamObserver<ScanRawResponse> responseObserver) {
    io.adongre.grpc.generated.ScanRawResponse.Builder scanRawResponseBuilder = io.adongre.grpc.generated.ScanRawResponse.newBuilder();
    io.adongre.grpc.generated.ScanRawRow.Builder scanRowBuilder = io.adongre.grpc.generated.ScanRawRow.newBuilder();
    try {
      int numOfColumns = request.getNumOfColumns();
      int columnSize = request.getSizeOfEachColumn();
      byte[] data = new byte[numOfColumns * columnSize];

      final ServerCallStreamObserver<ScanRawResponse> scso =
          (ServerCallStreamObserver<ScanRawResponse>) responseObserver;

      Runnable drain = new Runnable() {
        long remaining = request.getNumOfRows();

        public void run() {
          if (remaining == 0L) return;
          for (; remaining > 0L && scso.isReady(); remaining--) {
            scanRowBuilder.setRow(ByteString.copyFrom(data));
            scanRawResponseBuilder.addRow(scanRowBuilder.build());
            scanRowBuilder.clear();
            scso.onNext(scanRawResponseBuilder.build());
            scanRawResponseBuilder.clear();
          }
          if (remaining == 0) {
            scso.onCompleted();
          }
        }
      };
      scso.setOnReadyHandler(drain);
      drain.run();

    } catch (Exception e) {
      System.out.println(" EXCEPTION EXCEPTION");
      e.printStackTrace();
    }
  }
}
