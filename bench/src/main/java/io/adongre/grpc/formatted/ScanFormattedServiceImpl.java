package io.adongre.grpc.formatted;

import com.google.protobuf.ByteString;
import io.adongre.grpc.generated.*;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import java.nio.ByteBuffer;

/**
 * Created by adongre on 9/29/16.
 */
public class ScanFormattedServiceImpl extends io.adongre.grpc.generated.ScanServiceGrpc.ScanServiceImplBase {
  @Override
  public void formatedScan(ScanRequest request, StreamObserver<ScanFormattedResponse> responseObserver) {

    ScanFormattedResponse.Builder scanFormattedResponseBuilder = ScanFormattedResponse.newBuilder();
    ScanFormattedRow.Builder scanFormattedRowBuilder = ScanFormattedRow.newBuilder();
    ColumnValues.Builder columnValueBuilder = ColumnValues.newBuilder();

    try {
      int numOfColumns = request.getNumOfColumns();
      int columnSize = request.getSizeOfEachColumn();
      int batchSize = request.getBatchSize();
      byte[] data = new byte[columnSize];

      final ServerCallStreamObserver<ScanFormattedResponse> scso =
          (ServerCallStreamObserver<ScanFormattedResponse>) responseObserver;

      Runnable drain = new Runnable() {
        long remaining = request.getNumOfRows();

        public void run() {
          if (remaining == 0L) return;
          for (; remaining > 0L && scso.isReady(); remaining--) {

            for ( int columnIdx = 0; columnIdx < numOfColumns; columnIdx++) {
              columnValueBuilder.setColumnName(
                  ByteString.copyFrom(ByteBuffer.allocate(Long.BYTES).putLong(remaining).array()));
              columnValueBuilder.setColumnValue(
                  ByteString.copyFrom(data));
              scanFormattedRowBuilder.addColumnValue(columnValueBuilder.build());
              columnValueBuilder.clear();
            }
            scanFormattedRowBuilder.setRowId(remaining);
            scanFormattedRowBuilder.setTimeStamp(System.nanoTime());

            scanFormattedResponseBuilder.addRow(scanFormattedRowBuilder.build());
            scanFormattedRowBuilder.clear();
            if ( scanFormattedResponseBuilder.getRowList().size() % batchSize == 0 ) {
              scso.onNext(scanFormattedResponseBuilder.build());
              scanFormattedResponseBuilder.clear();
            }
          }

          if ( scanFormattedResponseBuilder.getRowList().size() % batchSize == 0 ) {
            scso.onNext(scanFormattedResponseBuilder.build());
            scanFormattedResponseBuilder.clear();
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
