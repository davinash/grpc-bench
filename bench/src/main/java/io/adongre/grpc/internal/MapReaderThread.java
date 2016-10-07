package io.adongre.grpc.internal;

import com.google.protobuf.ByteString;
import io.adongre.grpc.generated.ColumnValues;
import io.adongre.grpc.generated.ScanFormattedResponse;
import io.adongre.grpc.generated.ScanFormattedRow;
import io.adongre.grpc.generated.ScanRequest;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;

/**
 * Created by adongre on 10/7/16.
 */
public class MapReaderThread implements Runnable {

  private BlockingQueue<ScanFormattedResponse> Q;

  int batchSize = 0;
  int numOfColumns = 0;
  long numOfRows = 0L;
  int sizeOfEachColumn = 0;
  ScanFormattedResponse.Builder scanFormattedResponseBuilder = null;
  ScanFormattedRow.Builder scanFormattedRowBuilder = null;
  ColumnValues.Builder columnValueBuilder = null;
  byte[] data = null;


  public MapReaderThread(ScanRequest request, BlockingQueue<ScanFormattedResponse> q) {
    System.out.println("MapReaderThread.MapReaderThread");
    this.batchSize = request.getBatchSize();
    this.numOfColumns = request.getNumOfColumns();
    this.numOfRows = request.getNumOfRows();
    this.sizeOfEachColumn = request.getSizeOfEachColumn();
    this.scanFormattedResponseBuilder = ScanFormattedResponse.newBuilder();
    this.scanFormattedRowBuilder = ScanFormattedRow.newBuilder();
    this.columnValueBuilder = ColumnValues.newBuilder();
    this.data = new byte[this.numOfColumns];
    this.Q = q;

  }

  @Override
  public void run() {
    System.out.println("MapReaderThread.run");
    for (long l = 0L; l < this.numOfRows; l++) {
      for (int columnIdx = 0; columnIdx < numOfColumns; columnIdx++) {
        columnValueBuilder.setColumnName(
            ByteString.copyFrom(ByteBuffer.allocate(Long.BYTES).putLong(l).array()));
        columnValueBuilder.setColumnValue(
            ByteString.copyFrom(data));
        scanFormattedRowBuilder.addColumnValue(columnValueBuilder.build());
        columnValueBuilder.clear();
      }
      scanFormattedRowBuilder.setRowId(l);
      scanFormattedRowBuilder.setTimeStamp(System.nanoTime());

      scanFormattedResponseBuilder.addRow(scanFormattedRowBuilder.build());
      scanFormattedRowBuilder.clear();
      try {
        this.Q.put(scanFormattedResponseBuilder.build());
        System.out.println("MapReaderThread.Put Item on a Thread");
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}







