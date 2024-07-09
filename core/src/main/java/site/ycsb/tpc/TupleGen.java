package site.ycsb.tpc;

import java.io.IOException;

public class TupleGen {

  public static void openOutputFiles() throws IOException {
    assert true;
  }

  public static void closeOutputFiles() throws IOException {
    assert true;
  }

  public static void genWarehouse(int wId) throws IOException {
    assert true;
  }

  public static void genDistrict(int dId, int wId) throws IOException {
    assert true;
  }

  public static void genCustomer(int cId, int dId, int wId, String customerTime) throws IOException {
    assert true;
  }

  public static void genHistory(int cId, int dId, int wId) throws IOException {
    assert true;
  }

  public static void genNeworder(int oId, int dId, int wId) throws IOException {
    assert true;
  }

  public static void genOrder(int oId, int dId, int wId, int cId, int olCount, String orderTime) throws IOException {
    assert true;
  }

  public static void genOrderline(int oId, int dId, int wId, int olNumber, String orderTime) throws IOException {
    assert true;
  }

  public static void genItem(int iId) throws IOException {
    assert true;
  }

  public static void genStock(int iId, int wId) throws IOException {
    assert true;
  }

  public static void genNation(DataSource.Nation n) throws IOException {
    assert true;
  }

  public static void genSupplier(int suId) throws IOException {
    assert true;
  }

  public static void genRegion(int rId, String rName) throws IOException {
    assert true;
  }
}
