package site.ycsb.tpc;

public class NationData {
  public int id;
  public String name;
  public int regionId;

  public NationData(int id, String name, int regionId) {
    this.id = id;
    this.name = name;
    this.regionId = regionId;
  }
}
