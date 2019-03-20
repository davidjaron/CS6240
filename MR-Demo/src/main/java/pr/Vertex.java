package pr;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Vertex implements Writable {
  private int page;
  private double pageRank;
  private List<Integer> adjacent;
  private boolean isVertex;

  public Vertex(){
    this.adjacent = new ArrayList<>();
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    int size = adjacent.size();
    dataOutput.writeInt(size);
    for (int j : adjacent){
      dataOutput.writeInt(j);
    }
    dataOutput.writeDouble(pageRank);
    dataOutput.writeBoolean(isVertex);
    dataOutput.writeInt(page);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    List<Integer> l = new ArrayList<>();
    int size = dataInput.readInt();
    for (int i = 0; i < size; i++){
      l.add(dataInput.readInt());
    }
    this.pageRank = dataInput.readDouble();
    this.adjacent = l;
    this.isVertex = dataInput.readBoolean();
    this.page = dataInput.readInt();
  }

  @Override
  public String toString() {
    StringBuilder output = new StringBuilder();
    output.append(pageRank).append(" ");
    int size = adjacent.size();
    output.append(size).append(" ");
    for (int j : adjacent){
      output.append(j).append(" ");
    }
    return output.toString();
  }

  public double getPageRank() {
    return pageRank;
  }

  public void setPageRank(double pageRank) {
    this.pageRank = pageRank;
  }

  public List<Integer> getAdjacent() {
    return adjacent;
  }

  public void setAdjacent(List<Integer> adjacent) {
    this.adjacent = adjacent;
  }

  public void addAdjacent(int adj){
    this.adjacent.add(adj);
  }

  public boolean isVertex() {
    return isVertex;
  }

  public void setVertex(boolean vertex) {
    isVertex = vertex;
  }

  public int getPage() {
    return page;
  }

  public void setPage(int page) {
    this.page = page;
  }
}
