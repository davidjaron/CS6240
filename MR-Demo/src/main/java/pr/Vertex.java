package pr;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Vertex implements Writable {
  private int page;
  private List<AdjacentVertex> adjacent;

  public Vertex(){
    this.adjacent = new ArrayList<>();
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeInt(this.page);
    int size = adjacent.size();
    dataOutput.writeInt(size);
    for (AdjacentVertex j : adjacent){
      dataOutput.writeInt(j.vertex);
      dataOutput.writeInt(j.getCost());
    }
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    this.page = dataInput.readInt();
    List<AdjacentVertex> l = new ArrayList<>();
    int size = dataInput.readInt();
    for (int i = 0; i < size; i++){
      int vertex = dataInput.readInt();
      int cost = dataInput.readInt();
      l.add(new AdjacentVertex(vertex, cost));
    }
    this.adjacent = l;
  }

  @Override
  public String toString() {
    StringBuilder output = new StringBuilder();
    int size = adjacent.size();
    output.append(size).append(" ");
    for (AdjacentVertex j : adjacent){
      output.append(j).append("|");
    }
    return output.toString();
  }

  public List<AdjacentVertex> getAdjacent() {
    return adjacent;
  }

  public void setAdjacent(List<AdjacentVertex> adjacent) {
    this.adjacent = adjacent;
  }

  public void addAdjacent(AdjacentVertex adj){
    this.adjacent.add(adj);
  }

  public int getPage() {
    return page;
  }

  public void setPage(int page) {
    this.page = page;
  }
}
