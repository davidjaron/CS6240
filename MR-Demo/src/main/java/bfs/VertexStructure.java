package bfs;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class VertexStructure implements Writable {
  private String vertice;
  private int level;
  private boolean used;
  private boolean active;
  private boolean structure;
  private List<String> adjacent;

  public VertexStructure(){
    this.adjacent = new ArrayList<>();
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeChars(vertice);
    dataOutput.writeBoolean(active);
    dataOutput.writeBoolean(structure);
    dataOutput.writeInt(adjacent.size());
    for (String temp : adjacent){
      dataOutput.writeChars(temp);
    }
    dataOutput.writeInt(level);
    dataOutput.writeBoolean(used);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    this.vertice = String.valueOf(dataInput.readChar());
    this.active = dataInput.readBoolean();
    this.structure = dataInput.readBoolean();
    int size = dataInput.readInt();
    List<String> adj = new ArrayList<>();
    for (int i = 0; i < size; i++){
      adj.add(String.valueOf(dataInput.readChar()));
    }
    this.adjacent = adj;
    this.level = dataInput.readInt();
    this.used = dataInput.readBoolean();
  }

  @Override
  public String toString() {
    StringBuilder output = new StringBuilder(vertice + "," + active + ",");
    output.append(this.adjacent.size()).append(",");
    for (String adj : adjacent){
      output.append(adj).append(",");
    }
    output.append(level);
    output.append(",").append(used);

    return output.toString();
  }

  public String getVertice() {
    return vertice;
  }

  public void setVertice(String vertice) {
    this.vertice = vertice;
  }

  public boolean isActive() {
    return active;
  }

  public void setActive(boolean active) {
    this.active = active;
  }

  public List<String> getAdjacent() {
    return adjacent;
  }

  public void setAdjacent(List<String> adjacent) {
    this.adjacent = adjacent;
  }

  public void addAdjacent(String adj){
    adjacent.add(adj);
  }

  public boolean isStructure() {
    return structure;
  }

  public void setStructure(boolean structure) {
    this.structure = structure;
  }

  public int getLevel() {
    return level;
  }

  public void setLevel(int level) {
    this.level = level;
  }

  public boolean isUsed() {
    return used;
  }

  public void setUsed(boolean used) {
    this.used = used;
  }
}
