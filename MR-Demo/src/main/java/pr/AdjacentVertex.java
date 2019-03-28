package pr;

import java.util.ArrayList;
import java.util.List;

public class AdjacentVertex {
  int vertex;
  int cost;

  public AdjacentVertex(int vertex, int cost) {
    this.vertex = vertex;
    this.cost = cost;
  }


  public int getVertex() {
    return vertex;
  }

  public void setVertex(int vertex) {
    this.vertex = vertex;
  }

  public int getCost() {
    return cost;
  }

  public void setCost(int cost) {
    this.cost = cost;
  }

  @Override
  public String toString() {
    if (this.cost == Integer.MAX_VALUE || this.cost < 1){
      return this.vertex + "," + "∞";
    }
    return this.vertex + "," + this.cost;
  }

  @Override
  public boolean equals(Object obj) {
    AdjacentVertex a = (AdjacentVertex) obj;

    if (a.vertex == this.vertex){
      return a.cost == this.cost;
    } else {
      return false;
    }
  }

  public static void main(String[] args){
    AdjacentVertex t = new AdjacentVertex(1, 3);
    AdjacentVertex t2 = new AdjacentVertex(1, 3);

    List<AdjacentVertex> l1 = new ArrayList<>();
    l1.add(t);
    List<AdjacentVertex> l2 = new ArrayList<>();
    l2.add(t2);




    System.out.println(l1.containsAll(l2));
  }
}
