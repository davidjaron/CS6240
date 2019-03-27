package pr;

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
}
