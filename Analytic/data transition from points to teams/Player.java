package fantasy_football;

public class Player implements Comparable<Player> {

	private String name;
	private int salary;
	private double points;
	private Position p;
	
	public Player(String name, int salary, double points, Position p){
		this.name = name;
		this.salary = salary;
		this.points = points;
		this.p = p;
	}
	
	public int compareTo(Player o){
		double dif =  o.points - this.points; //this comes first if more points;
		dif *= 10000;
		return((int)dif);
	}
	
	public String toString(){
		return this.p+","+this.name+","+this.salary+","+this.points;
	}
	
	public double getPoints(){
		return this.points;
	}
	
	public int getSalary(){
		return this.salary;
	}
}
