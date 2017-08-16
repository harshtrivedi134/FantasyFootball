package fantasy_football;
import java.util.*;
import java.io.*;


public class readPreds {
	public static void main(String[] args){
		try {
			//read salaries
			BufferedReader read = new BufferedReader(new FileReader("salarylist.csv"));
			HashMap<String, Integer> salaries = new HashMap<String, Integer>();
			String in = read.readLine();
			while((in = read.readLine()) != null){
				//System.out.println(in);
				String[] bits = in.split(",");
				String tmpnm = bits[2]+" "+bits[4];
				tmpnm = tmpnm.toLowerCase();
				String sal = bits[7];
				int salary = Integer.parseInt(sal);
				System.out.println(tmpnm+salary);
				salaries.put(tmpnm, salary);
			}
			//load predicted points
			ArrayList<PriorityQueue<Player>> allplaying = new ArrayList<PriorityQueue<Player>>();
			BufferedReader br = new BufferedReader(new FileReader("scores.txt"));
			String line;
			FileWriter forR = new FileWriter("Rinput.csv");
			while((line = br.readLine()) != null){
				PriorityQueue<Player> thispos = new PriorityQueue<Player>();
				String player;
				double score = 0;
				System.out.println(line);
				String pos;
				String[] bits = line.split("\\{");
				String[] bits1 = bits[0].split("'");
				pos = bits1[1];
				pos = pos.toUpperCase();
				System.out.println(pos);
				Position p = Position.valueOf(pos);

				String[] players = bits[1].split(",");
				for(int i = 0; i<players.length-1; i++){
					//System.out.println(players[i]);
					String[] bits2 = players[i].split("'");
					player = bits2[1];
					//System.out.println(player);
					try{
						String[] toparse = bits2[2].split(": ");


						if(i == players.length-2){
							String[] parse2 = toparse[1].split("\\}");
							score = Double.parseDouble(parse2[0]);
							//System.out.println(score);
						}else{
							score = Double.parseDouble(toparse[1]);
							//System.out.println(score);
						}
					}catch(ArrayIndexOutOfBoundsException e){
						System.out.println(players[i]);

					}
					try{
						Player playr = new Player(player, salaries.get(player.toLowerCase()), score, p);
						thispos.add(playr);
						System.out.println(playr);
						forR.write(player+","+score+","+p+"\n");
					}catch(NullPointerException e){
						//System.out.println(player+" not playing today");
					}
				}
				allplaying.add(thispos);
			}
			FileWriter fw = new FileWriter("playerlist.csv");
			for(int i = 0; i<allplaying.size(); i++){
				while(!allplaying.get(i).isEmpty()){
					fw.write(allplaying.get(i).poll().toString()+"\n");
				}
			}
			
			int sum = 0;
			Player[] team = new Player[9];
			team[0] = allplaying.get(0).poll(); //best kicker
			team[1] = allplaying.get(1).poll(); //best quarterback
			team[2] = allplaying.get(2).poll(); //receiver one
			team[3] = allplaying.get(2).poll(); //receiver two
			team[4] = allplaying.get(2).poll(); // receiver three
			team[5] = allplaying.get(3).poll(); //rb one
			team[6] = allplaying.get(3).poll(); //rb two
			team[7] = allplaying.get(4).poll(); //tight end
			team[8] = allplaying.get(5).poll(); //def
			for(int j = 0; j<team.length; j++){
				sum += team[j].getSalary();
			}
			System.out.println(sum);
			int iteration = 0;
			while(sum > 60000){
				iteration++;
				
				double[] pdifs = new double[9];
				//least points lost
				pdifs[0] = team[0].getPoints() - allplaying.get(0).peek().getPoints();
				pdifs[1] = team[1].getPoints() - allplaying.get(1).peek().getPoints();
				pdifs[2] = team[2].getPoints() - allplaying.get(2).peek().getPoints();
				pdifs[3] = team[3].getPoints() - allplaying.get(2).peek().getPoints();
				pdifs[4] = team[4].getPoints() - allplaying.get(2).peek().getPoints();
				pdifs[5] = team[5].getPoints() - allplaying.get(3).peek().getPoints();
				pdifs[6] = team[6].getPoints() - allplaying.get(3).peek().getPoints();
				pdifs[7] = team[7].getPoints() - allplaying.get(4).peek().getPoints();
				pdifs[8] = team[8].getPoints() - allplaying.get(5).peek().getPoints();
				
				int[] sdifs = new int[9];
				//most money gained
				sdifs[0] = team[0].getSalary() - allplaying.get(0).peek().getSalary();
				sdifs[1] = team[1].getSalary() - allplaying.get(1).peek().getSalary();
				sdifs[2] = team[2].getSalary() - allplaying.get(2).peek().getSalary();
				sdifs[3] = team[3].getSalary() - allplaying.get(2).peek().getSalary();
				sdifs[4] = team[4].getSalary() - allplaying.get(2).peek().getSalary();
				sdifs[5] = team[5].getSalary() - allplaying.get(3).peek().getSalary();
				sdifs[6] = team[6].getSalary() - allplaying.get(3).peek().getSalary();
				sdifs[7] = team[7].getSalary() - allplaying.get(4).peek().getSalary();
				sdifs[8] = team[8].getSalary() - allplaying.get(5).peek().getSalary();
				
				double min = 10000;
				int minind = 10;
				for(int j=0; j<pdifs.length; j++){
					if(pdifs[j] < min){
						min = pdifs[j];
						minind = j;
					}
				}
				
				int max = -10000;
				int maxind = 10;
				for(int j = 0; j<sdifs.length; j++){
					if(sdifs[j] > max){
						max = sdifs[j];
						maxind = j;
					}
				}
				if(iteration %2 == 1){
					System.out.println("Iteration: "+iteration+" Replacing: "+team[minind]);

					if(minind == 0){
						team[0] = allplaying.get(0).poll(); //best kicker
					}else if(minind == 1){
						team[1] = allplaying.get(1).poll(); //best quarterback

					}else if(minind == 2){
						team[2] = allplaying.get(2).poll(); //receiver one

					}else if(minind == 3){
						team[3] = allplaying.get(2).poll(); //receiver two

					}else if(minind == 4){
						team[4] = allplaying.get(2).poll(); // receiver three

					}else if(minind == 5){
						team[5] = allplaying.get(3).poll(); //rb one

					}else if(minind == 6){
						team[6] = allplaying.get(3).poll(); //rb two

					}else if(minind == 7){
						team[7] = allplaying.get(4).poll(); //tight end

					}else if(minind == 8){
						team[8] = allplaying.get(5).poll(); //def

					}
				}
			
				if(iteration%2 == 0){
					System.out.println("Iteration: "+iteration+" Replacing: "+team[maxind]);

					if(maxind == 0){
						team[0] = allplaying.get(0).poll(); //best kicker
					}else if(maxind == 1){
						team[1] = allplaying.get(1).poll(); //best quarterback

					}else if(maxind == 2){
						team[2] = allplaying.get(2).poll(); //receiver one

					}else if(maxind == 3){
						team[3] = allplaying.get(2).poll(); //receiver two

					}else if(maxind == 4){
						team[4] = allplaying.get(2).poll(); // receiver three

					}else if(maxind == 5){
						team[5] = allplaying.get(3).poll(); //rb one

					}else if(maxind == 6){
						team[6] = allplaying.get(3).poll(); //rb two

					}else if(maxind == 7){
						team[7] = allplaying.get(4).poll(); //tight end

					}else if(maxind == 8){
						team[8] = allplaying.get(5).poll(); //def

					}
				}
			
				
				sum = 0;
				for(int j = 0; j<team.length; j++){
					sum += team[j].getSalary();
				}
			}
			for(int j = 0; j<team.length; j++){
				System.out.println(team[j]);
			}

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
