
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Main {

public static void main(String[] args) {
		
		try {
			boolean invalid = true;
			InputStreamReader ir = new InputStreamReader(System.in);
			BufferedReader br = new BufferedReader(ir);
			while(invalid){
				System.out.println("--------------------------------");
				System.out.println("Press L to Load or A to Analyse.");
				System.out.println("--------------------------------");

				String input = br.readLine();
				
				if (input.equalsIgnoreCase("L")) {
					System.out.println("--------------------------------");
					System.out.println("-----Preparing to Load Data-----");
					System.out.println("--------------------------------");

					ETL etl = new ETL();
					System.out.println("--------------------------------");
					System.out.println("------- Deleting old Data ------- ");
					System.out.println("--------------------------------");

					etl.deleteData();
					System.out.println("--------------------------------");
					System.out.println("-------Loading Fresh Data--------");
					System.out.println("--------------------------------");

					etl.loadShopDimension();
					etl.loadDateDimension();
					etl.loadArticleDimension();
					etl.loadFacts();
					System.out.println("--------------------------------");
					System.out.println("----------Data Loaded-----------");
					System.out.println("--------------------------------");


					invalid = false;
				} else if(input.equalsIgnoreCase("A")){
					System.out.println("---------------------------------");
					System.out.println("----Preparing to Analyse Data----");
					System.out.println("---------------------------------");


					DataWareHouse dwh = new DataWareHouse();
					dwh.getData();
					invalid = false;
				}
				else{
					System.out.println("--------------------------------");
					System.out.println("---------Invalid choice---------");
					System.out.println("--------------------------------");
				}
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		


	}

}