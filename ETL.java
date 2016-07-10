
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.manager.DBConManager;

public class ETL {
	
	public ETL() {
	}

	public void loadShopDimension() {
		System.out.println("-----Loading ShopDimension------");

		String selectQuery = "SELECT shop.SHOPID as SHOPID, shop.NAME as NAME, stadt.NAME as CITY, region.NAME as REGION, land.NAME as COUNTRY FROM DB2INST1.SHOPID shop "+
							"join DB2INST1.STADTID stadt on shop.STADTID = stadt.STADTID join DB2INST1.REGIONID region on stadt.REGIONID = region.REGIONID "+
									"join DB2INST1.LANDID land on region.LANDID = land.LANDID";
		String insertQuery = "INSERT INTO SHOPDIMENSION (SHOPID, NAME, CITY, REGION, COUNTRY) values (?,?,?,?,?)";
		try {
			int shopID;
			String shopName;
			String townName;
			String regionName;
			String countryName;
			PreparedStatement ps;
			
			Statement stm = DBConManager.getInstance().getConnection().createStatement();
			ResultSet rs = stm.executeQuery(selectQuery);
			//READ from DBINST1
			while(rs.next()) {
				shopID = rs.getInt("SHOPID");
				shopName = rs.getString("NAME");
				townName = rs.getString("CITY");
				regionName = rs.getString("REGION");
				countryName = rs.getString("COUNTRY");
				
				//WRITE into Dimension Table
				ps = DBConManager.getInstance().getConnection().prepareStatement(insertQuery);
				ps.setInt(1, shopID);
				ps.setString(2, shopName);
				ps.setString(3, townName);
				ps.setString(4, regionName);
				ps.setString(5, countryName);
				ps.execute();
				ps.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		System.out.println("-----ShopDimension Loaded------");

	}

	public void loadArticleDimension() {
		System.out.println("-----Loading ArticleDimension------");

		String selectQuery = "SELECT article.ARTICLEID as ARTICLEID, article.NAME as NAME, group.NAME as PRODUCTGROUP, prod.NAME as PRODUCTFAMILY, category.NAME as PRODUCTCATEGORY FROM DB2INST1.ARTICLEID article "+
							"join DB2INST1.PRODUCTGROUPID group on article.PRODUCTGROUPID = group.PRODUCTGROUPID join DB2INST1.PRODUCTFAMILYID prod on group.PRODUCTFAMILYID = prod.PRODUCTFAMILYID join DB2INST1.PRODUCTCATEGORYID category on category.PRODUCTCATEGORYID = prod.PRODUCTCATEGORYID";
		String insertQuery = "INSERT INTO ARTICLEDIMENSION (ARTICLEID, NAME, PRODUCTGROUP, PRODUCTFAMILY, PRODUCTCATEGORY) values (?,?,?,?,?)";
		
		try {
			int articleID;
			String articleName;
			String productGroup;
			String productFamily;
			String productCategory;
			PreparedStatement ps;
			
			Statement stm = DBConManager.getInstance().getConnection().createStatement();
			ResultSet rs = stm.executeQuery(selectQuery);
			
			while(rs.next()) {
				//READ from DBINST1
				articleID = rs.getInt("ARTICLEID");
				articleName = rs.getString("NAME");
				productGroup = rs.getString("PRODUCTGROUP");
				productFamily = rs.getString("PRODUCTFAMILY");
				productCategory = rs.getString("PRODUCTCATEGORY");
				
				//WRITE to ARTICLEDIMENSION
				ps = DBConManager.getInstance().getConnection().prepareStatement(insertQuery);
				ps.setInt(1, articleID);
				ps.setString(2, articleName);
				ps.setString(3, productGroup);
				ps.setString(4, productFamily);
				ps.setString(5, productCategory);
				ps.execute();
				ps.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		System.out.println("-----ArticleDimension Loaded------");

	}
	
	/**
	 * dd.mm.yyyy
	 */
	public void loadDateDimension() {
		System.out.println("-----Loading DateDimension------");

		int j = 0;
		int year = 2015;
		String month = "";
		String insertQuery = "INSERT INTO DATEDIMENSION (DATE, MONTH, QUARTER, YEAR) values (?,?,?,?)";
		
		for (int i=1;i<=5;i++){
			
			if(i<10){
				month = "0" + i;
			}
			else{
				month = "" +i;
			}
			int q = ((i-1)/3)+1;
			String yearAsString = ""+year;
			String quarter = "Q"+q+", "+yearAsString;
			try {
				PreparedStatement ps = DBConManager.getInstance().getConnection().prepareStatement(insertQuery);
				if(i==1 || i == 3 || i==5){
					j = 31;
				}
				else if(i==2){
					j = 28;
				}
				else{
					j=30;
				}
				for (int day = 1; day<=j; day++) {
					String dayAsString = (day < 10) ? "0"+day : ""+day;
					String date = dayAsString+"."+month+"."+yearAsString;
					//System.out.println(date + " " + month + " " + quarter + " " +yearAsString);

					ps.setString(1, date);
					ps.setString(2, month);
					ps.setString(3, quarter);
					ps.setString(4, yearAsString);
					ps.addBatch();
				}
				
				ps.executeBatch();
				DBConManager.getInstance().getConnection().commit();
				ps.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		System.out.println("-----DateDimension Loaded------");

	}

	public void loadFacts() {
		System.out.println("-----Loading Facts------");
		//ArrayList<ArrayList> shops = new ArrayList<ArrayList>();
		//ArrayList<ArrayList> articles = new ArrayList<ArrayList>();
		HashMap<String, Integer> shops = new HashMap<String, Integer>();
		HashMap<String, Integer> articles = new HashMap<String, Integer>();

		try {
			String selectShops = "SELECT SHOPID, NAME FROM SHOPDIMENSION";
			String selectArticles = "SELECT ARTICLEID, NAME FROM ARTICLEDIMENSION";
			
			Statement stm = DBConManager.getInstance().getConnection().createStatement();
			ResultSet rs = stm.executeQuery(selectShops);
			while(rs.next()) {
				shops.put(rs.getString("NAME"), rs.getInt("SHOPID"));				
			}
			
			Statement stm1 = DBConManager.getInstance().getConnection().createStatement();
			rs = stm1.executeQuery(selectArticles);
			while(rs.next()) {
				articles.put(rs.getString("NAME"), rs.getInt("ARTICLEID"));				

			}
			
		} catch(SQLException e) {
			e.printStackTrace();
		}
		
		int index = 0;
		String line = null;
		try {
			InputStreamReader ins = new InputStreamReader(new FileInputStream("sales.csv"),"Cp1252");
			//FileReader fr = new FileReader("sales.csv");
			BufferedReader br = new BufferedReader(ins);
			String[] slice;
			String dateFile;
			String shopFile;
			String articleFile;
			int soldFile;
			String[] turnoverFile;
			float turnover;
			String insertQuery = "INSERT INTO FACTS (DATE,SHOPID,ARTICLEID,SALE,TURNOVER) VALUES (?,?,?,?,?)";
			DBConManager.getInstance().getConnection().setAutoCommit(false);
			PreparedStatement ps = DBConManager.getInstance().getConnection().prepareStatement(insertQuery);
			
			br.readLine(); 
			line = br.readLine();
			

			while(line != null) {
				slice = line.split(";");
				dateFile = slice[0];
				shopFile = slice[1];
				//System.out.println(slice[0] + " " + slice[1]);
				//System.out.println(slice[2]);
				articleFile = slice[2];
				soldFile = Integer.parseInt(slice[3]);
				turnoverFile = slice[4].split("\\,");
				//System.out.println(turnoverFile[0] + " " + turnoverFile[1]);
				turnover = (Float.parseFloat(turnoverFile[0])) + (Float.parseFloat(turnoverFile[1])/100);
				if(dateFile != null && shopFile != null && articleFile != null && soldFile != 0){
					
					if ((shops.get(shopFile)) != null && (articles.get(articleFile) != null)) {
						

						ps.setString(1, dateFile); 
						ps.setInt(2, shops.get(shopFile)); //shopID
						ps.setInt(3, articles.get(articleFile)); //articleID
						ps.setInt(4, soldFile);
						ps.setFloat(5, turnover);
						ps.addBatch();
						
						index++;
						if (index%2000 == 0) {
							try{ 
								System.out.println("BatchNO: "+ index);
								ps.executeBatch();
							}catch(Exception e){
								e.printStackTrace();
							}
							DBConManager.getInstance().getConnection().commit();
							ps.clearBatch();
						}
					}

					
					line = br.readLine();
				}
				
				ps.executeBatch();
				DBConManager.getInstance().getConnection().commit();
				ps.clearBatch();
			}
			
			ps.close();
			DBConManager.getInstance().getConnection().setAutoCommit(true);
			
			br.close();
		} catch(FileNotFoundException e) {
			System.err.println("File not found.");
		} catch(IOException e) {
			e.printStackTrace();
		} 
		catch(SQLException e) {
			e.printStackTrace();
		}
		
		System.out.println("-----Facts Loaded------");

	}
	public void deleteData() {
		try {
			PreparedStatement deleteQuery;
			System.out.println("------- Deleting Facts ------- ");

			deleteQuery = DBConManager.getInstance().getConnection().prepareStatement("DELETE FROM FACTS");
			deleteQuery.executeUpdate();
			System.out.println("------- Deleting DateDimension ------- ");

			deleteQuery = DBConManager.getInstance().getConnection().prepareStatement("DELETE FROM DATEDIMENSION");
			deleteQuery.executeUpdate();
			System.out.println("------- Deleting ShopDimension ------- ");

			deleteQuery = DBConManager.getInstance().getConnection().prepareStatement("DELETE FROM SHOPDIMENSION");
			deleteQuery.executeUpdate();
			System.out.println("------- Deleting ArticleDimension ------- ");

			deleteQuery = DBConManager.getInstance().getConnection().prepareStatement("DELETE FROM ARTICLEDIMENSION");
			deleteQuery.executeUpdate();
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	


}