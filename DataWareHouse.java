import java.sql.*;

import com.manager.DBConManager;

public class DataWareHouse {

	public DataWareHouse() {
		
	}
	
	public void getData() {
		try {
//			 String datacubeSql = "select s.CITY,d.QUARTER,a.NAME, cast( sum(f.Sale) as DOUBLE) as Sale, round(sum( cast(f.TURNOVER as DOUBLE)), 3) as Turnover "+
//					"from FACTS f join SHOPDIMENSION s on (f.SHOPID = s.SHOPID) join ARTICLEDIMENSION a on (f.ARTICLEID = a.ARTICLEID)" +
//					 "join DATEDIMENSION d on (f.DATE = d.DATE) group by cube (s.CITY, d.QUARTER,a.NAME) order by s.CITY, d.QUARTER, a.NAME ";	
			 String datacubeSql = "select s.CITY,d.QUARTER, cast( sum(f.Sale) as DOUBLE) as Sale, a.NAME "+
						"from FACTS f join SHOPDIMENSION s on (f.SHOPID = s.SHOPID) join ARTICLEDIMENSION a on (f.ARTICLEID = a.ARTICLEID)" +
						 "join DATEDIMENSION d on (f.DATE = d.DATE) group by s.CITY, d.QUARTER,a.NAME order by s.CITY, d.QUARTER, a.NAME ";	
		
//			PreparedStatement ps = DBConManager.getInstance().getConnection().prepareStatement(datacubeSql);
//			ps.setString(1, "s."+shop);
//			ps.setString(2, "d."+datedimension);
//			ps.setString(3, "a."+article);
//			ps.setString(4, "s."+shop);
//			ps.setString(5, "d."+datedimension);
//			ps.setString(6, "a."+article);
//			ps.setString(7, "s."+shop);
//			ps.setString(8, "d."+datedimension);
//			ps.setString(9, "a."+article);
//			
			
			Statement stm = DBConManager.getInstance().getConnection().createStatement();
			ResultSet rs = stm.executeQuery(datacubeSql);
			int count = 0;
			System.out.println("------------------------------------------------------------------------------------------------------------------------------------------------");
			System.out.println("|  CITY              |             QUARTER             |               Sale                  |                   Name                    ");
			System.out.println("------------------------------------------------------------------------------------------------------------------------------------------------");
			try {
				while (rs.next()) {
					count++;
					System.out.println("|  " +rs.getString(1)+ "            |            "+ rs.getString(2) + "             |            " + rs.getDouble(3)+ "                   |            "+ rs.getString(4));
				}
				System.out.println("Count: "+count);
			
			} catch(SQLException e) {
				e.printStackTrace();
			}
			rs.close();
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}	
}