package utility;
import models.Stock;

public class StockUtility {
	
	public static Stock minDateStock(Stock s1, Stock s2) {
		if(s1.getData().compareTo(s2.getData()) < 0)
			return s1;
		else return s2;
		
	}
	
	public static Stock maxDateStock(Stock s1, Stock s2) {
		if(s1.getData().compareTo(s2.getData()) > 0)
			return s1;
		else return s2;
		
	}

}
