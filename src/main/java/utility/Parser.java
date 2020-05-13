package utility;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import constants.HistoricalStockPricesConstants;
import constants.HistoricalStocksConstants;
import models.AuxStockInfo;
import models.Stock;

public class Parser {

	public static Stock parseFile1(String line) throws ParseException {
		String[] parts = line.split(",");
		Stock out = new Stock();

		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		Date data;
		try {
			out.setTicker(parts[HistoricalStockPricesConstants.TICKER]);
			out.setClose(Double.parseDouble(parts[HistoricalStockPricesConstants.CLOSE]));			
			out.setOpen(Double.parseDouble(parts[HistoricalStockPricesConstants.OPEN]));
			out.setVolume(Long.parseLong(parts[HistoricalStockPricesConstants.VOLUME]));
			data = format.parse(parts[HistoricalStockPricesConstants.DATE]);
			out.setData(data);
		}catch(Exception e) {
			return null;
		}
		return out;
	}

	public static AuxStockInfo parseFile2(String line) {
		String[] parts = line.split(",");
		AuxStockInfo out = new AuxStockInfo();
		out.setTicker(parts[HistoricalStocksConstants.TICKER]);
		out.setSector(parts[HistoricalStocksConstants.SECTOR]);
		out.setAzienda(parts[HistoricalStocksConstants.NAME]);
		return out;
	}
	
	public static Stock completeStockInfo(Stock s, Map<String,AuxStockInfo> ticker2info) {
		AuxStockInfo info = ticker2info.get(s.getTicker());
		s.setSector(info.getSector());
		s.setAzienda(info.getAzienda());

		return s;
	}

}
