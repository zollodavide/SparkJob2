package models;
import java.util.Date;

import scala.Serializable;

public class Stock implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1345608432176121025L;
	private String ticker;
	private String sector;
	private String azienda;
	private Double close;
	private Double open;
	private Long volume;
	private Date data;
	
	public Stock() {
		
	}

	public String getAzienda() {
		return azienda;
	}

	public void setAzienda(String azienda) {
		this.azienda = azienda;
	}

	public String getTicker() {
		return ticker;
	}

	public void setTicker(String ticker) {
		this.ticker = ticker;
	}

	public String getSector() {
		return sector;
	}

	public void setSector(String sector) {
		this.sector = sector;
	}

	public Double getClose() {
		return close;
	}

	public void setClose(Double close) {
		this.close = close;
	}

	public Double getOpen() {
		return open;
	}

	public void setOpen(Double open) {
		this.open = open;
	}

	public Long getVolume() {
		return volume;
	}

	public void setVolume(Long volume) {
		this.volume = volume;
	}

	public Date getData() {
		return data;
	}

	public void setData(Date data) {
		this.data = data;
	}

	@Override
	public String toString() {
		return "Stock [ticker=" + ticker + ", sector=" + sector + ", azienda=" + azienda + ", close=" + close
				+ ", open=" + open + ", volume=" + volume + ", data=" + data + "]";
	}
	
	
	

}
