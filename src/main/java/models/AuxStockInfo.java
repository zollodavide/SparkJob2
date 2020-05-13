package models;
import scala.Serializable;

public class AuxStockInfo implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -1950158206717939007L;
	private String ticker;
	private String sector; 
	private String azienda;
	
	public AuxStockInfo() {
		
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
	public String getAzienda() {
		return azienda;
	}
	public void setAzienda(String azienda) {
		this.azienda = azienda;
	}
	
	

}
