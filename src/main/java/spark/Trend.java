package spark;
import java.util.Comparator;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import avro.shaded.com.google.common.collect.Sets;
import models.AuxStockInfo;
import models.Stock;
import scala.Tuple2;
import utility.Parser;
import utility.StockUtility;

public class Trend {

	private String file1;
	private String file2;
	
	public Trend(String file1, String file2) {
		this.file1 = file1;
		this.file2 = file2;
	}

	public JavaRDD<Stock> buildStocks() {

		SparkConf conf = new SparkConf().setAppName("Trend");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<Stock> raw1= sc.textFile(file1).map(line -> Parser.parseFile1(line))
				.filter(stock -> stock!=null)
				.filter(stock -> stock.getData().getYear()+1900>=2008 && stock.getData().getYear()+1900<=2018);
		JavaRDD<AuxStockInfo> raw2= sc.textFile(file2).map(line -> Parser.parseFile2(line));
		Map<String, AuxStockInfo> ticker2info= raw2.mapToPair(stock -> new Tuple2<String, AuxStockInfo>(stock.getTicker(), stock))
				.collectAsMap();
		JavaRDD<Stock> stocks = raw1.map(stock -> Parser.completeStockInfo(stock,ticker2info));
		return stocks;
	}
	
	public JavaPairRDD<Tuple2<String, Integer>, String> run() {
		JavaRDD<Stock> stocks = buildStocks();

		JavaPairRDD<Tuple2<String,Integer>,Long> meanVolume = meanVolume(stocks);
		JavaPairRDD<Tuple2<String, Integer>, Double> meanDiff = meanDifference(stocks);
		JavaPairRDD<Tuple2<String, Integer>, Double> meanQuotation = meanQuotation(stocks);
		
		JavaPairRDD<Tuple2<String, Integer>, String> output =  meanVolume
				.join(meanDiff)
				.join(meanQuotation)
				.mapToPair(tup -> new Tuple2<>(tup._1(),
						"Mean Volume: " + tup._2()._1()._1() 
						+", Mean Difference: " +  tup._2()._1()._2()
						+"%, Mean Quotation: " + tup._2()._2()))
				.sortByKey(new Comparator<Tuple2<String, Integer>>() {
						@Override
						public int compare(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) {
							int i = t1._1().compareTo(t2._1());
							if(i == 0)
								return t1._2().compareTo(t2._2());
							return i;
						}
					});

		stampa(output);
		return output;
	
	}

	private void stampa(JavaPairRDD<Tuple2<String, Integer>, String> output) {
		Map<Tuple2<String,Integer>,String> out = output.collectAsMap();
		System.out.println("");
		System.out.println("");
		System.out.println("");
		System.out.println("");
		System.out.println("");
		for(Tuple2<String,Integer> s : out.keySet()) 
			System.out.println(s._1() + " " + s._2() + "\t" + out.get(s));
		System.out.println("");
		System.out.println("");
		System.out.println("");
		System.out.println("");
		System.out.println("");
	}

	private JavaPairRDD<Tuple2<String, Integer>, Double> meanQuotation(JavaRDD<Stock> stocks) {
		JavaPairRDD<Tuple2<String,Integer>,Stock> st = stocks.mapToPair(stock -> new Tuple2<>(
				new Tuple2<>(stock.getSector(),stock.getData().getYear()+1900), stock));
		
		JavaPairRDD<Tuple2<String,Integer>,Double> closingPricesSum = stocks.mapToPair(stock -> new Tuple2<>(
				new Tuple2<>(stock.getSector(),stock.getData().getYear()+1900), stock.getClose()))
				.reduceByKey((s1,s2) -> s1+ s2);
		
		JavaPairRDD<Tuple2<String,Integer>,Integer> stocksCounter = st.mapToPair(tup -> 
				new Tuple2<>(tup._1(), 1)).reduceByKey((n1,n2) -> n1+n2);
		
		JavaPairRDD<Tuple2<String,Integer>,Double> meanQuotation = closingPricesSum.join(stocksCounter)
				.mapToPair(tup -> new Tuple2<>(tup._1(), tup._2()._1()/tup._2()._2()));
		return meanQuotation;
	}

	private JavaPairRDD<Tuple2<String, Integer>, Double> meanDifference(JavaRDD<Stock> stocks) {
		
		JavaPairRDD<Tuple2<String,Integer>,Stock> st = stocks.mapToPair(stock -> new Tuple2<>(
				new Tuple2<>(stock.getSector(),stock.getData().getYear()+1900), stock));
		
		JavaPairRDD<Tuple2<String,Integer>,Double> sectorYear2Minstocks = st.reduceByKey((s1,s2) -> StockUtility.minDateStock(s1, s2))
				.mapToPair(tup -> new Tuple2(tup._1(), tup._2().getClose()));
		
		JavaPairRDD<Tuple2<String,Integer>,Double> sectorYear2Maxstocks = st.reduceByKey((s1,s2) -> StockUtility.maxDateStock(s1, s2))
				.mapToPair(tup -> new Tuple2(tup._1(), tup._2().getClose()));
		
		JavaPairRDD<Tuple2<String,Integer>,Double> sectorYear2meanDiff = sectorYear2Minstocks.join(sectorYear2Maxstocks)
				.mapToPair(tup -> new Tuple2(tup._1(),((tup._2()._2() - tup._2()._1())/tup._2()._1())*100));
		return sectorYear2meanDiff;
	
	}

	private JavaPairRDD<Tuple2<String,Integer>,Long>  meanVolume(JavaRDD<Stock> stocks) {
		JavaPairRDD<Tuple2<String,Integer>,Integer> sector2numAziende= 
				stocks.mapToPair(stock -> new Tuple2<>(	new Tuple2<>(stock.getSector(),stock.getData().getYear()+1900), stock.getAzienda()))
				.groupByKey()
				.mapToPair(tup -> new Tuple2<>(tup._1(), Sets.newHashSet(tup._2()).size()));
		

		JavaPairRDD<Tuple2<String,Integer>,Long> sector2totalVol = stocks.mapToPair(stock -> new Tuple2<>(
				new Tuple2<>(stock.getSector(),stock.getData().getYear()+1900), stock.getVolume()))
				.reduceByKey((s1,s2) -> s1+ s2);
		
		JavaPairRDD<Tuple2<String,Integer>,Long> sector2meanVolume = sector2totalVol.join(sector2numAziende)
				.mapToPair(tup -> new Tuple2<>(tup._1(),tup._2()._1()/tup._2()._2()));
		
		return sector2meanVolume;
	}
	
}
