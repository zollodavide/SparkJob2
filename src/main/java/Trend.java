import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

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
				.filter(stock -> stock.getData().getYear()+1900>=2008);
		JavaRDD<AuxStockInfo> raw2= sc.textFile(file2).map(line -> Parser.parseFile2(line));
		Map<String, AuxStockInfo> ticker2info= raw2.mapToPair(stock -> new Tuple2<String, AuxStockInfo>(stock.getTicker(), stock))
				.collectAsMap();
		System.out.println();
		System.out.println(ticker2info.size());
		JavaRDD<Stock> stocks = raw1.map(stock -> Parser.completeStockInfo(stock,ticker2info));
		return stocks;
	}
	
	public JavaRDD<Stock> volumeAnnuale() {
		JavaRDD<Stock> stocks = buildStocks();
//		JavaPairRDD<String,Iterable<Stock>> sectors = stocks.mapToPair(stock -> new Tuple2<>(stock.getSector(), stock))
//				.groupByKey();
//		
//		for(int anno =2008; anno<=2018; anno++) {
//			
//			sectors.values().fil
//			JavaPairRDD<String,Integer> stockbyAnno = sectors.map(tuple -> tuple._2().);
//			
//		}
	
		//MANCA DIVIDERE PER IL NUMERO DI AZIENDE DEL SETTORE
		//ANNO NON PUÒ ESSERRE PARAMETRO DELLA FUNZIONE LAMBDA
		
		JavaPairRDD<String,Stock> sectors = stocks.mapToPair(stock -> new Tuple2<>(stock.getSector(), stock));
		for(int anno =2008; anno<=2018; anno++) {

			JavaPairRDD<String, Integer> s = sectors.filter(tup -> (tup._2().getData().getYear()+1900) == anno)
					.mapToPair(tup -> new Tuple2<>(tup._1(),tup._2().getVolume())).reduceByKey((s1,s2) -> s1+s2);
			
			Map<String, Integer> sd = s.collectAsMap();
			for(String sect : sd.keySet()) {
				System.out.println(sect + " " + anno + ": " + sd.get(sect));
				
			}
		}
	
		
		
	
		
	
	
	}
	
}
