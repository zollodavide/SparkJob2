

import spark.Trend;

public class Job2 {

	public static void main(String[] args) {

		Trend tr = new Trend(args[0], args[1]);
		tr.run();
	}

}
