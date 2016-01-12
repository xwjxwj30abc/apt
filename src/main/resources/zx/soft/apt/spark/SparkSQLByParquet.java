package zx.soft.apt.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class SparkSQLByParquet {

	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setAppName("sparkSQLByParquet").setMaster("local");
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		SQLContext sqlContext = new SQLContext(sparkContext);
		String basicDir = "hdfs://archtorm:9000/user/xuwenjuan/";
		//way1:
		//DataFrame dataFrame = sqlContext.parquetFile(basicDir + "mapreduce/output_parquet/part-m-00000.snappy.parquet");
		//way2:
		DataFrame dataFrame = sqlContext.read().parquet(
				basicDir + "mapreduce/output_parquet/part-m-00000.snappy.parquet");
		dataFrame.printSchema();
		dataFrame.filter(dataFrame.col("favorite_number").gt(0)).show();
		sparkContext.close();
	}

}
