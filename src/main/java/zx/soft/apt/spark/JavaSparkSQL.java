package zx.soft.apt.spark;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class JavaSparkSQL {

	public static class Person implements Serializable {
		private String name;
		private int age;

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public int getAge() {
			return age;
		}

		public void setAge(int age) {
			this.age = age;
		}
	}

	public static void main(String[] args) throws Exception {
		SparkConf sparkConf = new SparkConf().setAppName("JavaSparkSQL").setMaster("local");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		SQLContext sqlContext = new SQLContext(ctx);
		String basicDir = "hdfs://archtorm:9000/user/xuwenjuan/";
		System.out.println("=== Data source: RDD ===");
		JavaRDD<Person> people = ctx.textFile(basicDir + "input/people.txt").map(new Function<String, Person>() {
			@Override
			public Person call(String line) {
				String[] parts = line.split(",");

				Person person = new Person();
				person.setName(parts[0]);
				person.setAge(Integer.parseInt(parts[1].trim()));

				return person;
			}
		});

		DataFrame schemaPeople = sqlContext.createDataFrame(people, Person.class);
		schemaPeople.registerTempTable("people");

		DataFrame teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 29");

		List<String> teenagerNames = teenagers.toJavaRDD().map(new Function<Row, String>() {
			@Override
			public String call(Row row) {
				return "Name: " + row.getString(0);
			}
		}).collect();
		for (String name : teenagerNames) {
			System.out.println(name);
		}

		//teenagers.write().partitionBy("age").parquet("");

		/*parquetFile.registerTempTable("parquetFile");
		DataFrame teenagers2 = sqlContext.sql("SELECT name FROM parquetFile WHERE age >= 13 AND age <= 19");
		teenagerNames = teenagers2.toJavaRDD().map(new Function<Row, String>() {
			@Override
			public String call(Row row) {
				return "Name: " + row.getString(0);
			}
		}).collect();
		for (String name : teenagerNames) {
			System.out.println(name);
		}*/

		ctx.stop();
	}
}
