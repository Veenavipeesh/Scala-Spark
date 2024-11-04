import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object spark_checking_is_adult {
def main(args: Array[String]): Unit = {

  /*creating conf*/
  val conf = new SparkConf()
  conf.set("spark.app.name","spark_program")
  conf.set("spark.master","local[*]")

  /*creating spark session*/
  val ss = SparkSession.builder().config(conf).getOrCreate()

  import ss.implicits._

  /*creating dataframe*/
  val employees_df = List(
    (1, "John", 28),
    (2, "Jane", 35),
    (3, "Doe", 17)
  ).toDF("id", "name", "age")

  /*adding new column is_adult with values true
  if age is greater than or equal to 18 and false otherwise */
  employees_df.withColumn("is_adult", when(col("age")>= 18, "True")
  .otherwise("False")).show()

}
}
