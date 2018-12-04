package edu.ucr.cs.cs226.candr006;
import java.io.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.functions.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import static org.apache.spark.api.java.JavaRDDLike$class.collect;
import static org.apache.spark.sql.types.DataTypes.IntegerType;


/**
 * KNN
 *
 */

public class KNN
{

    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException {
        //check that all arguments are there
        if(args.length<3){
            System.out.println("\n\nERROR: You are missing one or more arguments.");
            System.out.println("<local file path> <point q> <k>");
            System.out.println("Exiting");
            return;
        }
        String str_local_file=args[0];

        //check if the local file exists
        File localFile= new File(str_local_file);
        if(!localFile.exists()){
            System.out.println("\n\nERROR: The local file you entered does not exist. Exiting.\n");
            return;
        }

        //first decompress bzip file
        FileInputStream is4 = new FileInputStream(localFile);
        BZip2CompressorInputStream inputStream4 = new BZip2CompressorInputStream(is4, true);
        OutputStream ostream4 = new FileOutputStream("local_copy.csv");
        final byte[] buffer4 = new byte[8192];
        int n4 = 0;
        while ((n4 = inputStream4.read(buffer4))>0) {
            ostream4.write(buffer4, 0, n4);
        }
        ostream4.close();
        inputStream4.close();

        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("MMddyyyyHHmmss");
        LocalDateTime now = LocalDateTime.now();
        String formatted = dtf.format(now);
        String out_path_rdd="SparkRDD_KNN_output_"+formatted+".txt";
        String out_path_sql="SparkSQL_KNN_output_"+formatted+".txt";

        //q=args[1] and k=args[2]
        /**********************SparkRDD Implementation**********************
         *
         */

        JavaSparkContext spark =
                new JavaSparkContext("local", "CS226-Demo");
        JavaRDD<String> logFile = spark.textFile("local_copy.csv");

        //calculate the distance from q to k and add to a map
        JavaPairRDD<Double, String> distanceMap = logFile.mapToPair(new PairFunction<String, Double, String>() {

            public Tuple2<Double, String> call(String line) throws Exception {
                String[] parts = line.split(",");
                String[] q=args[1].split(",");
                Double x1= Double.parseDouble(q[0]);
                Double y1= Double.parseDouble(q[1]);


                Double x2= Double.valueOf(parts[1]);
                Double y2= Double.valueOf(parts[2]);

                double dist=Math.sqrt(Math.pow((x2-x1),2)+Math.pow((y2-y1),2));

                String xy_string= parts[1]+","+parts[2];


                return new Tuple2<Double,String>(dist, xy_string);
            }
        });

        //now sort by distance
        JavaPairRDD<Double, String> sortedDistance = distanceMap.sortByKey(true);

        //remove duplicates
        JavaPairRDD<Double, String> distinctNeighbors = sortedDistance.distinct();

        //print k neighbors
        Integer k= Integer.valueOf(args[2]);
        FileWriter fileWriter = new FileWriter(out_path_rdd);
        PrintWriter printWriter = new PrintWriter(fileWriter);
        System.out.println("-------------------Spark RDD KNN -----------------------");


        for(Tuple2<Double, String> line:distinctNeighbors.sortByKey().collect()){
            if(k>0) {
                printWriter.println(line+"\n");
                k--;
            }else{
                break;
            }
        }
        printWriter.close();


        /**********************SparkSQL Implementation**********************
         *
         */
        SparkSession session_sql = SparkSession
                .builder()
                .appName("SparkSQL")
                .getOrCreate();


        String[] q=args[1].split(",");
        Double x1= Double.parseDouble(q[0]);
        Double y1= Double.parseDouble(q[1]);



        Dataset<Row> input_csv = session_sql.read().option("header", "false").csv("local_copy.csv");
        input_csv=input_csv.select(
                input_csv.col("_c0"),
                input_csv.col("_c1"), //x
                input_csv.col("_c2")); //y
        List<Row> mapped = new ArrayList<Row>();

       // System.out.println("\n\n------------entering for each loop-----------------\n\n");

        input_csv.foreach((ForeachFunction<Row>) row ->{

            double x2=Double.valueOf(row.getAs("_c1").toString()); //x
            double y2=Double.valueOf(row.getAs("_c2").toString()); //y
            double dist=Math.sqrt(Math.pow((x2-x1),2)+Math.pow((y2-y1),2));

            String xy_string= String.valueOf(x2)+","+String.valueOf(y2);
            Row r1=RowFactory.create(dist,xy_string);

            //Add to List of Row
            mapped.add(r1);
        });

        System.out.println("\n\n------------finished looping through list-----------------\n\n");

        StructType schema = new StructType(new StructField[] {
                new StructField("distance", DataTypes.DoubleType, false, null),
                new StructField("x_y_string", DataTypes.StringType, false, null),
        });
        System.out.println("\n\n------------create schema----------------\n\n");


        Dataset<Row> mapped_df =session_sql.createDataFrame(mapped,schema);
        System.out.println("\n\n------------create dataframe----------------\n\n");
       // Dataset<Row> distinct_neighbors = mapped_df.distinct().sort("distance");
        System.out.println("\n\n------------create distinct neighbors----------------\n\n");
        //distinct_neighbors.show(Integer.valueOf(args[2]));
        mapped_df.createOrReplaceTempView("nn");
        System.out.println("\n\n------------temp table----------------\n\n");
        Dataset<Row> reducedCSVDataset = session_sql.sql("select distinct distance,x_y_string from nn order by distance limit "+args[2]);
        System.out.println("\n\n------------sel from temp----------------\n\n");
        Dataset<String> knn = reducedCSVDataset.toDF().select("distance","x_y_string").as(Encoders.STRING());
        System.out.println("\n\n------------encode as string----------------\n\n");
        List<String> knn_list = knn.collectAsList();
        knn_list.forEach(x -> System.out.println(x));



                //.collect();
        //session_sql.sql("SELECT id,x,y FROM csv.`local_copy.csv`");
       /* input_csv=input_csv.select(
                input_csv.col("id"),
                input_csv.col("x"),
                input_csv.col("y"));

        // double dist=Math.sqrt(Math.pow((x2-x1),2)+Math.pow((y2-y1),2));
        input_csv= input_csv.select(
                input_csv.col("x").pow(2)
            );

       // Dataset<Row> mapped= new Dataset();
        //input_csv.foreach((ForeachFunction<Row>) row ->{ double x1=double.valueof(row[1]); });*/


        System.out.println("\n\nDone. Please check output files\n");

    }
}

