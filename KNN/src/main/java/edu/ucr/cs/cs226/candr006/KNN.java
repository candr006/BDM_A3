package edu.ucr.cs.cs226.candr006;
import java.io.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import static java.lang.System.out;


/**
 * KNN
 *
 */

public class KNN
{

    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException {
        //check that all arguments are there
        if(args.length<3){
            out.println("\n\nERROR: You are missing one or more arguments.");
            out.println("<local file path> <point q> <k>");
            out.println("Exiting");
            return;
        }
        String str_local_file=args[0];

        //check if the local file exists
        File localFile= new File(str_local_file);
        if(!localFile.exists()){
            out.println("\n\nERROR: The local file you entered does not exist. Exiting.\n");
            return;
        }

        //first decompress bzip file- Input file assumed to be a .bz2 type
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

        //This is used to give the output files a unique name
        //The RDD and SQL Implementations each output to their own file
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("MMddyyyyHHmmss");
        LocalDateTime now = LocalDateTime.now();
        String formatted = dtf.format(now);
        String out_path_rdd="SparkRDD_KNN_output_"+formatted+".txt";
        String out_path_sql="SparkSQL_KNN_output_"+formatted+".txt";



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

        //print k neighbors to a file
        Integer k= Integer.valueOf(args[2]);
        FileWriter fileWriter = new FileWriter(out_path_rdd);
        PrintWriter printWriter = new PrintWriter(fileWriter);

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


        //Read the file in
        Dataset<Row> input_csv = session_sql.read().option("header", "false").csv("local_copy.csv");
        input_csv=input_csv.select(
                input_csv.col("_c0"),
                input_csv.col("_c1"), //x
                input_csv.col("_c2")); //y

        Encoder<String> stringEncoder = Encoders.STRING();

        //Convert to Java RDD and perform map function to get distance of each point
        JavaRDD<Row> mapped_rdd = input_csv.javaRDD().map(

                new Function<Row, Row>() {

                    @Override
                    public Row call(Row line) throws Exception {
                        String[] q1 = args[1].split(",");
                        Double x11 = Double.parseDouble(q1[0]);
                        Double y11 = Double.parseDouble(q1[1]);

                        Double x2 = Double.parseDouble(line.get(1).toString());
                        Double y2 = Double.parseDouble(line.get(2).toString());

                        double dist = Math.sqrt(Math.pow((x2 - x11), 2) + Math.pow((y2 - y11), 2));

                        String xy_string = line.get(1).toString() + "," + line.get(2).toString();

                        Row r1=RowFactory.create(dist,xy_string);
                        return r1;
                    }
                });


    //schema details the data types
        StructType schema = new StructType(new StructField[] {
                new StructField("distance", DataTypes.DoubleType,true, Metadata.empty()),
                new StructField("x_y_string", DataTypes.StringType, true,Metadata.empty()),
        });


        //create a data frame from the mapped rdd
        Dataset<Row> mapped_df =session_sql.createDataFrame(mapped_rdd,schema);

        //create nearest neighbors table
        mapped_df.registerTempTable("nn");

        //use sql to get the distinct neigbors, sort by distance, and also limit to k lines
        Dataset<Row> reducedCSVDataset = session_sql.sql("select distinct concat(distance,',',x_y_string) as out from nn order by out limit "+args[2]);

        Dataset<String> knn = reducedCSVDataset.toDF().select("out").as(Encoders.STRING());
        //collect the knn as a list
        List<String> knn_list = knn.collectAsList();

        //output knn to a file
        knn.rdd().saveAsTextFile(out_path_sql);


        out.println("\n\nDone. Please check the 2 outputs (Spark RDD file and Spark SQL directory)\n");

    }

}

