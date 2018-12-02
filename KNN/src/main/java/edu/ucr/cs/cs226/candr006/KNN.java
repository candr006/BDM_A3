package edu.ucr.cs.cs226.candr006;
        import java.io.*;
        import java.time.LocalDateTime;
        import java.time.format.DateTimeFormatter;

        import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
        import java.io.IOException;
        import java.util.Comparator;
        import java.util.Scanner;

        import org.apache.spark.api.java.JavaPairRDD;
        import org.apache.spark.api.java.JavaRDD;
        import org.apache.spark.api.java.JavaSparkContext;
        import org.apache.spark.api.java.function.Function2;
        import org.apache.spark.api.java.function.PairFunction;
        import scala.Tuple2;


/**
 * KNN
 *
 */
class SparkRDDKNN
{
    private int k;
    private String q_comma;
    private String file_name;

    SparkRDDKNN(int k1, String q1, String file_name1){
        //constructor
        k=k1;
        q_comma=q1;
        file_name=file_name1;
    }

    void run(){

        JavaSparkContext spark =
                new JavaSparkContext("local", "SparkRDD-KNN");
        JavaRDD<String> logFile = spark.textFile("local_copy.csv");

        //calculate the distance from q to k and add to a map
        JavaPairRDD<Double, String> distanceMap = logFile.mapToPair(new PairFunction<String, Double, String>() {

            public Tuple2<Double, String> call(String line) throws Exception {
                String[] parts = line.split(",");
                String[] q=q_comma.split(",");
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
        //Integer k= Integer.valueOf(args[2]);

        for(Tuple2<Double, String> line:distinctNeighbors.sortByKey().collect()){
            if(k>0) {
                System.out.println("* " + line);
                k--;
            }else{
                break;
            }
        }
    }
}

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
        String out_path="KNN_output_"+formatted+".txt";

        //q=args[1] and k=args[2]
        SparkRDDKNN alg1 = new SparkRDDKNN(Integer.valueOf(args[2]), args[1], "local_copy.csv");

        Scanner reader = new Scanner(System.in);  // Reading from System.in
        int n=-1;
        System.out.println("\nSelect one:\n1. Spark RDD KNN \n2. Spark SQL KNN \n3. Exit \n \n");

        while (n!=3) {
            n = reader.nextInt();
            if(n==1){
                alg1.run();
            }else if(n==2){
                //call alg2
            }else if(n==3){
                return;
            }
            System.out.println("\n \n Choose one of the options: \n 1. Spark RDD KNN \n 2. Spark SQL KNN 3. Exit \n \n");


        }


    }
}
