package edu.ucr.cs.cs226.candr006;
import java.io.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.StringTokenizer;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import java.io.IOException;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Map;


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
        String str_local_file=args[1];

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

        //q=args[2] and k=args[3]
        final String desiredCode = "1.0";

        JavaSparkContext spark =
                new JavaSparkContext("local", "CS226-Demo");
        JavaRDD<String> logFile = spark.textFile("local_copy.csv");
        JavaRDD<String> okLines = logFile.filter(new Function<String, Boolean>() {
            public Boolean call(String s) {
                String[] parts = s.split("\t");
                String code = parts[5];
                return code.equals(desiredCode);
            }
        });

        JavaRDD<Long> bytes = okLines.map(new Function<String, Long>() {
            public Long call(String s) throws Exception {
                return Long.parseLong(s.split("\t")[6]);
            }
        });

        JavaPairRDD<String, Long> codeSize = logFile.mapToPair(new PairFunction<String, String, Long>() {

            public Tuple2<String, Long> call(String line) throws Exception {
                String[] parts = line.split("\t");
                String code = parts[5];
                Long size = Long.parseLong(parts[6]);
                return new Tuple2<String, Long>(code, size);
            }
        });
        JavaPairRDD<String, Long> sumBytesByCode = codeSize.reduceByKey(new Function2<Long, Long, Long>() {
            public Long call(Long s1, Long s2) throws Exception {
                return s1 + s2;
            }
        });
        Map<String, Long> sumBytesByCodeMap = sumBytesByCode.collectAsMap();
        System.out.println(sumBytesByCodeMap);
        Long sumBytes = bytes.reduce(new Function2<Long, Long, Long>() {
            public Long call(Long s1, Long s2) throws Exception {
                return s1 + s2;
            }
        });

        System.out.printf("Sum Bytes is %d\n", sumBytes);

        long numOfLines = okLines.count();
        System.out.printf("%d lines with OK in the file\n", numOfLines);
        System.out.println( "Hello World!" );
    }
}
