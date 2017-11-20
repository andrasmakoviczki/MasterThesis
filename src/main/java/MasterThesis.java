import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.hipi.image.ByteImage;
import org.hipi.image.HipiImage;
import org.hipi.image.HipiImageHeader;
import org.hipi.image.io.JpegCodec;
import org.hipi.imagebundle.mapreduce.HibInputFormat;
import scala.Tuple2;

import java.io.IOException;

public class MasterThesis {

    /*private static void removeDir(String pathToDirectory, Configuration conf) throws IOException {
        Path pathToRemove = new Path(pathToDirectory);
        FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(pathToRemove)) {
            fileSystem.delete(pathToRemove, true);
        }
    }*/

    public static transient FileSystem fileSystem = null;

    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf().setAppName("ReadImages");
        JavaSparkContext sc = new JavaSparkContext(conf);
        Configuration hconf = sc.hadoopConfiguration();
        hconf.set("imagetype", "ByteImage");

        String inputPath = args[0];
        String outputPath = args[1];
        hconf.setStrings("jpegfromhib.outdir", outputPath);

        JavaPairRDD<HipiImageHeader, HipiImage> images = sc.newAPIHadoopFile(inputPath, HibInputFormat.class, HipiImageHeader.class, HipiImage.class, hconf);

        fileSystem = FileSystem.get(sc.hadoopConfiguration());
        final Path path = new Path(outputPath);
        fileSystem.mkdirs(path);

        JavaPairRDD<Boolean, String> i = images.mapToPair(new PairFunction<Tuple2<HipiImageHeader, HipiImage>, Boolean, String>() {
            @Override
            public Tuple2<Boolean, String> call(Tuple2<HipiImageHeader, HipiImage> hipiImageHeaderHipiImageTuple2) throws Exception {
                HipiImageHeader header = hipiImageHeaderHipiImageTuple2._1;
                ByteImage image = (ByteImage) hipiImageHeaderHipiImageTuple2._2;

                String source = header.getMetaData("source");
                String base = FilenameUtils.getBaseName(source);
                Path outpath = new Path(path + "/" + base + ".jpg");

                FSDataOutputStream os = fileSystem.create(outpath);
                JpegCodec.getInstance().encodeImage(image, os);
                os.flush();
                os.close();

                return new Tuple2<Boolean, String>(true, base);
            }
        });

        System.out.println(images.count());
    }
}
