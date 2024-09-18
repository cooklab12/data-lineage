import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.util.List;

public class ParquetReaderExampleHDFS {
    public static void main(String[] args) throws IOException {
        // Change this to your HDFS path
        String hdfsPath = "hdfs://your-hdfs-namenode:8020/path/to/your/file.parquet";
        
        Configuration conf = new Configuration();
        // Set HDFS configuration
        conf.set("fs.defaultFS", "hdfs://your-hdfs-namenode:8020");
        Path path = new Path(hdfsPath);
        
        try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf))) {
            // Rest of the code remains the same
            MessageType schema = reader.getFooter().getFileMetaData().getSchema();
            List<Type> fields = schema.getFields();
            
            // ... (rest of the code)
        }
    }
}