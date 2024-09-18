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

public class ParquetReaderExample {
    public static void main(String[] args) throws IOException {
        String parquetFilePath = "/path/to/your/file.parquet";
        
        Configuration conf = new Configuration();
        Path path = new Path(parquetFilePath);
        
        try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf))) {
            MessageType schema = reader.getFooter().getFileMetaData().getSchema();
            List<Type> fields = schema.getFields();
            
            System.out.println("Column Headers:");
            for (Type field : fields) {
                System.out.println(field.getName());
            }
            
            System.out.println("\nFirst 10 rows of data:");
            PageReadStore pages;
            int rowCount = 0;
            
            while ((pages = reader.readNextRowGroup()) != null && rowCount < 10) {
                long rows = pages.getRowCount();
                MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
                RecordReader<SimpleGroup> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
                
                for (int i = 0; i < rows && rowCount < 10; i++) {
                    SimpleGroup simpleGroup = recordReader.read();
                    System.out.println(simpleGroup);
                    rowCount++;
                }
            }
            
            // Example of filtering using two columns
            System.out.println("\nFiltered results (example):");
            reader.close();
            reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf));
            
            while ((pages = reader.readNextRowGroup()) != null) {
                long rows = pages.getRowCount();
                MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
                RecordReader<SimpleGroup> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
                
                for (int i = 0; i < rows; i++) {
                    SimpleGroup simpleGroup = recordReader.read();
                    // Replace "column1" and "column2" with your actual column names
                    // Adjust the conditions as needed
                    if (simpleGroup.getString("column1", 0).equals("desiredValue1") &&
                        simpleGroup.getInteger("column2", 0) > 100) {
                        System.out.println(simpleGroup);
                    }
                }
            }
        }
    }
}