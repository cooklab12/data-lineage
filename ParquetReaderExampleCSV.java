import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ParquetReaderExampleCSV {
    public static void main(String[] args) throws IOException {
        String localFilePath = "/path/to/your/local/file.parquet";
        String outputCsvPath = "/path/to/your/output/file.csv";
        
        Configuration conf = new Configuration();
        Path path = new Path(localFilePath);
        
        try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf));
             FileWriter fileWriter = new FileWriter(outputCsvPath);
             CSVPrinter csvPrinter = new CSVPrinter(fileWriter, CSVFormat.DEFAULT)) {
            
            MessageType schema = reader.getFooter().getFileMetaData().getSchema();
            List<Type> fields = schema.getFields();
            
            // Write header
            List<String> header = new ArrayList<>();
            for (Type field : fields) {
                header.add(field.getName());
            }
            csvPrinter.printRecord(header);
            
            PageReadStore pages;
            while ((pages = reader.readNextRowGroup()) != null) {
                long rows = pages.getRowCount();
                MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
                RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
                
                for (int i = 0; i < rows; i++) {
                    Group group = recordReader.read();
                    List<String> record = new ArrayList<>();
                    
                    for (int j = 0; j < fields.size(); j++) {
                        Type.Repetition repetition = fields.get(j).getRepetition();
                        
                        if (repetition == Type.Repetition.REPEATED) {
                            int valueCount = group.getFieldRepetitionCount(j);
                            List<String> values = new ArrayList<>();
                            for (int k = 0; k < valueCount; k++) {
                                values.add(String.valueOf(getFieldValue(group, j, k)));
                            }
                            record.add(String.join("|", values));
                        } else {
                            record.add(String.valueOf(getFieldValue(group, j, 0)));
                        }
                    }
                    
                    csvPrinter.printRecord(record);
                }
            }
            
            System.out.println("CSV file has been created at: " + outputCsvPath);
        }
    }
    
    private static Object getFieldValue(Group group, int fieldIndex, int valueIndex) {
        Type fieldType = group.getType().getType(fieldIndex);
        String typeName = fieldType.asPrimitiveType().getPrimitiveTypeName().name();
        
        switch (typeName) {
            case "BINARY":
                return group.getString(fieldIndex, valueIndex);
            case "INT32":
                return group.getInteger(fieldIndex, valueIndex);
            case "INT64":
                return group.getLong(fieldIndex, valueIndex);
            case "FLOAT":
                return group.getFloat(fieldIndex, valueIndex);
            case "DOUBLE":
                return group.getDouble(fieldIndex, valueIndex);
            case "BOOLEAN":
                return group.getBoolean(fieldIndex, valueIndex);
            default:
                return "";
        }
    }
}