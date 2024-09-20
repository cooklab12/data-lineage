import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import java.util.ArrayList;
import java.util.List;

public class ParquetReaderExample {
    public static void main(String[] args) throws IOException {
        // Change this to your local file path
        String localFilePath = "/path/to/your/local/file.parquet";
        
        Configuration conf = new Configuration();
        Path path = new Path(localFilePath);
        
        ObjectMapper objectMapper = new ObjectMapper();
        List<ObjectNode> jsonRows = new ArrayList<>();
        
        try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf))) {
            MessageType schema = reader.getFooter().getFileMetaData().getSchema();
            List<Type> fields = schema.getFields();
            
            PageReadStore pages;
            while ((pages = reader.readNextRowGroup()) != null) {
                long rows = pages.getRowCount();
                MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
                RecordReader<SimpleGroup> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
                
                for (int i = 0; i < rows; i++) {
                    SimpleGroup simpleGroup = recordReader.read();
                    ObjectNode jsonRow = objectMapper.createObjectNode();
                    
                    for (int j = 0; j < fields.size(); j++) {
                        String fieldName = fields.get(j).getName();
                        Type.Repetition repetition = fields.get(j).getRepetition();
                        
                        if (repetition == Type.Repetition.REPEATED) {
                            int valueCount = simpleGroup.getFieldRepetitionCount(j);
                            List<Object> values = new ArrayList<>();
                            for (int k = 0; k < valueCount; k++) {
                                values.add(getFieldValue(simpleGroup, j, k));
                            }
                            jsonRow.putPOJO(fieldName, values);
                        } else {
                            Object value = getFieldValue(simpleGroup, j, 0);
                            if (value != null) {
                                jsonRow.putPOJO(fieldName, value);
                            }
                        }
                    }
                    
                    jsonRows.add(jsonRow);
                }
            }
        }
        
        // Print all rows in JSON format
        System.out.println(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(jsonRows));
    }
    
    private static Object getFieldValue(SimpleGroup simpleGroup, int fieldIndex, int valueIndex) {
        Type fieldType = simpleGroup.getType().getType(fieldIndex);
        String typeName = fieldType.asPrimitiveType().getPrimitiveTypeName().name();
        
        switch (typeName) {
            case "BINARY":
                return simpleGroup.getString(fieldIndex, valueIndex);
            case "INT32":
                return simpleGroup.getInteger(fieldIndex, valueIndex);
            case "INT64":
                return simpleGroup.getLong(fieldIndex, valueIndex);
            case "FLOAT":
                return simpleGroup.getFloat(fieldIndex, valueIndex);
            case "DOUBLE":
                return simpleGroup.getDouble(fieldIndex, valueIndex);
            case "BOOLEAN":
                return simpleGroup.getBoolean(fieldIndex, valueIndex);
            default:
                return null;
        }
    }
}