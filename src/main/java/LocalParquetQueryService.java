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

import com.google.gson.Gson;
import spark.Spark;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.schema.PrimitiveType;


public class LocalParquetQueryService {
    private static final String PARQUET_FILE_PATH = "C:\\Users\\ravib\\Downloads\\MTCars.parquet";
    private static final Gson gson = new Gson();
    private static MessageType schema;
    private static List<String> columnNames;

    public static void main(String[] args) {
        initializeSchema();

        Spark.port(4567);

        Spark.get("/columns", (req, res) -> {
            res.type("application/json");
            return gson.toJson(getColumnInfo());
        });

        Spark.get("/analyze/:column", (req, res) -> {
            String columnName = req.params(":column");
            res.type("application/json");
            return gson.toJson(analyzeColumn(columnName));
        });
    }

    private static void initializeSchema() {
        try {
            ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(PARQUET_FILE_PATH), new Configuration()));
            schema = reader.getFooter().getFileMetaData().getSchema();
            columnNames = schema.getFields().stream().map(Type::getName).toList();
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static List<Map<String, String>> getColumnInfo() {
        List<Map<String, String>> columns = new ArrayList<>();
        for (Type field : schema.getFields()) {
            Map<String, String> column = new HashMap<>();
            column.put("name", field.getName());
            column.put("type", field.asPrimitiveType().getPrimitiveTypeName().name());
            columns.add(column);
        }
        return columns;
    }

    private static Map<String, Object> analyzeColumn(String columnName) {
        Map<String, Object> analysis = new HashMap<>();
        try {
            ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(PARQUET_FILE_PATH), new Configuration()));

            long totalRows = 0;
            Set<Object> distinctValues = new HashSet<>();
            long nullValues = 0;
            Comparable<?> minValue = null;
            Comparable<?> maxValue = null;

            int columnIndex = schema.getFieldIndex(columnName);
            Type columnType = schema.getType(columnIndex);

            PageReadStore pages;
            while ((pages = reader.readNextRowGroup()) != null) {
                long rows = pages.getRowCount();
                totalRows += rows;
                MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
                RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

                for (int i = 0; i < rows; i++) {
                    Group group = recordReader.read();
                    if (group.getFieldRepetitionCount(columnName) == 0) {
                        nullValues++;
                    } else {
                        Object value = getValue(group, columnName, columnType);
                        distinctValues.add(value);
                        if (value instanceof Comparable) {
                            if (minValue == null || ((Comparable) value).compareTo(minValue) < 0) {
                                minValue = (Comparable) value;
                            }
                            if (maxValue == null || ((Comparable) value).compareTo(maxValue) > 0) {
                                maxValue = (Comparable) value;
                            }
                        }
                    }
                }
            }
            reader.close();

            analysis.put("totalRows", totalRows);
            analysis.put("distinctValues", distinctValues.size());
            analysis.put("nullValues", nullValues);
            analysis.put("minValue", minValue);
            analysis.put("maxValue", maxValue);

        } catch (IOException e) {
            e.printStackTrace();
            analysis.put("error", e.getMessage());
        }
        return analysis;
    }

    private static Object getValue(Group group, String columnName, Type columnType) {
        switch (columnType.asPrimitiveType().getPrimitiveTypeName()) {
            case INT32:
                return group.getInteger(columnName, 0);
            case INT64:
                return group.getLong(columnName, 0);
            case FLOAT:
                return group.getFloat(columnName, 0);
            case DOUBLE:
                return group.getDouble(columnName, 0);
            case BOOLEAN:
                return group.getBoolean(columnName, 0);
            case BINARY:
            default:
                return group.getString(columnName, 0);
        }
    }
}