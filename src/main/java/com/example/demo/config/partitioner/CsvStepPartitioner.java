package com.example.demo.config.partitioner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.core.io.ClassPathResource;

import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.HashMap;
import java.util.Map;

import static com.example.demo.constants.BatchConstants.CUSTOMERS_FILENAME;

public class CsvStepPartitioner implements Partitioner {

    private static final Logger logger = LoggerFactory.getLogger(CsvStepPartitioner.class);

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        Map<String, ExecutionContext> result = new HashMap<>();

        int noOfLines = 0;
        try {
            noOfLines = getNoOfLines(CUSTOMERS_FILENAME);
        } catch (IOException e) {
            e.printStackTrace();
        }

        int firstLine = 2; 
        int lastLine = gridSize + 1;
        int partitionNumber = 0;

        while(firstLine < noOfLines) {

            if(lastLine >= noOfLines) {
                lastLine = noOfLines;
            }

            logger.info("Partition number : {}, first line is : {}, last  line is : {} ", partitionNumber, firstLine, lastLine);

            ExecutionContext value = new ExecutionContext();

            value.putLong("partition_number", partitionNumber);
            value.putLong("first_line", firstLine);
            value.putLong("last_line", lastLine);

            result.put("PartitionNumber-" + partitionNumber, value);

            firstLine = firstLine + gridSize;
            lastLine = lastLine + gridSize;
            partitionNumber++;
        }

        logger.info("No of lines {}", noOfLines);

        return result;
    }

    public int getNoOfLines(String fileName) throws IOException {
        ClassPathResource classPathResource = new ClassPathResource(fileName);
        LineNumberReader reader = new LineNumberReader(new FileReader(classPathResource.getFile().getAbsolutePath()));
        reader.skip(Integer.MAX_VALUE);
        return reader.getLineNumber();
    }
}
