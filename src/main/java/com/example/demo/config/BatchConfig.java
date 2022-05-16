package com.example.demo.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.logging.log4j.LogManager;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.configuration.support.JobRegistryBeanPostProcessor;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;

import com.example.demo.repository.CustomerRepository;
import com.example.demo.config.partitioner.*;
import com.example.demo.config.writer.CustomerItemWriter;
import com.example.demo.entity.Customer;

import static com.example.demo.constants.BatchConstants.*;

@Configuration
@EnableBatchProcessing
public class BatchConfig {

    private static final org.apache.logging.log4j.Logger Logger = LogManager.getLogger(BatchConfig.class);
    public static final Long LONG_OVERRIDE = null;

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final CustomerRepository customerRepository;

    public BatchConfig(JobBuilderFactory jobBuilderFactory,
                       StepBuilderFactory stepBuilderFactory,
                       CustomerRepository customerRepository) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.customerRepository = customerRepository;
    }

    @Bean
    public Job loadCsvFileJob() {
        return this.jobBuilderFactory.get(LOAD_CSV_FILE_JOB)
                .start(loadCsvStepPartitioner())
                .build();
    }

    private Step loadCsvStepPartitioner() {
        return stepBuilderFactory.get(LOAD_CSV_STEP_PARTITIONER)
                .partitioner(loadCsvStep().getName(), csvStepPartitioner())
                .partitionHandler(loadCsvFileStepPartitionHandler(loadCsvStep(), GRID_SIZE))
                .build();
    }

    private CsvStepPartitioner csvStepPartitioner() {
        return new CsvStepPartitioner();
    }

    private PartitionHandler loadCsvFileStepPartitionHandler(final Step step,
                                                             final int gridSize) {
        TaskExecutorPartitionHandler taskExecutorPartitionHandler =
                new TaskExecutorPartitionHandler();
        taskExecutorPartitionHandler.setTaskExecutor(taskExecutor());
        taskExecutorPartitionHandler.setStep(step);
        taskExecutorPartitionHandler.setGridSize(gridSize);
        return taskExecutorPartitionHandler;
    }

    public TaskExecutor taskExecutor() {
        SimpleAsyncTaskExecutor asyncTaskExecutor =
                new SimpleAsyncTaskExecutor(THREAD_NAME_PREFIX);
        asyncTaskExecutor.setThreadNamePrefix(SLAVE_THREAD);
        asyncTaskExecutor.setConcurrencyLimit(CONCURRENCY_LIMIT);
        return asyncTaskExecutor;
    }

    public Step loadCsvStep() {
        return this.stepBuilderFactory.get(LOAD_CSV_STEP)
                .<Customer, Customer>chunk(CHUNK_SIZE)
                .reader(flatFileItemReader(LONG_OVERRIDE, LONG_OVERRIDE,
                        LONG_OVERRIDE))
                .writer(writer())
                .build();
    }

    @Bean
    public JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor(JobRegistry jobRegistry) {
        JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor = new JobRegistryBeanPostProcessor();
        jobRegistryBeanPostProcessor.setJobRegistry(jobRegistry);
        return jobRegistryBeanPostProcessor;
    }


    public CustomerItemWriter writer() {
        return new CustomerItemWriter(customerRepository);
    }

    @Bean
    @StepScope
    public FlatFileItemReader<Customer> flatFileItemReader(
            @Value("#{stepExecutionContext[partition_number]}") final Long partitionNumber,
            @Value("#{stepExecutionContext[first_line]}") final Long firstLine,
            @Value("#{stepExecutionContext[last_line]}") final Long lastLine) {

    	Logger.info("Partition Number : {}, Reading file from line : {}, to line: {} ", partitionNumber, firstLine, lastLine);

        FlatFileItemReader<Customer> reader = new FlatFileItemReader<>();
        reader.setLinesToSkip(Math.toIntExact(firstLine));
        reader.setMaxItemCount(Math.toIntExact(lastLine));
        reader.setResource(new ClassPathResource(CUSTOMERS_FILENAME));
        reader.setLineMapper(new DefaultLineMapper<Customer>() {
            {
                setLineTokenizer(new DelimitedLineTokenizer() {
                    {
                        setNames(Attribute_1, Attribute_2, Attribute_3, Attribute_4);
                    }
                });

                setFieldSetMapper(new BeanWrapperFieldSetMapper<Customer>() {
                    {
                        setTargetType(Customer.class);
                    }
                });

            }
        });
        return reader;
    }
}

