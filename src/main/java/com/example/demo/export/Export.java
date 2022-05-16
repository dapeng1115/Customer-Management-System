package com.example.demo.export;

import com.example.demo.entity.*;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.sql.DataSource;

import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;
import static com.example.demo.constants.BatchConstants.*;

@EnableBatchProcessing
@Configuration
public class Export {
	@Bean
    public Job multithreadedJob(JobBuilderFactory jobBuilderFactory) throws Exception {
        return jobBuilderFactory
                .get("Multithreaded JOB")
                .incrementer(new RunIdIncrementer())
                .flow(multithreadedManagerStep(null))
                .end()
                .build();
    }

    @Bean
    public Step multithreadedManagerStep(StepBuilderFactory stepBuilderFactory) throws Exception {
        return stepBuilderFactory
                .get("Multithreaded : Read -> Process -> Write ")
                .<Customer, Customer>chunk(CHUNK_SIZE)
                .reader(multithreadedcReader(null))
                .writer(multithreadedcWriter())
                .taskExecutor(taskExecutor())
                .build();
    }

    @Bean
    public TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(POOL_SIZE);
        executor.setMaxPoolSize(POOL_SIZE);
        executor.setQueueCapacity(QUEUE_CAPACITY);
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.setThreadNamePrefix(THREAD_NAME_PREFIX);
        return executor;
    }

    @Bean
    public ItemReader<Customer> multithreadedcReader(DataSource dataSource) {

        return new JdbcPagingItemReaderBuilder<Customer>()
                .name("Reader")
                .dataSource(dataSource)
                .selectClause("SELECT * ")
                .fromClause("FROM customers ")
                .sortKeys(Collections.singletonMap("ID", Order.ASCENDING))
                .rowMapper(new CustomerRowMapper())
                .build();
    }

    @Bean
    public FlatFileItemWriter<Customer> multithreadedcWriter() {

        return new FlatFileItemWriterBuilder<Customer>()
                .name("Writer")
                .append(false)
                .resource(new FileSystemResource("customers.csv"))
                .lineAggregator(new DelimitedLineAggregator<Customer>() {
                    {
                        setDelimiter(";");
                        setFieldExtractor(new BeanWrapperFieldExtractor<Customer>() {
                            {
                                setNames(new String[]{"id", "firstName", "lastName", "email", "dateOfBirth"});
                            }
                        });
                    }
                })
                .build();
    }
}
