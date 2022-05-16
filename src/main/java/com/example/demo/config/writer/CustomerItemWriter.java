package com.example.demo.config.writer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;

import com.example.demo.entity.Customer;
import com.example.demo.repository.CustomerRepository;

import java.util.List;

public class CustomerItemWriter implements ItemWriter<Customer> {

    private static final Logger logger = LoggerFactory.getLogger(CustomerItemWriter.class);

    private final CustomerRepository customerRepository;

    public CustomerItemWriter(CustomerRepository customerRepository) {
        this.customerRepository = customerRepository;
    }

    @Override
    public void write(List<? extends Customer> customerlist) throws Exception {
        logger.info("Saving {} customers objects", customerlist.size());
        customerRepository.saveAll(customerlist);
    }
}
