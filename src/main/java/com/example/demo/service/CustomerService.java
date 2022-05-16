package com.example.demo.service;

import java.util.List;
import com.example.demo.entity.Customer;



public interface CustomerService {
	List<Customer> getAllCustomers();
	
	Customer saveCustomer(Customer customer);
	
	Customer getCustomerById(Long id);
	
	Customer updateCustomer(Customer customer);
	
	void deleteCustomerById(Long id);
	
	List<Customer> getByKeyword(String keyword);
}
