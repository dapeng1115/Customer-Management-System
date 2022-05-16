package com.example.demo.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.example.demo.entity.Customer;

public interface CustomerRepository extends JpaRepository<Customer, Long>{
	
	 @Query(value = "select * from customers s where s.first_name like %:keyword% or s.last_name like %:keyword% or s.date_birth like %:keyword%", nativeQuery = true)
	 List<Customer> findByKeyword(@Param("keyword") String keyword);
}



