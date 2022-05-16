package com.example.demo.entity;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.ResolverStyle;
import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EntityListeners;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import org.hibernate.annotations.SQLDelete;
import org.hibernate.annotations.Where;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

import com.example.demo.validator.DateValidator;

@EntityListeners(value = AuditingEntityListener.class)
@Table(name = "customers")
@Where(clause="active = true")
@SQLDelete(sql = "update customers set active=false where id=?")
@Entity
public class Customer {
	
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;
	
	@Column(name = "first_name", nullable = false)
	private String firstName;
	
	@Column(name = "last_name")
	private String lastName;
	
	@Column(name = "email")
	private String email;
	
	@Column(name = "date_birth")
	private String dateOfBirth;
	
	@CreatedDate
	@Column(name = "create_time")
	private LocalDateTime createTime;
	
	@LastModifiedDate
	@Column(name = "update_time")
	private LocalDateTime updateTime;

	@Column(name="active")
	private Boolean active = true;
	
	private static final Logger logger = LoggerFactory.getLogger(Customer.class);
	public Customer() {
		
	}
	
	public Customer(String firstName, String lastName, String email, String dateOfBirth) {
		super();
		this.firstName = firstName;
		this.lastName = lastName;
		this.email = email;
		
		
		DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("MM-dd-uuuu", Locale.US)
			    .withResolverStyle(ResolverStyle.STRICT);
		DateValidator validator = new DateValidator(dateFormatter);
		if (validator.isValid(dateOfBirth)) {
			this.dateOfBirth = dateOfBirth;
		} else {
			 logger.info("Invalid date of birth");
		}
	}
	
	public Customer(String firstName, String lastName) {
		super();
		this.firstName = firstName;
		this.lastName = lastName;
	}
	
	
	
	public Long getId() {
		return id;
	}
	public void setId(Long id) {
		this.id = id;
	}
	public String getFirstName() {
		return firstName;
	}
	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}
	public String getLastName() {
		return lastName;
	}
	public void setLastName(String lastName) {
		this.lastName = lastName;
	}
	public String getEmail() {
		return email;
	}
	public void setEmail(String email) {
		this.email = email;
	}
	public String getDateOfBirth() {
		return dateOfBirth;
	}
	public void setDateOfBirth(String dateOfBirth) {
		this.dateOfBirth = dateOfBirth;
	}




	
}