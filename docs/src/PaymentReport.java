package com.sapienter.jbilling.server.invoice.reportdto;

import java.math.BigDecimal;
import java.util.Date;

public class PaymentReport {
	Date date;
	String method;
	String notes;
	BigDecimal amount;
	
	public PaymentReport(Date date, String method, String notes,
			BigDecimal amount) {
		super();
		this.date = date;
		this.method = method;
		this.notes = notes;
		this.amount = amount;
	}
	public Date getDate() {
		return date;
	}
	public void setDate(Date date) {
		this.date = date;
	}
	public String getMethod() {
		return method;
	}
	public void setMethod(String method) {
		this.method = method;
	}
	public String getNotes() {
		return notes;
	}
	public void setNotes(String notes) {
		this.notes = notes;
	}
	public BigDecimal getAmount() {
		return amount;
	}
	public void setAmount(BigDecimal amount) {
		this.amount = amount;
	}
	
}
