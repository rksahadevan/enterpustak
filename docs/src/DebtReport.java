package com.sapienter.jbilling.server.invoice.reportdto;

import java.math.BigDecimal;

public class DebtReport {

	BigDecimal current;
	BigDecimal thirtydays;
	BigDecimal sxtydays;
	BigDecimal nientydays;
	BigDecimal amount;
	BigDecimal overnientydays;
	public BigDecimal getCurrent() {
		return current;
	}
	public void setCurrent(BigDecimal current) {
		this.current = current;
	}
	public BigDecimal getThirtydays() {
		return thirtydays;
	}
	public void setThirtydays(BigDecimal thirtydays) {
		this.thirtydays = thirtydays;
	}
	public BigDecimal getSxtydays() {
		return sxtydays;
	}
	public void setSxtydays(BigDecimal sxtydays) {
		this.sxtydays = sxtydays;
	}
	public BigDecimal getNientydays() {
		return nientydays;
	}
	public void setNientydays(BigDecimal nientydays) {
		this.nientydays = nientydays;
	}
	public BigDecimal getAmount() {
		return amount;
	}
	public void setAmount(BigDecimal amount) {
		this.amount = amount;
	}
	public BigDecimal getOvernientydays() {
		return overnientydays;
	}
	public void setOvernientydays(BigDecimal overnientydays) {
		this.overnientydays = overnientydays;
	}
	public DebtReport(BigDecimal current, BigDecimal thirtydays,
			BigDecimal sxtydays, BigDecimal nientydays, BigDecimal amount,
			BigDecimal overnientydays) {
		super();
		this.current = current;
		this.thirtydays = thirtydays;
		this.sxtydays = sxtydays;
		this.nientydays = nientydays;
		this.amount = amount;
		this.overnientydays = overnientydays;
	}
	
}
