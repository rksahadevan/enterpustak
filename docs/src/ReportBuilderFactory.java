package com.sapienter.jbilling.server.invoice.reportdto;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class ReportBuilderFactory {

	public List<PaymentReport> getPaymentDetails() {
		List<PaymentReport> list = new ArrayList<PaymentReport>();
		list.add(new PaymentReport(new Date(), "credit memo", "Note - 1", new BigDecimal(50)));
		list.add(new PaymentReport(new Date(), "Net Banking", "Note - 2", new BigDecimal(60)));
		list.add(new PaymentReport(new Date(), "Cash", "Note - 3", new BigDecimal(70)));
		list.add(new PaymentReport(new Date(), "Credit Card", "Note - 4", new BigDecimal(80)));
		return list;
	
	}
	
	public List<DebtReport> getDebtList(){
		List<DebtReport> list = new ArrayList<DebtReport>();
		list.add(new DebtReport(new BigDecimal(1), new BigDecimal(2), new BigDecimal(3), new BigDecimal(4), new BigDecimal(5), new BigDecimal(6)));
		list.add(new DebtReport(new BigDecimal(1), new BigDecimal(2), new BigDecimal(3), new BigDecimal(4), new BigDecimal(5), new BigDecimal(6)));
		list.add(new DebtReport(new BigDecimal(1), new BigDecimal(2), new BigDecimal(3), new BigDecimal(4), new BigDecimal(5), new BigDecimal(6)));
		list.add(new DebtReport(new BigDecimal(1), new BigDecimal(2), new BigDecimal(3), new BigDecimal(4), new BigDecimal(5), new BigDecimal(6)));/*
		list.add(new DebtReport(new BigDecimal(1), new BigDecimal(2), new BigDecimal(3), new BigDecimal(4), new BigDecimal(5), new BigDecimal(6)));
		list.add(new DebtReport(new BigDecimal(1), new BigDecimal(2), new BigDecimal(3), new BigDecimal(4), new BigDecimal(5), new BigDecimal(6)));*/
		
		return list;
	}

}
