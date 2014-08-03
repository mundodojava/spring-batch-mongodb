package br.com.javatar.batch;

import java.math.BigDecimal;

import javax.persistence.Id;

import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.domain.Persistable;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * Account model object
 */
@Document
@TypeAlias("account")
public class Account implements Persistable<String> {

	private static final long serialVersionUID = -3166540015278455392L;
	
	@Id
	private String id;
	
	private String accountHolderName;
	private String accountCurrency;
	private BigDecimal balance;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getAccountHolderName() {
		return accountHolderName;
	}

	public void setAccountHolderName(String accountHolderName) {
		this.accountHolderName = accountHolderName;
	}

	public String getAccountCurrency() {
		return accountCurrency;
	}

	public void setAccountCurrency(String accountCurrency) {
		this.accountCurrency = accountCurrency;
	}

	public BigDecimal getBalance() {
		return balance;
	}

	public void setBalance(BigDecimal balance) {
		this.balance = balance;
	}

	@Override
	public String toString() {
		return "Account [id=" + id + ", accountHolderName=" + accountHolderName
				+ ", accountCurrency=" + accountCurrency + ", balance="
				+ balance + "]";
	}

	@Override
	public boolean isNew() {
		return getId() == null;
	}
}