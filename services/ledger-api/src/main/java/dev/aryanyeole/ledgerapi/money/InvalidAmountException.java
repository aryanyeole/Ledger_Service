package dev.aryanyeole.ledgerapi.money;

import org.springframework.http.HttpStatus;

import dev.aryanyeole.ledgerapi.api.ApiException;

/** 400 invalid_amount (D-047). */
public class InvalidAmountException extends ApiException {

	public static final String CODE = "invalid_amount";

	InvalidAmountException(String detail) {
		super(HttpStatus.BAD_REQUEST, CODE, detail);
	}

}
