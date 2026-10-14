package dev.aryanyeole.ledgerapi.money;

import org.springframework.http.HttpStatus;

import dev.aryanyeole.ledgerapi.api.ApiException;
import dev.aryanyeole.ledgerapi.api.ProblemCode;

/** 400 invalid_amount (D-047). */
public class InvalidAmountException extends ApiException {

	InvalidAmountException(String detail) {
		super(HttpStatus.BAD_REQUEST, ProblemCode.INVALID_AMOUNT, detail);
	}

}
