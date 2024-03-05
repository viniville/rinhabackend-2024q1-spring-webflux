package com.viniville.rinhabackendwebflux.exception;

public class ClienteNaoExisteException extends RuntimeException {
    public ClienteNaoExisteException(String message) {
        super(message);
    }
}
