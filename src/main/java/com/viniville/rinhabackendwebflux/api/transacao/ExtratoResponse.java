package com.viniville.rinhabackendwebflux.api.transacao;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public record ExtratoResponse(
        ExtratoResponseSaldo saldo,
        @JsonProperty("ultimas_transacoes") List<ExtratoResponseTransacao> ultimasTransacoes
) { }

