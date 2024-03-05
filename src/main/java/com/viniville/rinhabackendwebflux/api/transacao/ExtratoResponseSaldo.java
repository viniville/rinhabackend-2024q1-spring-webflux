package com.viniville.rinhabackendwebflux.api.transacao;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.OffsetDateTime;

public record ExtratoResponseSaldo(
        Long total,
        @JsonProperty("data_extrato") OffsetDateTime dataExtrato,
        Long limite
) {}
