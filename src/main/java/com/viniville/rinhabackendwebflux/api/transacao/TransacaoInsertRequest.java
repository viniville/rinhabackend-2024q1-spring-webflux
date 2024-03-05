package com.viniville.rinhabackendwebflux.api.transacao;

import com.fasterxml.jackson.annotation.JsonProperty;

public record TransacaoInsertRequest (
        @JsonProperty(access = JsonProperty.Access.READ_ONLY) Long idCliente,
        Long valor,
        String tipo,
        String descricao
)
{
    public TransacaoInsertRequest withIdCliente(Long idCliente) {
        return new TransacaoInsertRequest(idCliente, valor, tipo, descricao);
    }
}