package com.viniville.rinhabackendwebflux.repository;

import com.viniville.rinhabackendwebflux.api.transacao.*;
import com.viniville.rinhabackendwebflux.exception.ClienteNaoExisteException;
import com.viniville.rinhabackendwebflux.exception.SaldoInsuficienteException;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

@Repository
@Transactional(readOnly = true)
public class TransacaoRepository {

    private static final String SQL_UPDATE_SALDO_CLIENTE = """
                WITH results AS (
                    UPDATE cliente
                    SET saldo = saldo + ($1)
                    WHERE id = $2
                    RETURNING limite, saldo
                )
                SELECT * FROM results
            """;

    private static final String SQL_INSERT_TRANSACAO = """
            INSERT INTO transacao (id_cliente, descricao, tipo, valor)
            VALUES($1, $2, $3, $4);
            """;

    private static final String SQL_EXTRATO = """
                select
                	c.saldo,
                	c.limite,
                	t.id,
                	t.valor,
                	t.tipo,
                	t.descricao,
                	t.realizada_em
                from
                	cliente c
                	left join transacao t on (t.id_cliente = c.id)
                where
                	c.id = :idCliente
                order by 
                    t.realizada_em desc
                limit 10
            """;

    private final DatabaseClient databaseClient;

    public TransacaoRepository(DatabaseClient databaseClient) {
        this.databaseClient = databaseClient;
    }

    @Transactional
    public Mono<TransacaoInsertResponse> registrarTransacao(TransacaoInsertRequest transacao) {
        return databaseClient
                .sql(SQL_UPDATE_SALDO_CLIENTE)
                .bind("$1", valorTransacaoParaSomaSaldo(transacao))
                .bind("$2", transacao.idCliente())
                .map(row -> new TransacaoInsertResponse(row.get("limite", Long.class), row.get("saldo", Long.class)))
                .first()
                .flatMap(resposta -> {
                    if (transacao.tipo().equals("d") && resposta.saldo() < 0 && ((-resposta.saldo()) > resposta.limite())) {
                        return Mono.error(new SaldoInsuficienteException("saldo insuficiente"));
                    } else {
                        return databaseClient
                                .sql(SQL_INSERT_TRANSACAO)
                                .bind("$1", transacao.idCliente())
                                .bind("$2", transacao.descricao())
                                .bind("$3", transacao.tipo())
                                .bind("$4", transacao.valor())
                                .fetch().rowsUpdated()
                                .thenReturn(resposta);
                    }
                });
    }

    public Mono<ExtratoResponse> extrato(Long idCliente) {
        return databaseClient
                .sql(SQL_EXTRATO)
                .bind("idCliente", idCliente)
                .map(row ->
                        new ExtratoResponseQuery(
                                row.get("saldo", Long.class),
                                OffsetDateTime.now(ZoneOffset.UTC),
                                row.get("limite", Long.class),
                                row.get("id", Long.class),
                                row.get("valor", Long.class),
                                row.get("tipo", String.class),
                                row.get("descricao", String.class),
                                row.get("realizada_em") == null ? null :
                                        row.get("realizada_em", LocalDateTime.class).atOffset(ZoneOffset.UTC)
                        )
                )
                .all()
                .collectList()
                .flatMap(saldoETransacoes -> {
                    if (saldoETransacoes.isEmpty()) {
                        return Mono.error(new ClienteNaoExisteException("Cliente não encontrado"));
                        //throw new ClienteNaoExisteException("Cliente não encontrado");
                    }
                    return Mono.just(ExtratoResponseQuery.toExtratoResponse(saldoETransacoes));
                });
    }

    private Long valorTransacaoParaSomaSaldo(TransacaoInsertRequest transacaoInsertRequest) {
        return "c".equals(transacaoInsertRequest.tipo()) ?
                transacaoInsertRequest.valor() : -transacaoInsertRequest.valor();
    }


    private record ExtratoResponseQuery(
            Long total,
            OffsetDateTime dataExtrato,
            Long limite,
            Long idTransacao,
            Long valor,
            String tipo,
            String descricao,
            OffsetDateTime realizadaEm
    ) {
        public static ExtratoResponse toExtratoResponse(List<ExtratoResponseQuery> list) {
            ExtratoResponseSaldo responseSaldo = null;
            List<ExtratoResponseTransacao> responseTransacoes = new ArrayList<>();
            for (ExtratoResponseQuery extratoResponseQuery : list) {
                if (responseSaldo == null) {
                    responseSaldo = list.getFirst().toSaldoResponse();
                }
                if (extratoResponseQuery.idTransacao() != null) {
                    responseTransacoes.add(extratoResponseQuery.toTransacaoResponse());
                }
            }
            return new ExtratoResponse(responseSaldo, responseTransacoes);
        }

        public ExtratoResponseSaldo toSaldoResponse() {
            return new ExtratoResponseSaldo(total, dataExtrato, limite);
        }

        public ExtratoResponseTransacao toTransacaoResponse() {
            return new ExtratoResponseTransacao(valor, tipo, descricao, realizadaEm);
        }

    }
}
