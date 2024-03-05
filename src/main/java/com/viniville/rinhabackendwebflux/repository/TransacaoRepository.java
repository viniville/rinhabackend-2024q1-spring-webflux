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

    private static final String SQL_UPDATE_SALDO_AND_INSERT_TRANSACAO = """
                select * from registrar_transacao(:idCliente, :tipo, :descricao, :valor)
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
                    t.id desc
                limit 10
            """;

    private final DatabaseClient databaseClient;

    public TransacaoRepository(DatabaseClient databaseClient) {
        this.databaseClient = databaseClient;
    }

    @Transactional
    public Mono<TransacaoInsertResponse> registrarTransacao(TransacaoInsertRequest transacao) {
        return databaseClient
                .sql(SQL_UPDATE_SALDO_AND_INSERT_TRANSACAO)
                .bind("idCliente", transacao.idCliente())
                .bind("tipo", transacao.tipo())
                .bind("descricao", transacao.descricao())
                .bind("valor", transacao.valor())
                .map(row -> new RegistroTransacaoResponseQuery(
                        row.get("out_limite_cliente", Long.class),
                        row.get("out_novo_saldo_cliente", Long.class),
                        row.get("out_status", String.class)))
                .first()
                .flatMap(resposta -> {
                    if ("CI".equalsIgnoreCase(resposta.status())) {
                        return Mono.error(new ClienteNaoExisteException("Cliente não encontrado"));
                    }
                    if ("SI".equalsIgnoreCase(resposta.status())) {
                        return Mono.error(new SaldoInsuficienteException("Saldo insuficiente"));
                    }
                    return Mono.just(new TransacaoInsertResponse(resposta.limite(), resposta.saldo()));
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
                    }
                    return Mono.just(ExtratoResponseQuery.toExtratoResponse(saldoETransacoes));
                });
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

    private record RegistroTransacaoResponseQuery(
            Long limite,
            Long saldo,
            String status
    ) { }
}
