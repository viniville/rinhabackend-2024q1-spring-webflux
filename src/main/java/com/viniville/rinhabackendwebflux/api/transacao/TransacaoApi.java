package com.viniville.rinhabackendwebflux.api.transacao;

import com.viniville.rinhabackendwebflux.exception.ValidacaoRegistrarTransacaoException;
import com.viniville.rinhabackendwebflux.repository.TransacaoRepository;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.util.List;

@RestController
@RequestMapping("/clientes")
public class TransacaoApi {

    private final TransacaoRepository transacaoRepository;

    public TransacaoApi(TransacaoRepository transacaoRepository) {
        this.transacaoRepository = transacaoRepository;
    }

    @PostMapping("/{id}/transacoes")
    public Mono<TransacaoInsertResponse> registrarTransacao(@PathVariable("id") Long idCliente, @RequestBody TransacaoInsertRequest transacaoInsertRequest) {
        validaInputTransacao(transacaoInsertRequest);
        return transacaoRepository.registrarTransacao(transacaoInsertRequest.withIdCliente(idCliente));
    }

    @GetMapping("/{id}/extrato")
    public Mono<ExtratoResponse> extrato(@PathVariable("id") Long idCliente) {
        return transacaoRepository.extrato(idCliente);
    }

    private void validaInputTransacao(TransacaoInsertRequest requestData) {
        if (requestData.valor() == null || requestData.valor() <= 0) {
            throw new ValidacaoRegistrarTransacaoException("Valor deve ser informado e positivo");
        }
        if (requestData.tipo() == null || !List.of("c", "d").contains(requestData.tipo())) {
            throw new ValidacaoRegistrarTransacaoException("Tipo de transação informada é inválido");
        }
        if (requestData.descricao() == null || requestData.descricao().isBlank() || requestData.descricao().strip().length() > 10) {
            throw new ValidacaoRegistrarTransacaoException("Descrição da transação deve ser informada e ter tamanho máximo de 10 caractéres");
        }
    }

}
