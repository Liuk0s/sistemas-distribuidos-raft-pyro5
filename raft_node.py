"""
raft_node.py — Implementação do algoritmo Raft com PyRO5.
"""

import sys
import random
import logging
import threading

import Pyro5.api
import Pyro5.server


# ---------------------------------------------------------------------------
# Configuração dos nós
# ---------------------------------------------------------------------------

NOS = {
    1: {"object_id": "raft.node1", "porta": 9091},
    2: {"object_id": "raft.node2", "porta": 9092},
    3: {"object_id": "raft.node3", "porta": 9093},
    4: {"object_id": "raft.node4", "porta": 9094},
}

# Uniform Resource Identifier
URIS = {}
for node_id in NOS:
    cfg = NOS[node_id]

    objeto_id = cfg["object_id"]
    num_porta = cfg["porta"]

    porta_string      = str(num_porta)
    endereco_completo = "PYRO:" + objeto_id + "@localhost:" + porta_string

    URIS[node_id] = endereco_completo 

# Nome do líder no servidor de nomes
NOME_LIDER = "raft.leader"

# Intervalo entre heartbeats do líder (segundos)
INTERVALO_HEARTBEAT = 0.5

# Faixa do timeout de eleição — aleatório para evitar candidatos simultâneos
TIMEOUT_ELEICAO_MIN = 1.5
TIMEOUT_ELEICAO_MAX = 3.0

# Estados possíveis de um nó
SEGUIDOR  = "SEGUIDOR"
CANDIDATO = "CANDIDATO"
LIDER     = "LIDER"


# ---------------------------------------------------------------------------
# Nó Raft
# ---------------------------------------------------------------------------

@Pyro5.api.expose
class NoRaft:

    def __init__(self, node_id):
        self.node_id = node_id
        self.lock    = threading.Lock()

        # Estado do nó
        self.estado      = SEGUIDOR
        self.termo_atual = 0
        self.votou_em    = None

        # Log de entradas: [{"termo": int, "comando": str}, ...]
        self.log = []

        # Índice da última entrada confirmada pela maioria
        self.commit_index = -1

        # Relacionado a "máquina de estados" no paper. Atualmente, sem uso.
        self.last_applied = -1

        # Progresso de replicação por seguidor (só usado quando líder)
        self.next_index  = {}
        self.match_index = {}

        # Timers
        self.timer_eleicao   = None
        self.timer_heartbeat = None

        # Começa contando o timeout de eleição
        self.reiniciar_timer_eleicao()

        logging.info("[Nó " + str(self.node_id) + "] Iniciado como SEGUIDOR | termo=0")


    # -----------------------------------------------------------------------
    # Timer de eleição
    # -----------------------------------------------------------------------

    def reiniciar_timer_eleicao(self):
        # Cancela o timer anterior
        if self.timer_eleicao is not None:
            self.timer_eleicao.cancel()

        # Sorteia um timeout aleatório e agenda a eleição
        timeout = random.uniform(TIMEOUT_ELEICAO_MIN, TIMEOUT_ELEICAO_MAX)
        self.timer_eleicao = threading.Timer(timeout, self.iniciar_eleicao)
        self.timer_eleicao.daemon = True
        self.timer_eleicao.start()


    # -----------------------------------------------------------------------
    # Transições de estado
    # -----------------------------------------------------------------------

    def virar_seguidor(self, termo):
        self.estado      = SEGUIDOR
        self.termo_atual = termo
        self.votou_em    = None
        self.reiniciar_timer_eleicao()
        logging.info("[Nó " + str(self.node_id) + "] -> SEGUIDOR | termo=" + str(termo))

    def virar_lider(self):
        self.estado = LIDER
        logging.info("[Nó " + str(self.node_id) + "] *** ELEITO LIDER *** | termo=" + str(self.termo_atual))

        # Líder não precisa de timer de eleição
        if self.timer_eleicao is not None:
            self.timer_eleicao.cancel()

        # Inicializa o progresso de replicação para cada seguidor
        for pid in URIS:
            if pid != self.node_id:
                self.next_index[pid]  = len(self.log)
                self.match_index[pid] = -1

        # Registra no servidor de nomes em thread separada
        t = threading.Thread(target=self.registrar_como_lider, daemon=True)
        t.start()

        # Começa a mandar heartbeats imediatamente
        self.enviar_heartbeats()


    # -----------------------------------------------------------------------
    # Eleição
    # -----------------------------------------------------------------------

    def iniciar_eleicao(self):
        self.lock.acquire()
        try:
            self.estado      = CANDIDATO
            self.termo_atual = self.termo_atual + 1
            self.votou_em    = self.node_id
            
            votos         = 1
            termo_eleicao = self.termo_atual
            ultimo_indice = len(self.log) - 1

            if len(self.log) > 0:
                ultimo_termo = self.log[ultimo_indice]["termo"]
            else:
                ultimo_termo = 0
        finally:
            self.lock.release()

        logging.info(
            "[Nó " + str(self.node_id) + "] Timeout expirou -> CANDIDATO | " +
            "termo=" + str(termo_eleicao) + " | voto proprio computado (total=1)"
        )

        lock_votos = threading.Lock()
        contagem   = [votos]

        def pedir_voto(peer_id):
            try:
                peer = Pyro5.api.Proxy(URIS[peer_id])
                try:
                    resultado      = peer.rpc_pedir_voto(termo_eleicao, self.node_id, ultimo_indice, ultimo_termo)
                    termo_resposta = resultado[0]
                    voto_concedido = resultado[1]
                finally:
                    peer._pyroRelease()

                lock_votos.acquire()
                try:
                    if termo_resposta > self.termo_atual:
                        self.lock.acquire()
                        try:
                            self.virar_seguidor(termo_resposta)
                        finally:
                            self.lock.release()
                        return

                    if voto_concedido:
                        contagem[0] = contagem[0] + 1
                        logging.info(
                            "[Nó " + str(self.node_id) + "] Recebeu voto do nó " +
                            str(peer_id) + " | total=" + str(contagem[0]) + "/" + str(len(URIS))
                        )
                    else:
                        logging.info(
                            "[Nó " + str(self.node_id) + "] Nó " + str(peer_id) + " negou o voto"
                        )
                finally:
                    lock_votos.release()

            except Exception as erro:
                logging.warning(
                    "[Nó " + str(self.node_id) + "] Não conseguiu contatar nó " +
                    str(peer_id) + ": " + str(erro)
                )

        threads = []
        for pid in URIS:
            if pid != self.node_id:
                t = threading.Thread(target=pedir_voto, args=(pid,), daemon=True)
                t.start()
                threads.append(t)

        for t in threads:
            t.join(timeout=2.0)

        self.lock.acquire()
        try:
            if self.estado != CANDIDATO or self.termo_atual != termo_eleicao:
                return

            maioria = int(len(URIS) / 2) + 1

            if contagem[0] >= maioria:
                logging.info(
                    "[Nó " + str(self.node_id) + "] Maioria atingida (" +
                    str(contagem[0]) + "/" + str(len(URIS)) + ", precisava de " + str(maioria) + ")"
                )
                self.virar_lider()
            else:
                logging.info(
                    "[Nó " + str(self.node_id) + "] Eleicao perdida (" +
                    str(contagem[0]) + "/" + str(len(URIS)) + " votos) | voltando a SEGUIDOR"
                )
                self.virar_seguidor(self.termo_atual)
        finally:
            self.lock.release()


    # -----------------------------------------------------------------------
    # Registro no servidor de nomes
    # -----------------------------------------------------------------------

    def registrar_como_lider(self):
        try:
            ns  = Pyro5.api.locate_ns()
            uri = URIS[self.node_id]

            try:
                ns.remove(NOME_LIDER)
            except Exception:
                pass

            ns.register(NOME_LIDER, uri)
            logging.info(
                "[Nó " + str(self.node_id) + "] Registrado no servidor de nomes " +
                "como '" + NOME_LIDER + "' -> " + uri
            )
        except Exception as erro:
            logging.error(
                "[Nó " + str(self.node_id) + "] Erro ao registrar no servidor de nomes: " + str(erro)
            )


    # -----------------------------------------------------------------------
    # Heartbeat e replicação
    # -----------------------------------------------------------------------

    def enviar_heartbeats(self):
        if self.estado != LIDER:
            return

        def heartbeat_para(peer_id):
            try:
                self.lock.acquire()
                try:
                    if self.estado != LIDER:
                        return
                    
                    # Pega o próximo índice de log que o seguidor espera receber
                    proximo = self.next_index[peer_id]

                    indice_anterior = proximo - 1
                    termo_anterior  = 0
                    tamanho_log     = len(self.log)

                    # Se existe uma entrada anterior a essa que vamos enviar, checa-se o seu termo
                    if indice_anterior >= 0 and tamanho_log > 0:
                        termo_anterior = self.log[indice_anterior]["termo"]

                    entradas = []
                    
                    for i in range(proximo, tamanho_log):
                        entradas.append(self.log[i])

                    termo      = self.termo_atual
                    commit_idx = self.commit_index
                finally:
                    self.lock.release()

                peer = Pyro5.api.Proxy(URIS[peer_id])
                try:
                    resultado = peer.rpc_append_entries(
                        termo,
                        self.node_id,
                        indice_anterior,
                        termo_anterior,
                        entradas,
                        commit_idx,
                    )
                    termo_resposta  = resultado[0]
                    sucesso         = resultado[1]
                    indice_seguidor = resultado[2]
                finally:
                    peer._pyroRelease()

                self.lock.acquire()
                try:
                    if termo_resposta > self.termo_atual:
                        self.virar_seguidor(termo_resposta)
                        return

                    if sucesso:
                        self.match_index[peer_id] = indice_seguidor
                        self.next_index[peer_id]  = indice_seguidor + 1
                        self.tentar_commit()
                    else:
                        self.next_index[peer_id] = indice_seguidor + 1
                finally:
                    self.lock.release()

            except Exception as erro:
                logging.warning(
                    "[Nó " + str(self.node_id) + "] Falha no heartbeat para nó " +
                    str(peer_id) + ": " + str(erro)
                )

        for pid in URIS:
            if pid != self.node_id:
                t = threading.Thread(target=heartbeat_para, args=(pid,), daemon=True)
                t.start()

        self.timer_heartbeat = threading.Timer(INTERVALO_HEARTBEAT, self.enviar_heartbeats)
        self.timer_heartbeat.daemon = True
        self.timer_heartbeat.start()


    # -----------------------------------------------------------------------
    # Commit
    # -----------------------------------------------------------------------

    def tentar_commit(self):
        # Deve ser chamada com self.lock adquirido
        maioria = int(len(URIS) / 2) + 1

        # Percorre o log de trás para frente, só acima do commit atual
        indice = len(self.log) - 1
        while indice > self.commit_index:

            # Conta quantos nós têm esta entrada
            total = 1  # o próprio líder sempre tem
            for peer_id in self.match_index:
                if self.match_index[peer_id] >= indice:
                    total = total + 1

            # Só efetiva entradas do termo atual (seção 5.4.2 do paper)
            entrada_do_termo_atual = False
            if self.log[indice]["termo"] == self.termo_atual:
                entrada_do_termo_atual = True

            if total >= maioria and entrada_do_termo_atual:
                self.commit_index = indice

                comandos = []
                for i in range(indice + 1):
                    comandos.append(self.log[i]["comando"])
                logging.info(
                    "[Nó " + str(self.node_id) + "] COMMIT -> índice=" +
                    str(indice) + " | log=" + str(comandos)
                )
                break

            indice = indice - 1


    # -----------------------------------------------------------------------
    # RPCs
    # -----------------------------------------------------------------------

    def rpc_pedir_voto(self, termo, candidato_id, ultimo_indice, ultimo_termo):
        self.lock.acquire()
        try:
            # Candidato com termo mais velho: recusa
            if termo < self.termo_atual:
                return (self.termo_atual, False)

            # Candidato com termo maior: atualiza e vira seguidor
            if termo > self.termo_atual:
                self.virar_seguidor(termo)

            # Já votamos em outro candidato neste termo?
            if self.votou_em is not None and self.votou_em != candidato_id:
                return (self.termo_atual, False)

            # O log do candidato é tão atual quanto o nosso?
            if len(self.log) > 0:
                nosso_ultimo_termo = self.log[len(self.log) - 1]["termo"]
            else:
                nosso_ultimo_termo = 0

            nosso_ultimo_indice  = len(self.log) - 1
            candidato_atualizado = False

            if ultimo_termo > nosso_ultimo_termo:
                candidato_atualizado = True
            elif ultimo_termo == nosso_ultimo_termo and ultimo_indice >= nosso_ultimo_indice:
                candidato_atualizado = True

            if not candidato_atualizado:
                return (self.termo_atual, False)

            # Concede o voto
            self.votou_em = candidato_id
            self.reiniciar_timer_eleicao()
            logging.info(
                "[Nó " + str(self.node_id) + "] Votou no candidato " +
                str(candidato_id) + " | termo=" + str(termo)
            )
            return (self.termo_atual, True)
        finally:
            self.lock.release()


    def rpc_append_entries(self, termo, lider_id,
                       indice_anterior, termo_anterior,
                       entradas, lider_commit):
        self.lock.acquire()
        try:
            # Líder desatualizado: rejeita e informa nosso termo atual
            if termo < self.termo_atual:
                return (self.termo_atual, False, len(self.log) - 1)

            # Líder legítimo com termo maior: vira seguidor
            # Se candidato, também volta a ser seguidor (líder já eleito)
            if termo > self.termo_atual or self.estado == CANDIDATO:
                self.virar_seguidor(termo)
            else:
                # Mesmo termo, só reinicia o timer pra não iniciar eleição
                self.reiniciar_timer_eleicao()

            self.estado = SEGUIDOR

            if indice_anterior >= 0:
                # O líder aponta pra uma entrada que ainda não tem: log incompleto
                if indice_anterior >= len(self.log):
                    return (self.termo_atual, False, len(self.log) - 1)

                # Tem a entrada mas o termo não bate: log divergiu
                # Trunca tudo a partir do ponto conflitante
                if self.log[indice_anterior]["termo"] != termo_anterior:
                    while len(self.log) > indice_anterior:
                        self.log.pop()
                    return (self.termo_atual, False, indice_anterior - 1)

            # Aplica as novas entradas enviadas pelo líder
            for i in range(len(entradas)):
                posicao = indice_anterior + 1 + i

                if posicao < len(self.log):
                    # Já há uma entrada nessa posição: verifica se conflita
                    if self.log[posicao]["termo"] != entradas[i]["termo"]:
                        # Termos diferentes: trunca a partir do conflito e adiciona a do líder
                        while len(self.log) > posicao:
                            self.log.pop()
                        self.log.append(entradas[i])
                    # Se os termos são iguais, a entrada já é a correta: não faz nada
                else:
                    # Posição nova, além do tamanho atual: adiciona diretamente
                    self.log.append(entradas[i])

            # Loga o estado do log após a atualização
            if len(entradas) > 0:
                comandos = []
                for entrada in self.log:
                    comandos.append(entrada["comando"])
                logging.info("[Nó " + str(self.node_id) + "] Log atualizado: " + str(comandos))

            # Avança o commit_index se o líder confirmou mais entradas
            if lider_commit > self.commit_index:
                # Não avança além do que realmente temos no log
                if lider_commit < len(self.log) - 1:
                    self.commit_index = lider_commit
                else:
                    self.commit_index = len(self.log) - 1
                logging.info(
                    "[Nó " + str(self.node_id) + "] COMMIT (do líder) -> índice=" +
                    str(self.commit_index)
                )

            return (self.termo_atual, True, len(self.log) - 1)
        finally:
            self.lock.release()

    def rpc_comando_cliente(self, comando):
        """
        Chamado pelo cliente para enviar um comando ao cluster.
        Só o líder aceita.
        """
        with self.lock:
            if self.estado != LIDER:
                return False, "Não sou o líder. Consulte o servidor de nomes."

            entrada = {"termo": self.termo_atual, "comando": comando}
            self.log.append(entrada)

            logging.info(
                "[Nó " + str(self.node_id) + "] Comando recebido: '" +
                comando + "' (índice=" + str(len(self.log) - 1) + ")"
            )
            return True, (
                "Comando '" + comando + "' adicionado " +
                "(índice=" + str(len(self.log) - 1) + ", termo=" + str(self.termo_atual) + ")"
            )


    def rpc_status(self):
        """Retorna o estado atual do nó."""
        with self.lock:
            return {
                "node_id":      self.node_id,
                "estado":       self.estado,
                "termo":        self.termo_atual,
                "votou_em":     self.votou_em,
                "log":          list(self.log),
                "commit_index": self.commit_index,
            }


# ---------------------------------------------------------------------------
# Inicialização
# ---------------------------------------------------------------------------

def main():
    if len(sys.argv) != 2 or not sys.argv[1].isdigit():
        print("Uso: python raft_node.py <node_id>")
        print("     node_id deve ser um de: " + str(list(NOS.keys())))
        sys.exit(1)

    node_id = int(sys.argv[1])
    if node_id not in NOS:
        print("node_id inválido. Use um de: " + str(list(NOS.keys())))
        sys.exit(1)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    cfg    = NOS[node_id]
    porta  = cfg["porta"]
    obj_id = cfg["object_id"]

    daemon = Pyro5.server.Daemon(port=porta)

    no  = NoRaft(node_id)
    uri = daemon.register(no, objectId=obj_id)
    logging.info("[Nó " + str(node_id) + "] URI: " + str(uri))

    try:
        ns = Pyro5.api.locate_ns()
        try:
            ns.remove(obj_id)
        except Exception:
            pass
        ns.register(obj_id, uri)
        logging.info("[Nó " + str(node_id) + "] Registrado no servidor de nomes como '" + obj_id + "'")
    except Exception as erro:
        logging.warning("[Nó " + str(node_id) + "] Servidor de nomes indisponível: " + str(erro))

    logging.info("[Nó " + str(node_id) + "] Aguardando requisições...")
    daemon.requestLoop()


if __name__ == "__main__":
    main()
