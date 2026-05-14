"""
raft_node.py: Implementação do algoritmo de consenso Raft usando PyRO5.

Doc: Raft (https://raft.github.io/raft.pdf)

Uso:
    python raft_node.py <node_id>   (node_id: 1, 2, 3 ou 4)

Cada nó cria um Daemon PyRO na porta fixada em NODE_CONFIG e se registra
no servidor de nomes do PyRO. As URIs resultantes ficam hard-coded:
    PYRO:raft.node1@localhost:9091
    PYRO:raft.node2@localhost:9092
    PYRO:raft.node3@localhost:9093
    PYRO:raft.node4@localhost:9094
"""

import sys
# import time
import random
import logging
import threading

import Pyro5.api
import Pyro5.server
# import Pyro5.errors

# ---------------------------------------------------------------------------
# Configuração dos nós
# ---------------------------------------------------------------------------

# Mapeamento node_id -> {objectId, porta}
NODE_CONFIG = {
    1: {"object_id": "raft.node1", "port": 9091},
    2: {"object_id": "raft.node2", "port": 9092},
    3: {"object_id": "raft.node3", "port": 9093},
    4: {"object_id": "raft.node4", "port": 9094},
}

# URIs resultantes (PYRO:objectId@localhost:porta): usadas para contato direto
NODE_URIS = {
    nid: f"PYRO:{cfg['object_id']}@localhost:{cfg['port']}"
    for nid, cfg in NODE_CONFIG.items()
}

# Nome registrado no servidor de nomes para identificar o líder atual
LEADER_NS_NAME = "raft.leader"

# ---------------------------------------------------------------------------
# Parâmetros de tempo
# ---------------------------------------------------------------------------

# Intervalo entre heartbeats enviados pelo líder (segundos)
HEARTBEAT_INTERVAL = 0.5

# Faixa do timeout de eleição (aleatório para evitar candidatos simultâneos)
ELECTION_TIMEOUT_MIN = 1.5
ELECTION_TIMEOUT_MAX = 3.0

# ---------------------------------------------------------------------------
# Estados possíveis de um nó Raft
# ---------------------------------------------------------------------------
FOLLOWER  = "FOLLOWER"
CANDIDATE = "CANDIDATE"
LEADER    = "LEADER"


# ---------------------------------------------------------------------------
# Classe principal do nó Raft
# ---------------------------------------------------------------------------

@Pyro5.api.expose          # expõe todos os métodos públicos via PyRO
class RaftNode:
    """
    Representa um nó participante do cluster Raft.
    """

    def __init__(self, node_id: int):
        self.node_id = node_id
        self.lock    = threading.Lock()

        # --- Estado persistente ---
        self.current_term = 0     # sempre crescente
        self.voted_for    = None  # a quem votam no termo atual
        self.log          = []    # entradas: [{"term": int, "command": str}, ...]

        # --- Estado volátil ---
        self.state        = FOLLOWER  # todo nó começa como seguidor
        self.commit_index = -1        # maior índice de entrada efetivada
        self.last_applied = -1        # maior índice aplicado à máquina de estados

        # --- Estado volátil do líder (reiniciado a cada eleição ganha) ---
        self.next_index  = {}   # next_index[peer]:  próxima entrada a enviar para aquele seguidor
        self.match_index = {}   # match_index[peer]: maior entrada confirmada naquele seguidor

        # --- Timers ---
        self._election_timer  = None
        self._heartbeat_timer = None
        self._reset_election_timeout()
        self._start_election_timer()   # começa contando o timeout

        logging.info(f"[Nó {self.node_id}] Iniciado como FOLLOWER | termo=0")

    # =======================================================================
    # Utilitários internos
    # =======================================================================

    def _reset_election_timeout(self):
        """Sorteia um novo timeout de eleição dentro da faixa configurada."""
        self.election_timeout = random.uniform(
            ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX
        )

    def _start_election_timer(self):
        """
        Cancela o timer anterior (se existir) e agenda um novo.
        Quando disparar, o nó tentará iniciar uma eleição.
        """
        if self._election_timer is not None:
            self._election_timer.cancel()
        self._reset_election_timeout()
        self._election_timer = threading.Timer(
            self.election_timeout, self._start_election
        )
        self._election_timer.daemon = True
        self._election_timer.start()

    def _last_log_index(self) -> int:
        """Índice da última entrada do log (-1 se vazio)."""
        return len(self.log) - 1

    def _last_log_term(self) -> int:
        """Termo da última entrada do log (0 se vazio)."""
        return self.log[-1]["term"] if self.log else 0

    def _become_follower(self, term: int):
        """
        Transição para FOLLOWER.
        Chamada quando descobre-se um termo maior (de qualquer mensagem recebida).
        Reseta votos e reinicia o timer de eleição.
        """
        self.state        = FOLLOWER
        self.current_term = term
        self.voted_for    = None
        self._start_election_timer()
        logging.info(f"[Nó {self.node_id}] -> FOLLOWER | termo={term}")

    # =======================================================================
    # Eleição de líder
    # =======================================================================

    def _start_election(self):
        """
        Disparado quando o timer de eleição expira sem receber heartbeat.
        O nó incrementa o termo, se candidata, vota em si mesmo
        e solicita votos a todos os outros nós em paralelo.
        """
        with self.lock:
            self.state         = CANDIDATE
            self.current_term += 1
            self.voted_for     = self.node_id
            votes_recebidos    = 1          # voto próprio
            termo_da_eleicao   = self.current_term
            ultimo_idx         = self._last_log_index()
            ultimo_termo       = self._last_log_term()

        logging.info(
            f"[Nó {self.node_id}] Timeout expirou -> CANDIDATE | termo={termo_da_eleicao} "
            f"| voto próprio computado (total=1)"
        )

        # Coleta votos em paralelo (um thread por peer)
        contagem_lock = threading.Lock()
        contagem      = [votes_recebidos]   # lista para mutabilidade em closure

        def pedir_voto(peer_id):
            try:
                with Pyro5.api.Proxy(NODE_URIS[peer_id]) as peer:
                    termo_resp, voto = peer.request_vote(
                        termo_da_eleicao,
                        self.node_id,
                        ultimo_idx,
                        ultimo_termo,
                    )
                with contagem_lock:
                    if termo_resp > self.current_term:
                        # Peer tem termo maior; abandona a candidatura
                        with self.lock:
                            self._become_follower(termo_resp)
                        return
                    if voto:
                        contagem[0] += 1
                        logging.info(
                            f"[Nó {self.node_id}] Recebeu voto do nó {peer_id} "
                            f"| total={contagem[0]}/{len(NODE_URIS)}"
                        )
                    else:
                        logging.info(
                            f"[Nó {self.node_id}] Nó {peer_id} negou o voto"
                        )
            except Exception as exc:
                logging.warning(
                    f"[Nó {self.node_id}] Não conseguiu votar de nó {peer_id}: {exc}"
                )

        threads = [
            threading.Thread(target=pedir_voto, args=(pid,), daemon=True)
            for pid in NODE_URIS if pid != self.node_id
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=2.0)   # aguarda respostas

        with self.lock:
            # Só assume liderança se ainda for candidato no mesmo termo
            if self.state != CANDIDATE or self.current_term != termo_da_eleicao:
                return

            maioria = len(NODE_URIS) // 2 + 1
            if contagem[0] >= maioria:
                logging.info(
                    f"[Nó {self.node_id}] Maioria atingida "
                    f"({contagem[0]}/{len(NODE_URIS)}, precisava de {maioria})"
                )
                self._become_leader()
            else:
                logging.info(
                    f"[Nó {self.node_id}] Eleição perdida "
                    f"({contagem[0]}/{len(NODE_URIS)} votos) | voltando a FOLLOWER"
                )
                self._become_follower(self.current_term)

    def _become_leader(self):
        """
        Assume a liderança após obter maioria dos votos.
        Inicializa os índices de progresso dos seguidores,
        registra-se no servidor de nomes e inicia os heartbeats.
        """
        self.state = LEADER
        logging.info(
            f"[Nó {self.node_id}] *** ELEITO LÍDER *** | termo={self.current_term}"
        )

        # Cancela o timer de eleição: líder não precisa esperar heartbeat
        if self._election_timer:
            self._election_timer.cancel()

        # Reinicia os contadores por seguidor
        for pid in NODE_URIS:
            if pid != self.node_id:
                self.next_index[pid]  = len(self.log)   # otimista: começa do fim
                self.match_index[pid] = -1               # nada confirmado ainda

        # Registra no servidor de nomes (thread separada para não bloquear)
        threading.Thread(target=self._registrar_lider_no_ns, daemon=True).start()

        # Inicia ciclo de heartbeats
        self._enviar_heartbeats()

    def _registrar_lider_no_ns(self):
        """
        Registra (ou sobrescreve) a entrada 'raft.leader' no servidor de nomes
        com a URI deste nó, para que o cliente consiga encontrar o líder atual.
        """
        try:
            ns = Pyro5.api.locate_ns()
            uri = NODE_URIS[self.node_id]
            try:
                ns.remove(LEADER_NS_NAME)   # remove entrada anterior (2ª eleição em diante)
            except Exception:
                pass
            ns.register(LEADER_NS_NAME, uri)
            logging.info(
                f"[Nó {self.node_id}] Registrado no NS como '{LEADER_NS_NAME}' -> {uri}"
            )
        except Exception as exc:
            logging.error(f"[Nó {self.node_id}] Erro ao registrar no NS: {exc}")

    # =======================================================================
    # Heartbeat e replicação iniciada pelo líder
    # =======================================================================

    def _enviar_heartbeats(self):
        """
        Envia AppendEntries para todos os seguidores periodicamente.
        Se entries=[], funciona como heartbeat puro (mantém autoridade do líder).
        Se há entradas novas pendentes para um seguidor, elas são incluídas.
        Reagenda a si mesmo enquanto o nó permanecer líder.
        """
        if self.state != LEADER:
            return   # para se perde a liderança

        def heartbeat_para(peer_id):
            try:
                # Captura o estado atual do log para este peer (com lock)
                with self.lock:
                    if self.state != LEADER:
                        return
                    ni         = self.next_index.get(peer_id, len(self.log))
                    prev_idx   = ni - 1
                    prev_term  = self.log[prev_idx]["term"] if prev_idx >= 0 and self.log else 0
                    entradas   = list(self.log[ni:])    # cópia para envio
                    termo      = self.current_term
                    commit_idx = self.commit_index

                # Chamada PyRO fora do lock (operação de rede pode demorar)
                with Pyro5.api.Proxy(NODE_URIS[peer_id]) as peer:
                    termo_resp, sucesso = peer.append_entries(
                        termo, self.node_id,
                        prev_idx, prev_term,
                        entradas, commit_idx,
                    )

                # Processa resposta (com lock)
                with self.lock:
                    if termo_resp > self.current_term:
                        # Seguidor conhece termo maior; perde a liderança
                        self._become_follower(termo_resp)
                        return

                    if sucesso:
                        # Atualiza progresso: seguidor confirmou até prev_idx + len(entradas)
                        novo_match = prev_idx + len(entradas)
                        self.match_index[peer_id] = novo_match
                        self.next_index[peer_id]  = novo_match + 1
                        self._tentar_commit()
                    else:
                        # Inconsistência de log: recua next_index e tentará novamente
                        self.next_index[peer_id] = max(0, ni - 1)

            except Exception as exc:
                logging.warning(
                    f"[Nó {self.node_id}] Falha no heartbeat para nó {peer_id}: {exc}"
                )

        # Dispara heartbeats para todos os peers em paralelo
        threads = [
            threading.Thread(target=heartbeat_para, args=(pid,), daemon=True)
            for pid in NODE_URIS if pid != self.node_id
        ]
        for t in threads:
            t.start()

        # Reagenda o próximo ciclo de heartbeats
        self._heartbeat_timer = threading.Timer(
            HEARTBEAT_INTERVAL, self._enviar_heartbeats
        )
        self._heartbeat_timer.daemon = True
        self._heartbeat_timer.start()

    def _tentar_commit(self):
        """
        Verifica se alguma entrada ainda não efetivada já foi replicada
        na maioria dos nós e, se sim, avança o commit_index.
        """
        maioria = len(NODE_URIS) // 2 + 1

        # Procura do fim do log para o primeiro candidato a commit
        for n in range(len(self.log) - 1, self.commit_index, -1):
            # Conta nós que têm esta entrada (líder + seguidores confirmados)
            replicado = 1 + sum(
                1 for mi in self.match_index.values() if mi >= n
            )
            if replicado >= maioria and self.log[n]["term"] == self.current_term:
                self.commit_index = n
                logging.info(
                    f"[Nó {self.node_id}] COMMIT -> índice={n} | "
                    f"log={[e['command'] for e in self.log[:n+1]]}"
                )
                break   # o próximo heartbeat propagará o novo commit_index

    # =======================================================================
    # RPCs expostos via PyRO
    # =======================================================================

    def request_vote(self, term: int, candidate_id: int,
                     last_log_index: int, last_log_term: int):
        """
        RPC chamado por um candidato solicitando voto.
        """
        with self.lock:
            # Candidato com termo desatualizado: recusa
            if term < self.current_term:
                return self.current_term, False

            # Candidato com termo maior: atualiza e vira seguidor
            if term > self.current_term:
                self._become_follower(term)

            # Já vota em outro candidato neste termo?
            if self.voted_for is not None and self.voted_for != candidate_id:
                return self.current_term, False

            # O log do candidato é pelo menos tão atual quanto o nosso?
            # (critério "up-to-date" - seção 5.4.1 do paper Raft)
            candidato_atualizado = (
                last_log_term > self._last_log_term()
                or (
                    last_log_term == self._last_log_term()
                    and last_log_index >= self._last_log_index()
                )
            )
            if not candidato_atualizado:
                return self.current_term, False

            # Tudo certo: concede o voto
            self.voted_for = candidate_id
            self._start_election_timer()   # reseta timer ao votar
            logging.info(
                f"[Nó {self.node_id}] Votou no candidato {candidate_id} | termo={term}"
            )
            return self.current_term, True

    def append_entries(self, term: int, leader_id: int,
                       prev_log_index: int, prev_log_term: int,
                       entries: list, leader_commit: int):
        """
        RPC do líder para replicar entradas de log ou enviar heartbeat.
        Quando entries=[], trata-se de um heartbeat (sem novas entradas).
        """
        with self.lock:
            # Líder com termo desatualizado: recusa
            if term < self.current_term:
                return self.current_term, False

            # Líder legítimo (term >= current_term): reseta timer de eleição
            if term > self.current_term or self.state == CANDIDATE:
                self._become_follower(term)
            else:
                # Mesmo termo: apenas reinicia o timer (sem mudar voted_for)
                self._start_election_timer()

            # Garante que está como seguidor (pode ter sido candidato)
            self.state = FOLLOWER

            # --- Verificação de consistência do log (seção 5.3) ---
            if prev_log_index >= 0:
                # Não tem a entrada em prev_log_index?
                if prev_log_index >= len(self.log):
                    return self.current_term, False
                # Tem, mas com termo diferente? Conflito.
                if self.log[prev_log_index]["term"] != prev_log_term:
                    # Trunca o log a partir do ponto conflitante
                    self.log = self.log[:prev_log_index]
                    return self.current_term, False

            # --- Adiciona/sobrescreve entradas novas ---
            for i, entry in enumerate(entries):
                # Índice absoluto desta entrada no log
                idx = prev_log_index + 1 + i
                if idx < len(self.log):
                    # Entrada existe: verifica conflito de termo
                    if self.log[idx]["term"] != entry["term"]:
                        self.log = self.log[:idx]    # trunca a partir daqui
                        self.log.append(entry)
                else:
                    self.log.append(entry)           # nova entrada

            if entries:
                logging.info(
                    f"[Nó {self.node_id}] Log atualizado: "
                    f"{[e['command'] for e in self.log]}"
                )

            # --- Atualiza commit_index se o líder avançou ---
            if leader_commit > self.commit_index:
                self.commit_index = min(leader_commit, len(self.log) - 1)
                logging.info(
                    f"[Nó {self.node_id}] COMMIT (do líder) -> índice={self.commit_index}"
                )

            return self.current_term, True

    def client_request(self, command: str):
        """
        RPC chamado pelo cliente para submeter um novo comando ao cluster.

        Somente o líder pode aceitar comandos.
        Seguidores rejeitam e informam quem (acham que) é o líder.
        """
        with self.lock:
            if self.state != LEADER:
                return False, "Este nó não é o lider."

            # Anexa ao log; a replicação ocorrerá no próximo ciclo de heartbeat
            entry = {"term": self.current_term, "command": command}
            self.log.append(entry)
            logging.info(
                f"[Nó {self.node_id}] Comando recebido do cliente: '{command}' "
                f"(índice={len(self.log) - 1})"
            )
            return True, (
                f"Comando '{command}' adicionado ao log "
                f"(índice={len(self.log) - 1}, termo={self.current_term})"
            )

    def get_status(self):
        """
        Retorna um dicionário com o estado atual do nó.
        Útil para diagnóstico e comparação entre nós ao final da execução.
        """
        with self.lock:
            return {
                "node_id":      self.node_id,
                "state":        self.state,
                "term":         self.current_term,
                "voted_for":    self.voted_for,
                "log":          list(self.log),
                "commit_index": self.commit_index,
            }


# ---------------------------------------------------------------------------
# Ponto de entrada
# ---------------------------------------------------------------------------

def main():
    if len(sys.argv) != 2 or not sys.argv[1].isdigit():
        print("Uso: python raft_node.py <node_id>")
        print(f"     node_id deve ser um de: {list(NODE_CONFIG.keys())}")
        sys.exit(1)

    node_id = int(sys.argv[1])
    if node_id not in NODE_CONFIG:
        print(f"node_id inválido. Use um de: {list(NODE_CONFIG.keys())}")
        sys.exit(1)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    cfg    = NODE_CONFIG[node_id]
    porta  = cfg["port"]
    obj_id = cfg["object_id"]

    # Cria o Daemon PyRO na porta configurada para este nó
    daemon = Pyro5.server.Daemon(port=porta)

    # Instancia o nó e registra no Daemon com o objectId pré-definido
    # -> gera a URI: PYRO:obj_id@localhost:porta
    node = RaftNode(node_id)
    uri  = daemon.register(node, objectId=obj_id)
    logging.info(f"[Nó {node_id}] URI: {uri}")

    # Também registra no servidor de nomes (para localização geral)
    try:
        ns = Pyro5.api.locate_ns()
        try:
            ns.remove(obj_id)
        except Exception:
            pass
        ns.register(obj_id, uri)
        logging.info(f"[Nó {node_id}] Registrado no NS como '{obj_id}'")
    except Exception as exc:
        logging.warning(f"[Nó {node_id}] Servidor de nomes indisponível: {exc}")

    logging.info(f"[Nó {node_id}] Aguardando requisições...")
    daemon.requestLoop()


if __name__ == "__main__":
    main()
