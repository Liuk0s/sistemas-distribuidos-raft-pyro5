"""
client.py: Cliente do cluster Raft.
"""

import time
import logging

import Pyro5.api
import Pyro5.errors

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

# Nome sob o qual o líder atual se registra no servidor de nomes
LEADER_NS_NAME = "raft.leader"

# URIs hard-coded (para exibir status de cada nó ao final)
NODE_URIS = {
    1: "PYRO:raft.node1@localhost:9091",
    2: "PYRO:raft.node2@localhost:9092",
    3: "PYRO:raft.node3@localhost:9093",
    4: "PYRO:raft.node4@localhost:9094",
}

# Comandos que o cliente irá enviar ao cluster (simulam operações numa KV-store)
COMANDOS = [
    "SET x=10",
    "SET y=20",
    "SET z=30",
    "DEL x",
    "SET w=99",
    "SET x=42",
]

# Segundos de espera inicial para o cluster eleger um líder
ESPERA_INICIAL = 6

# Número de tentativas antes de desistir de enviar um comando
MAX_TENTATIVAS = 5

# ---------------------------------------------------------------------------
# Funções auxiliares
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [Cliente] %(message)s",
    datefmt="%H:%M:%S",
)


def localizar_lider() -> str | None:
    """
    Consulta o servidor de nomes e retorna a URI do líder atual.
    Retorna None se o líder ainda não estiver registrado.
    """
    try:
        ns = Pyro5.api.locate_ns()
        uri = ns.lookup(LEADER_NS_NAME)
        return str(uri)
    except Pyro5.errors.NamingError:
        return None   # líder ainda não registrado
    except Exception as exc:
        logging.warning(f"Erro ao contatar servidor de nomes: {exc}")
        return None


def enviar_comando(comando: str) -> bool:
    """
    Localiza o líder e envia um comando.
    Tenta até MAX_TENTATIVAS vezes, aguardando entre cada tentativa.
    """
    for tentativa in range(1, MAX_TENTATIVAS + 1):
        uri_lider = localizar_lider()

        if uri_lider is None:
            logging.warning(
                f"Líder não encontrado no NS. "
                f"Aguardando 2s... (tentativa {tentativa}/{MAX_TENTATIVAS})"
            )
            time.sleep(2)
            continue

        logging.info(f"Líder em: {uri_lider}")
        try:
            with Pyro5.api.Proxy(uri_lider) as lider:
                sucesso, mensagem = lider.client_request(comando)

            if sucesso:
                logging.info(f"Aceito: {mensagem}")
                return True
            else:
                # Nó não era líder (pode ter ocorrido reeleição entre consulta e envio)
                logging.warning(f"Recusado: {mensagem}. Tentativa {tentativa}/{MAX_TENTATIVAS}")
                time.sleep(2)

        except Pyro5.errors.CommunicationError as exc:
            logging.warning(
                f"Líder inacessível ({exc}). "
                f"Tentativa {tentativa}/{MAX_TENTATIVAS}"
            )
            time.sleep(2)

    logging.error(f"Desistindo do comando '{comando}' após {MAX_TENTATIVAS} tentativas.")
    return False


def exibir_status_cluster():
    """
    Conecta a cada nó e exibe seu estado atual.
    Útil para verificar se os logs estão convergindo após a replicação.
    """
    print("\n" + "=" * 60)
    print(" Status final do cluster")
    print("=" * 60)

    for node_id, uri in NODE_URIS.items():
        try:
            with Pyro5.api.Proxy(uri) as node:
                status = node.get_status()
            print(
                f"\n  Nó {node_id} [{status['state']}] "
                f"termo={status['term']} "
                f"commit={status['commit_index']}"
            )
            print(f"  Log: {[e['command'] for e in status['log']]}")
        except Exception as exc:
            print(f"\n  Nó {node_id}: OFFLINE ou inacessível ({exc})")

    print("=" * 60)


# ---------------------------------------------------------------------------
# Ponto de entrada
# ---------------------------------------------------------------------------

def main():
    print(f"\nAguardando {ESPERA_INICIAL}s para o cluster eleger um líder...\n")
    time.sleep(ESPERA_INICIAL)

    # Envia cada comando com uma pequena pausa entre eles
    for cmd in COMANDOS:
        print(f"\n>> Enviando: {cmd}")
        enviar_comando(cmd)
        time.sleep(1.0)

    # Aguarda um pouco para os heartbeats propagarem os commits
    print("\nAguardando propagação dos commits (3s)...")
    time.sleep(3)

    # Exibe o estado de todos os nós para comparação
    exibir_status_cluster()


if __name__ == "__main__":
    main()
