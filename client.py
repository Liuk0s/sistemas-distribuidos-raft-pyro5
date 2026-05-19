"""
client.py — Cliente do cluster Raft.

Localiza o líder pelo servidor de nomes e envia comandos.
Ao final exibe o status de todos os nós para comparação dos logs.

Uso:
    python client.py
"""

import time
import logging
import Pyro5.api
import Pyro5.errors

# Nome do líder no servidor de nomes
NOME_LIDER = "raft.leader"

# URIs diretas dos nós (para exibir status ao final)
URIS = {
    1: "PYRO:raft.node1@localhost:9091",
    2: "PYRO:raft.node2@localhost:9092",
    3: "PYRO:raft.node3@localhost:9093",
    4: "PYRO:raft.node4@localhost:9094",
}

# Comandos a enviar ao cluster
COMANDOS = [
    "SET x=10",
    "SET y=20",
    "SET z=30",
    "DEL x",
    "SET w=99",
    "SET x=42",
]

ESPERA_INICIAL  = 6   # segundos para aguardar a eleição
MAX_TENTATIVAS  = 5   # tentativas por comando

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [Cliente] %(message)s",
    datefmt="%H:%M:%S",
)


def localizar_lider():
    # Consulta o servidor de nomes pelo nome do líder
    try:
        ns  = Pyro5.api.locate_ns()
        uri = ns.lookup(NOME_LIDER)
        return str(uri)
    except Pyro5.errors.NamingError:
        return None  # líder ainda não registrado
    except Exception as erro:
        logging.warning(f"Erro ao contatar servidor de nomes: {erro}")
        return None


def enviar_comando(comando):
    for tentativa in range(1, MAX_TENTATIVAS + 1):
        uri_lider = localizar_lider()

        if uri_lider is None:
            logging.warning(
                f"Líder não encontrado. Aguardando... "
                f"(tentativa {tentativa}/{MAX_TENTATIVAS})"
            )
            time.sleep(2)
            continue

        logging.info(f"Líder em: {uri_lider}")

        try:
            with Pyro5.api.Proxy(uri_lider) as lider:
                sucesso, mensagem = lider.rpc_comando_cliente(comando)

            if sucesso:
                logging.info(f"✓ Aceito: {mensagem}")
                return True
            else:
                logging.warning(
                    f"✗ Recusado: {mensagem} "
                    f"(tentativa {tentativa}/{MAX_TENTATIVAS})"
                )
                time.sleep(2)

        except Exception as erro:
            logging.warning(
                f"Erro ao contatar líder: {erro} "
                f"(tentativa {tentativa}/{MAX_TENTATIVAS})"
            )
            time.sleep(2)

    logging.error(f"Desistindo do comando '{comando}' após {MAX_TENTATIVAS} tentativas.")
    return False


def exibir_status():
    print("\n" + "=" * 55)
    print(" Status final do cluster")
    print("=" * 55)

    for node_id, uri in URIS.items():
        try:
            with Pyro5.api.Proxy(uri) as no:
                status = no.rpc_status()
            print(
                f"\n  Nó {node_id} [{status['estado']}] "
                f"termo={status['termo']} "
                f"commit={status['commit_index']}"
            )
            comandos = [e["comando"] for e in status["log"]]
            print(f"  Log: {comandos}")
        except Exception as erro:
            print(f"\n  Nó {node_id}: OFFLINE ({erro})")

    print("=" * 55)


def main():
    print(f"\nAguardando {ESPERA_INICIAL}s para o cluster eleger um líder...\n")
    time.sleep(ESPERA_INICIAL)

    for cmd in COMANDOS:
        print(f"\n>> Enviando: {cmd}")
        enviar_comando(cmd)
        time.sleep(1.0)

    print("\nAguardando propagação dos commits (3s)...")
    time.sleep(3)

    exibir_status()


if __name__ == "__main__":
    main()
