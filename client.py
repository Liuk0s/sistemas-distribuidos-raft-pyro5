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

ESPERA_INICIAL = 6  # segundos para aguardar a eleição
MAX_TENTATIVAS = 5  # tentativas por comando

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [Cliente] %(message)s",
    datefmt="%H:%M:%S",
)


def localizar_lider():
    # Consulta o servidor de nomes pelo nome registrado pelo líder eleito
    try:
        ns  = Pyro5.api.locate_ns()
        uri = ns.lookup(NOME_LIDER)
        return str(uri)
    except Pyro5.errors.NamingError:
        # Líder ainda não se registrou
        return None
    except Exception as erro:
        logging.warning("Erro ao contatar servidor de nomes: " + str(erro))
        return None


def enviar_comando(comando):
    for tentativa in range(1, MAX_TENTATIVAS + 1):
        uri_lider = localizar_lider()

        # Líder ainda não disponível — aguarda e tenta de novo
        if uri_lider is None:
            logging.warning(
                "Lider nao encontrado. Aguardando... " +
                "(tentativa " + str(tentativa) + "/" + str(MAX_TENTATIVAS) + ")"
            )
            time.sleep(2)
            continue

        logging.info("Lider em: " + uri_lider)

        try:
            # Conecta ao líder, envia o comando e libera a conexão
            lider = Pyro5.api.Proxy(uri_lider)
            try:
                resultado = lider.rpc_comando_cliente(comando)
                sucesso   = resultado[0]
                mensagem  = resultado[1]
            finally:
                lider._pyroRelease()

            if sucesso:
                logging.info("Aceito: " + str(mensagem))
                return True
            else:
                # Líder recusou — pode estar em transição, tenta de novo
                logging.warning(
                    "Recusado: " + str(mensagem) + " " +
                    "(tentativa " + str(tentativa) + "/" + str(MAX_TENTATIVAS) + ")"
                )
                time.sleep(2)

        except Exception as erro:
            logging.warning(
                "Erro ao contatar lider: " + str(erro) + " " +
                "(tentativa " + str(tentativa) + "/" + str(MAX_TENTATIVAS) + ")"
            )
            time.sleep(2)

    logging.error(
        "Desistindo do comando '" + comando + "' apos " + str(MAX_TENTATIVAS) + " tentativas."
    )
    return False


def exibir_status():
    print("\n" + "=" * 55)
    print(" Status final do cluster")
    print("=" * 55)

    # Consulta cada nó diretamente pela URI
    for node_id in URIS:
        uri = URIS[node_id]
        try:
            no = Pyro5.api.Proxy(uri)
            try:
                status = no.rpc_status()
            finally:
                no._pyroRelease()

            print(
                "\n  No " + str(node_id) +
                " [" + status["estado"] + "]" +
                " termo=" + str(status["termo"]) +
                " commit=" + str(status["commit_index"])
            )

            # Monta lista de comandos do log deste nó
            comandos = []
            for entrada in status["log"]:
                comandos.append(entrada["comando"])

            print("  Log: " + str(comandos))

        except Exception as erro:
            print("\n  No " + str(node_id) + ": OFFLINE (" + str(erro) + ")")

    print("=" * 55)


def main():
    print("\nAguardando " + str(ESPERA_INICIAL) + "s para o cluster eleger um lider...\n")
    time.sleep(ESPERA_INICIAL)

    for cmd in COMANDOS:
        print("\n>> Enviando: " + cmd)
        enviar_comando(cmd)
        time.sleep(1.0)

    print("\nAguardando propagacao dos commits (3s)...")
    time.sleep(3)

    exibir_status()


if __name__ == "__main__":
    main()