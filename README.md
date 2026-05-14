# Raft com PyRO5 — Sistemas Distribuídos

Implementação do algoritmo de consenso **Raft** para replicação de log entre
4 processos comunicando-se via **PyRO5** (Python Remote Objects).

---

## Arquivos

| Arquivo          | Descrição                                          |
|------------------|----------------------------------------------------|
| `raft_node.py`   | Nó Raft (FOLLOWER / CANDIDATE / LEADER) + Daemon  |
| `client.py`      | Cliente que localiza o líder e envia comandos      |
| `README.md`      | Este arquivo                                       |

---

## Dependências

```bash
pip install Pyro5
```

---

## Execução (6 terminais separados)

### 1. Servidor de nomes PyRO
```bash
python -m Pyro5.nameserver
```

### 2. Quatro nós Raft (um por terminal)
```bash
python raft_node.py 1   # PYRO:raft.node1@localhost:9091
python raft_node.py 2   # PYRO:raft.node2@localhost:9092
python raft_node.py 3   # PYRO:raft.node3@localhost:9093
python raft_node.py 4   # PYRO:raft.node4@localhost:9094
```

### 3. Cliente
```bash
python client.py
```

---

## Testando falha de líder

Após a eleição inicial, pressione **Ctrl+C** no terminal do líder atual
para simular uma falha. Os demais nós detectarão a ausência de heartbeats,
o timer de eleição de algum deles expirará, e uma nova eleição ocorrerá.
O novo líder se registrará no servidor de nomes sobrescrevendo a entrada
anterior de `raft.leader`.

---

## URIs hard-coded

As URIs seguem o formato `PYRO:<objectId>@localhost:<porta>`:

| Nó | objectId      | Porta |
|----|---------------|-------|
| 1  | raft.node1    | 9091  |
| 2  | raft.node2    | 9092  |
| 3  | raft.node3    | 9093  |
| 4  | raft.node4    | 9094  |

O líder atual é registrado adicionalmente no NS com o nome `raft.leader`.

---

## Resumo do fluxo Raft implementado

```
  Inicialização
      ↓
  Todos: FOLLOWER, termo=0, timer aleatório rodando

  Timer expira (sem heartbeat do líder)
      ↓
  Nó vira CANDIDATE, incrementa termo, vota em si mesmo
  Solicita request_vote() a todos os outros nós em paralelo
      ↓
  Maioria responde vote_granted=True?
      ├── Sim → LEADER: registra no NS, inicia heartbeats
      └── Não → volta a FOLLOWER, sorteia novo timer

  LEADER recebe client_request(command)
      ↓
  Anexa ao log, replica via append_entries() no próximo heartbeat
      ↓
  Maioria confirma a entrada no log?
      ├── Sim → avança commit_index (próximo heartbeat propaga para seguidores)
      └── Não → aguarda próximo ciclo
```
