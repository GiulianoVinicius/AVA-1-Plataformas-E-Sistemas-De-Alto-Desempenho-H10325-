# AVA-1-Plataformas-E-Sistemas-De-Alto-Desempenho-H10325-
AVA 1 Plataformas E Sistemas De Alto Desempenho (H10325)

# ============================================================================ 

# PRÁTICA 1 — SIMULAÇÃO DE SISTEMAS DISTRIBUÍDOS COM PROCESSOS E SOCKETS TCP 

# 

# OBJETIVO GERAL: 

# Demonstrar como diferentes processos Python trocam mensagens pela rede local, 

# compartilham atualizações de estado e identificam possíveis falhas de nós. 

# 

# O QUE O PROGRAMA FAZ: 

# 1. Inicia um servidor TCP que funciona como coordenador. 

# 2. Cria três processos independentes, chamados A, B e C. 

# 3. Os processos enviam mensagens periódicas (heartbeats) ao coordenador. 

# 4. O coordenador registra o estoque de um produto e sua versão; os processos 

#    recebem os dados atualizados quando detectam uma nova versão. 

# 5. Simula uma falha, encerrando o processo B, e mostra a detecção por timeout. 

# 6. Reinicia B e demonstra a recuperação da versão mais recente do estoque. 

# 

# INSTALAÇÃO E PRÉ-REQUISITOS: 

# - É necessário ter Python 3.9 ou superior instalado no computador. 

# - Para usar o VS Code, instale o editor e, preferencialmente, a extensão 

#   "Python" da Microsoft; a extensão é recomendada, não obrigatória. 

# - NÃO é necessário instalar bibliotecas extras via pip: o programa utiliza 

#   somente módulos da biblioteca padrão do Python. 

# - Os processos se comunicam em 127.0.0.1 (localhost); não é preciso criar 

#   servidor remoto, banco de dados ou conta em serviço externo. 

# - O sistema precisa permitir conexões TCP locais (regras de firewall podem 

#   afetar a execução em computadores com restrições de segurança). 

# 

# COMO EXECUTAR NO TERMINAL DO VS CODE: 

#   python --version                  # conferir a versão instalada 

#   python pratica1_tcp_processos.py 

# No Windows, se necessário, troque o comando "python" por "py". 

# 

# OBSERVAÇÃO: é um laboratório didático com coordenador central; não implementa 

# consenso distribuído nem replica tolerante à falha do próprio coordenador. 

# ============================================================================ 

 

"""Prática 1: processos TCP, heartbeats, réplica de estado e falha. 

 

Uso: python pratica1_tcp_processos.py 

Requer apenas a biblioteca padrão do Python 3.9+. 

Todos os serviços são locais (127.0.0.1), para demonstração em aula. 

""" 

 

import json 

import multiprocessing as mp 

import socket 

import socketserver 

import threading 

import time 

from datetime import datetime 

 

HOST = "127.0.0.1" 

TIMEOUT_FALHA = 2.0 

 

 

def registrar(texto): 

    print(f"[{datetime.now():%H:%M:%S}] {texto}", flush=True) 

 

 

def requisitar(porta, pedido): 

    """Protocolo: uma requisição/resposta JSON por linha em TCP.""" 

    with socket.create_connection((HOST, porta), timeout=2.0) as conexao: 

        conexao.sendall((json.dumps(pedido) + "\n").encode("utf-8")) 

        with conexao.makefile("r", encoding="utf-8") as entrada: 

            return json.loads(entrada.readline()) 

 

 

class EstadoCompartilhado: 

    def __init__(self): 

        self.trava = threading.Lock() 

        self.versao = 0 

        self.dados = {"produto": "livro", "estoque": 10} 

        self.ultimo_contato = {} 

 

    def tratar(self, mensagem): 

        tipo = mensagem.get("tipo") 

        identificador = mensagem.get("id") 

        with self.trava: 

            if tipo in ("REGISTRAR", "PULSO"): 

                if identificador not in ("A", "B", "C"): 

                    return {"erro": "identificador desconhecido"} 

                self.ultimo_contato[identificador] = time.monotonic() 

                versao_cliente = mensagem.get("versao", -1) 

                resposta = {"ok": True, "versao": self.versao} 

                # A cópia completa só é enviada quando a versão mudou. 

                if versao_cliente != self.versao: 

                    resposta["dados"] = self.dados.copy() 

                return resposta 

            if tipo == "ATUALIZAR": 

                self.versao += 1 

                self.dados["estoque"] = int(mensagem["estoque"]) 

                return {"ok": True, "versao": self.versao} 

            if tipo == "STATUS": 

                agora = time.monotonic() 

                situacao = {} 

                for nome in ("A", "B", "C"): 

                    ultimo = self.ultimo_contato.get(nome) 

                    situacao[nome] = ( 

                        "ATIVO" if ultimo is not None and agora - ultimo < TIMEOUT_FALHA 

                        else "SEM CONTATO" 

                    ) 

                return {"versao": self.versao, "dados": self.dados.copy(), 

                        "trabalhadores": situacao} 

            return {"erro": "operacao desconhecida"} 

 

 

class AtendenteTCP(socketserver.StreamRequestHandler): 

    def handle(self): 

        try: 

            linha = self.rfile.readline()  # delimitação explícita por \n 

            pedido = json.loads(linha.decode("utf-8")) 

            resposta = self.server.estado.tratar(pedido) 

        except (ValueError, KeyError, UnicodeDecodeError) as erro: 

            resposta = {"erro": str(erro)} 

        self.wfile.write((json.dumps(resposta) + "\n").encode("utf-8")) 

 

 

class ServidorTCP(socketserver.ThreadingTCPServer): 

    allow_reuse_address = True 

    daemon_threads = True 

 

 

def trabalhador(nome, porta): 

    """Cada trabalhador executa em um processo do sistema operacional.""" 

    versao_local = -1 

    registrar(f"Nó {nome}: iniciado em PID={mp.current_process().pid}") 

    while True: 

        try: 

            tipo = "REGISTRAR" if versao_local < 0 else "PULSO" 

            resposta = requisitar(porta, {"tipo": tipo, "id": nome, 

                                          "versao": versao_local}) 

            if "dados" in resposta: 

                versao_local = resposta["versao"] 

                registrar(f"Nó {nome}: versão={versao_local}; " 

                          f"réplica={resposta['dados']}") 

            time.sleep(0.55) 

        except (OSError, ValueError) as erro: 

            registrar(f"Nó {nome}: comunicação interrompida: {erro}") 

            time.sleep(0.7) 

 

 

def mostrar_status(porta, titulo): 

    resposta = requisitar(porta, {"tipo": "STATUS"}) 

    registrar(f"{titulo}: {resposta['trabalhadores']} " 

              f"| dados={resposta['dados']} (v{resposta['versao']})") 

 

 

def main(): 

    # A porta 0 faz o sistema escolher uma porta TCP local disponível. 

    with ServidorTCP((HOST, 0), AtendenteTCP) as servidor: 

        servidor.estado = EstadoCompartilhado() 

        porta = servidor.server_address[1] 

        thread = threading.Thread(target=servidor.serve_forever, daemon=True) 

        thread.start() 

        registrar(f"Coordenador TCP ativo em {HOST}:{porta}") 

 

        contexto = mp.get_context("spawn")  # funciona também no Windows 

        nos = {} 

        try: 

            for nome in ("A", "B", "C"): 

                nos[nome] = contexto.Process(target=trabalhador, args=(nome, porta)) 

                nos[nome].start() 

            # O tempo de inicialização de subprocessos varia em cada computador. 

            limite = time.monotonic() + 15 

            while time.monotonic() < limite: 

                status = requisitar(porta, {"tipo": "STATUS"})["trabalhadores"] 

                if all(valor == "ATIVO" for valor in status.values()): 

                    break 

                time.sleep(0.2) 

            mostrar_status(porta, "1. Estado inicial dos três processos") 

 

            resposta = requisitar(porta, {"tipo": "ATUALIZAR", "estoque": 7}) 

            registrar(f"Professor: estoque alterado para 7 (v{resposta['versao']})") 

            time.sleep(1.3) 

 

            registrar("2. Simulando falha do nó B...") 

            nos["B"].terminate() 

            nos["B"].join(timeout=2) 

            time.sleep(TIMEOUT_FALHA + 0.5) 

            mostrar_status(porta, "3. Detecção por timeout") 

 

            requisitar(porta, {"tipo": "ATUALIZAR", "estoque": 5}) 

            registrar("Professor: estoque alterado para 5 durante a falha") 

            registrar("4. Reiniciando o nó B...") 

            nos["B"] = contexto.Process(target=trabalhador, args=("B", porta)) 

            nos["B"].start() 

            time.sleep(1.6) 

            mostrar_status(porta, "5. Nó B recuperou a versão mais recente") 

        finally: 

            for processo in nos.values(): 

                if processo.is_alive(): 

                    processo.terminate() 

                processo.join(timeout=2) 

            servidor.shutdown() 

            thread.join(timeout=2) 

            registrar("Demonstração encerrada.") 

 

 

if __name__ == "__main__": 

    mp.freeze_support() 

    main()# ============================================================================ 

# PRÁTICA 1 — SIMULAÇÃO DE SISTEMAS DISTRIBUÍDOS COM PROCESSOS E SOCKETS TCP 

# 

# OBJETIVO GERAL: 

# Demonstrar como diferentes processos Python trocam mensagens pela rede local, 

# compartilham atualizações de estado e identificam possíveis falhas de nós. 

# 

# O QUE O PROGRAMA FAZ: 

# 1. Inicia um servidor TCP que funciona como coordenador. 

# 2. Cria quatro processos independentes, chamados A, B, C e D. 

# 3. Os processos enviam mensagens periódicas (heartbeats) ao coordenador. 

# 4. O coordenador registra o estoque de um produto e sua versão; os processos 

#    recebem os dados atualizados quando detectam uma nova versão. 

# 5. Simula uma falha, encerrando o processo B, e mostra a detecção por timeout. 

# 6. Reinicia B e demonstra a recuperação da versão mais recente do estoque. 

# ============================================================================ 

 

"""Prática 1: processos TCP, heartbeats, réplica de estado e falha. 

 

Uso: python pratica1_tcp_processos.py 

Requer apenas a biblioteca padrão do Python 3.9+. 

Todos os serviços são locais (127.0.0.1), para demonstração em aula. 

""" 

 

import json 

import multiprocessing as mp 

import socket 

import socketserver 

import threading 

import time 

from datetime import datetime 

 

HOST = "127.0.0.1" 

TIMEOUT_FALHA = 2.0 

 

 

def registrar(texto): 

    print(f"[{datetime.now():%H:%M:%S}] {texto}", flush=True) 

 

 

def requisitar(porta, pedido): 

    """Protocolo: uma requisição/resposta JSON por linha em TCP.""" 

    with socket.create_connection((HOST, porta), timeout=2.0) as conexao: 

        conexao.sendall((json.dumps(pedido) + "\n").encode("utf-8")) 

        with conexao.makefile("r", encoding="utf-8") as entrada: 

            return json.loads(entrada.readline()) 

 

 

class EstadoCompartilhado: 

    def __init__(self): 

        self.trava = threading.Lock() 

        self.versao = 0 

        self.dados = {"produto": "livro", "estoque": 10} 

        self.ultimo_contato = {} 

 

    def tratar(self, mensagem): 

        tipo = mensagem.get("tipo") 

        identificador = mensagem.get("id") 

        with self.trava: 

            if tipo in ("REGISTRAR", "PULSO"): 

                # Atualizado para incluir o nó "D" 

                if identificador not in ("A", "B", "C", "D"): 

                    return {"erro": "identificador desconhecido"} 

                self.ultimo_contato[identificador] = time.monotonic() 

                versao_cliente = mensagem.get("versao", -1) 

                resposta = {"ok": True, "versao": self.versao} 
