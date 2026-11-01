1. O servidor coordenador é iniciado.
2. Três processos independentes (A, B e C) são criados.
3. Os processos enviam sinais periódicos chamados heartbeats.
4. O servidor atualiza o estoque e comunica as mudanças.
5. O processo B é encerrado para simular uma falha.
6. O processo B é reiniciado e recebe a versão mais recente do estoque.
O objetivo é demonstrar como diferentes processos podem trabalhar juntos, mesmo quando um deles apresenta uma falha.
2. Bibliotecas utilizadas
No começo do código temos:
import jsonimport multiprocessing as mpimport socketimport socketserverimport threadingimport timefrom datetime import datetime



Cada biblioteca tem uma função:
Biblioteca	Função
json	Organiza os dados em formato JSON para transmissão.
multiprocessing	Cria processos independentes.
socket	Permite a comunicação pela rede.
socketserver	Facilita a criação do servidor TCP.
threading	Executa tarefas simultaneamente por threads.
time	Controla intervalos e tempo de comunicação.
datetime	Registra o horário das mensagens.

Um detalhe importante: o programa não precisa instalar bibliotecas adicionais. Todos esses módulos já fazem parte da biblioteca padrão do Python.
3. Configurações iniciais
HOST = "127.0.0.1"TIMEOUT_FALHA = 2.0



Aqui temos duas configurações.
- HOST: determina o endereço de comunicação. O endereço 127.0.0.1 representa o próprio computador, também conhecido como localhost.
- TIMEOUT_FALHA: determina o tempo máximo sem comunicação para considerar um processo sem contato. Nesse caso, são 2 segundos.
Isso significa que todos os processos e o servidor estão funcionando no mesmo computador, mas se comunicam utilizando a estrutura de rede TCP.
4. Função para registrar mensagens
def registrar(texto):    print(f"[{datetime.now():%H:%M:%S}] {texto}", flush=True)



Essa função serve para mostrar mensagens no terminal.

Por exemplo:
[14:30:05] Coordenador TCP ativo em 127.0.0.1:54321


- datetime.now() obtém o horário atual.
- %H:%M:%S formata o horário em horas, minutos e segundos.
- flush=True força a exibição imediata da mensagem.
É uma função de acompanhamento, que ajuda a visualizar o que está acontecendo durante a execução.
5. Comunicação entre os processos
Essa é uma das partes mais importantes do programa.
def requisitar(porta, pedido):    with socket.create_connection((HOST, porta), timeout=2.0) as conexao:        conexao.sendall((json.dumps(pedido) + "\n").encode("utf-8"))        with conexao.makefile("r", encoding="utf-8") as entrada:            return json.loads(entrada.readline())



Vamos dividir:

1. Criar conexão
socket.create_connection((HOST, porta), timeout=2.0)



Estabelece uma conexão TCP com o servidor.

2. Enviar mensagem
conexao.sendall((json.dumps(pedido) + "\n").encode("utf-8"))



Aqui acontecem três operações:
- json.dumps() transforma os dados em texto JSON.
- encode("utf-8") transforma o texto em bytes.
- sendall() envia os dados pela conexão.
3. Receber resposta
entrada.readline()



Lê a resposta enviada pelo servidor.

4. Interpretar resposta
json.loads(entrada.readline())



Transforma o JSON recebido novamente em um objeto Python.

Por exemplo, uma mensagem enviada pode ser:
{    "tipo": "PULSO",    "id": "A",    "versao": 0}



Isso significa que o processo A está informando ao servidor que continua ativo e possui a versão 0 dos dados.
6. Classe EstadoCompartilhado
class EstadoCompartilhado:    def __init__(self):        self.trava = threading.Lock()        self.versao = 0        self.dados = {"produto": "livro", "estoque": 10}        self.ultimo_contato = {}



Essa classe é responsável por armazenar e controlar as informações do sistema.

Vamos entender cada variável:
Variável	Significado
trava	Evita que várias threads alterem os dados simultaneamente.
versao	Identifica a versão atual das informações.
dados	Armazena o produto e a quantidade em estoque.
ultimo_contato	Registra quando cada processo se comunicou pela última vez.

Inicialmente, o estoque é:
Produto

Livro


Estoque inicial
10

Versão
0


O método tratar()
Essa função recebe e interpreta as mensagens enviadas pelos processos.
tipo = mensagem.get("tipo")identificador = mensagem.get("id")



Ela identifica o tipo da mensagem e quem está enviando.

O código trabalha principalmente com quatro operações:
Operação	Função
REGISTRAR	Registra um processo pela primeira vez.
PULSO	Recebe o sinal periódico de atividade.
ATUALIZAR	Altera o estoque e incrementa a versão.
STATUS	Consulta quais processos estão ativos.

Um trecho importante:
with self.trava:



Ele utiliza um mecanismo chamado Lock (bloqueio).

Imagine que os processos A, B e C tentem atualizar ou consultar informações ao mesmo tempo. O bloqueio organiza o acesso aos dados compartilhados pelo servidor, evitando alterações simultâneas conflitantes.
Como funciona a atualização?
if tipo == "ATUALIZAR":    self.versao += 1    self.dados["estoque"] = int(mensagem["estoque"])    return {"ok": True, "versao": self.versao}



Quando o servidor recebe uma atualização:
1. Incrementa a versão.
2. Altera a quantidade do estoque.
3. Retorna a nova versão.
Por exemplo:
- Estoque inicial: 10 livros, versão 0.
- Primeira atualização: 7 livros, versão 1.
- Segunda atualização: 5 livros, versão 2.
Isso permite que os processos saibam se estão trabalhando com informações antigas ou atualizadas.
7. Servidor TCP
O servidor é responsável por receber as mensagens dos processos.

Primeiro temos:
class AtendenteTCP(socketserver.StreamRequestHandler):



Essa classe define como o servidor deve atender cada conexão.

Dentro dela:
linha = self.rfile.readline()pedido = json.loads(linha.decode("utf-8"))resposta = self.server.estado.tratar(pedido)



O servidor:
1. Recebe a mensagem.
2. Interpreta o JSON.
3. Encaminha a solicitação para a classe EstadoCompartilhado.
4. Obtém uma resposta.
Depois envia a resposta:
self.wfile.write((json.dumps(resposta) + "\n").encode("utf-8"))



Já esta classe:
class ServidorTCP(socketserver.ThreadingTCPServer):    allow_reuse_address = True    daemon_threads = True



Permite que o servidor atenda várias conexões utilizando threads.

Aqui temos dois conceitos diferentes:
- Processos: executam os nós A, B e C de forma independente.
- Threads: permitem que o servidor atenda diferentes conexões simultaneamente.
8. Os trabalhadores (A, B e C)
Agora chegamos à função que representa cada processo independente.
def trabalhador(nome, porta):    versao_local = -1    registrar(f"Nó {nome}: iniciado em PID={mp.current_process().pid}")



Cada trabalhador recebe um nome e a porta de comunicação.

PID significa Process Identifier, ou identificador do processo no sistema operacional.

Cada processo possui seu próprio PID.

Em seguida:
while True:



Cria um laço de repetição contínuo.

Isso significa que o processo continuará executando enquanto não for encerrado.

Dentro do laço:
tipo = "REGISTRAR" if versao_local < 0 else "PULSO"



Essa linha utiliza uma condição simplificada:
- Se a versão local for menor que zero, o processo ainda não foi registrado.
- Caso contrário, envia um pulso periódico.
Depois:
resposta = requisitar(porta, {    "tipo": tipo,    "id": nome,    "versao": versao_local})



O processo envia sua identificação e sua versão atual ao servidor.

Se receber dados atualizados:
if "dados" in resposta:    versao_local = resposta["versao"]    registrar(f"Nó {nome}: versão={versao_local}; réplica={resposta['dados']}")



Ele atualiza sua versão local e registra a cópia dos dados recebidos.

Finalmente:
time.sleep(0.55)



Espera 0,55 segundo antes de enviar outra mensagem.

Esse intervalo é responsável pela frequência dos heartbeats.
O que são heartbeats?
Heartbeat
Sinal periódico de atividade

É uma mensagem que um processo envia regularmente para informar ao servidor que continua funcionando.

Imagine que A, B e C enviem mensagens a cada 0,55 segundo.

O servidor consegue acompanhar quando cada processo se comunicou pela última vez.

Se um deles parar de enviar mensagens, o servidor poderá identificar a ausência de comunicação.
9. Detecção de falhas
Essa é uma das partes mais interessantes da atividade.
if tipo == "STATUS":    agora = time.monotonic()    situacao = {}    for nome in ("A", "B", "C"):        ultimo = self.ultimo_contato.get(nome)        situacao[nome] = (            "ATIVO" if ultimo is not None and agora - ultimo < TIMEOUT_FALHA            else "SEM CONTATO"        )



O servidor verifica o último contato de cada processo.

A lógica é:
- Se o último contato ocorreu há menos de 2 segundos, o processo é considerado ativo.
- Se já passaram 2 segundos ou mais, ele é considerado sem contato.
Observe que o código utiliza time.monotonic().

Essa função fornece uma referência de tempo adequada para medir intervalos, pois não depende de alterações no relógio do computador.

Exemplo:
Processo	Último contato	Situação
A	0,4 segundo	ATIVO
B	2,5 segundos	SEM CONTATO
C	0,7 segundo	ATIVO

Assim, o servidor identifica que B deixou de se comunicar.

Vale destacar: o programa detecta ausência de comunicação, não necessariamente uma falha física do computador. Um processo pode estar lento ou temporariamente sem conseguir se comunicar.
10. Função principal (main)
Agora vamos entender como todas as partes são colocadas em funcionamento.
def main():    with ServidorTCP((HOST, 0), AtendenteTCP) as servidor:        servidor.estado = EstadoCompartilhado()        porta = servidor.server_address[1]



Aqui o programa cria o servidor TCP.

Um detalhe interessante é:
(HOST, 0)



A porta 0 permite que o próprio sistema operacional escolha uma porta TCP disponível.

Depois:
thread = threading.Thread(    target=servidor.serve_forever,    daemon=True)thread.start()



O servidor começa a funcionar em uma thread separada.

Isso permite que o programa continue executando outras tarefas enquanto o servidor atende às conexões.
Criação dos processos
contexto = mp.get_context("spawn")nos = {}for nome in ("A", "B", "C"):    nos[nome] = contexto.Process(        target=trabalhador,        args=(nome, porta)    )    nos[nome].start()



Aqui acontece a criação dos três processos.
- Processo A.
- Processo B.
- Processo C.
O método Process() cria um novo processo independente.

O método start() inicia sua execução.

O parâmetro spawn determina como os novos processos são iniciados, sendo uma opção compatível com o Windows.
Simulação de falha
O código espera que os três processos estejam ativos e depois apresenta o estado inicial.

Em seguida:
resposta = requisitar(    porta,    {"tipo": "ATUALIZAR", "estoque": 7})



O estoque passa de 10 para 7.

Depois acontece:
nos["B"].terminate()nos["B"].join(timeout=2)



Aqui está a simulação de falha.
- terminate() encerra o processo B.
- join() aguarda o encerramento do processo.
Como B deixou de enviar heartbeats, o servidor identifica sua ausência após o timeout.
Recuperação do processo
Depois de detectar a falha, o programa altera novamente o estoque:
requisitar(    porta,    {"tipo": "ATUALIZAR", "estoque": 5})



Agora o estoque é 5, versão 2.

Em seguida:
nos["B"] = contexto.Process(    target=trabalhador,    args=("B", porta))nos["B"].start()



O processo B é criado novamente.

Como ele começa com:
versao_local = -1



Ele solicita seu registro inicial ao servidor.

O servidor percebe que sua versão é diferente da versão atual e envia os dados mais recentes.

Assim, B recupera o estado atualizado do estoque: 5 livros, versão 2.
11. Encerramento do programa
finally:    for processo in nos.values():        if processo.is_alive():            processo.terminate()        processo.join(timeout=2)    servidor.shutdown()    thread.join(timeout=2)



Essa parte garante que os processos e o servidor sejam encerrados ao final da demonstração.

Também evita deixar processos executando desnecessariamente no computador.

Por último:
if __name__ == "__main__":    mp.freeze_support()    main()



Essa condição verifica se o arquivo está sendo executado diretamente.

O freeze_support() auxilia na compatibilidade da criação de processos no Windows.

O main() inicia a execução principal do programa.

12. Resumo do funcionamento completo
Veja como o estoque evolui durante a demonstração:
Linha do tempo da simulação
1. Inicialização
   
   A, B e C são iniciados e registrados no servidor.
   Estoque
   
   10 livros
2. Primeira atualização
   
   O coordenador altera o estoque e os processos recebem a nova versão.
   Estoque
   
   7 livros — versão 1
3. Falha do processo B
   
   B é encerrado. Após 2 segundos sem contato, é identificado como indisponível.
   A ativo
   B sem contato
   C ativo
4. Nova atualização durante a falha
   
   O coordenador altera o estoque novamente.
   Estoque
   
   5 livros — versão 2
5. Recuperação de B
   
   B é reiniciado, registra-se novamente e recebe os dados atualizados.
   Estado final
   3 processos ativos

13. O que você precisa aprender com essa atividade?
Como você está estudando Engenharia da Computação, vale dar atenção especial a estes conceitos:
Conceito	O que você aprende
TCP	Comunicação entre computadores e processos.
Multiprocessing	Execução de processos independentes.
Threading	Execução concorrente de tarefas.
JSON	Organização e transmissão de informações.
Heartbeat	Monitoramento de disponibilidade.
Timeout	Identificação de ausência de comunicação.
Sincronização	Controle de acesso aos dados compartilhados.
Versionamento	Identificação de atualizações de informações.

Uma observação importante sobre o arquivo: ele contém duas versões do código. A primeira utiliza três processos (A, B e C) e está completa. A segunda começa uma adaptação para quatro processos (A, B, C e D), mas está incompleta no arquivo. Para a atividade, precisamos escolher uma versão e manter o código e o relatório consistentes.
