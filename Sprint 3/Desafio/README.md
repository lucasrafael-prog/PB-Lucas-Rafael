# Desafio 
Nesta Sprint, o desafio consistiu em combinar conhecimentos de *Python* e *Docker*, concluindo um processo de análise e limpeza de uma base de dados para a resolução de algumas questões com a utilização de scripts e *Jupyter* como notebook(se necessário), além das bibliotecas **Pandas**, **Matplotlib** e **REGEX** para manuseio e visualização dos dados, e consequentemente a orquestração de *containers* para este projeto.

## Etapas
Abaixo consta o passo a passo de como foram aplicadas as etapas de atividades sobre o projeto desde a limpeza do dataset, até a implantação de volumes para os *containeres* do projeto.

1. O primeiro passo foi o download do Rancher Desktop (para as funções de Docker). A escolha do Rancher se deve a questão de desempenho da máquina. 

2. Em seguida preparei o ambiente com os *VSCode* e realizei o download e a abertura do arquivo [*'concert_tours_by_women.csv'*](./Arquivos/etl/concert_tours_by_women.csv) para identificar com o que estava lidando.

    ![Abertura Arquivo CSV](../Evidencias/Etapa1_Linhas_CsvSujo.png)

    * É uma base de dados com 20 linhas de informação sobre turnês de artistas mulheres.

3. Após a abertura, criei o arquivo *Jupyter Notebook*    [*'etl_dados.ipynb'*](./Arquivos/etl/etl_dados.ipynb) para a organização das etapas, antes de implantá-las no script *Python*.

4. Para iniciar os processos de ETL da Etapa 1, que consta na limpeza do dataset, realizei a importação da biblioteca **Pandas**.

    ![Abertura Arquivo CSV](../Evidencias/Etapa1_ImportacaoBibliotecas.png)

5. A seguir, foi efetuada a leitura dos dados do dataset:

    ![Leitura Arquivo CSV](../Evidencias/Etapa1_LeituraCSV.png)

6. Levando em consideração o resultado pedido no desafio, é possível observar que há muitos erros no dataset a serem transformados até chegar a este resultado:

    ![Resultado Imagem](../Evidencias/Etapa1_Resultado+CSV.png)

    * Há colunas que não serão utilizadas, colunas com formatação errada, símbolos desnecessários e etc.  

7. Para iniciar a transformação destes dados, efetuo a exclusão das colunas desnecessárias que não aparecerão no estado final: 

    ![Etapa 1 - Exclusão Colunas](../Evidencias/Etapa1_ExclusaoColunas.png)

    * Foram excluídas as colunas **Peak**, **All Time Peak** e **Ref.**

8. Em seguida, efetivo a remoção dos símbolos e letras das colunas numéricas que possuem valores em dinheiro, para o manuseio correto das informações:

    ![Etapa 1 - Removendo Símbolos](../Evidencias/Etapa1_LimpandoSimbolos.png)

    * Foram removidos símbolos como cifrões($), colchetes([ ]), vírgulas(,) e letras com a utilização do *replace* e de atributos de **REGEX**.

9. Para completar a limpeza das respectivas colunas, é feita a conversão dos dados de *string* para *float*:

    ![Etapa 1 - Conversão Colunas](../Evidencias/Etapa1_ConvertendoColunas.png)

10. Continuando a limpeza das próximas colunas, renomeio o título da coluna **Adjusted gross (in 2022 dollars)** que possui um espaço a menos do que o exigido no resultado:

    ![Etapa 1 - Renomeando Coluna](../Evidencias/Etapa1_RenomeandoColuna.png)

11. Seguindo para a coluna **Tour title** que também possui símbolos e letras, utilizo o seguinte código que por consequência ocasiona num erro:

    ![Etapa 1 - Erro REGEX](../Evidencias/Etapa1_ErroRegex.png)

    * Após leitura e pesquisa descobri que não poderia deixar símbolos como asterísticos(*) soltos na expressão, pois servem como quantificadores na expressão do *Regex*.

12. Para solucionar o pequeno problema, adicionei um *r* antes da expressão e juntei os símbolos, letras e números dentro dos colchetes:

    ![Etapa 1 - Limpeza Correta](../Evidencias/Etapa1_LimpandoColunaTour.png)

    * Foram removidos os símbolos e as letras/números de forma separada.

13. Para progredir com os processos, agora na coluna **Year(s)** é necessário separá-las em ano de início da turnê e ano de encerramento. Para isso, aplico a separação das colunas com o atributo *expand*: 

    ![Etapa 1 - Separando Coluna Years](../Evidencias/Etapa1_SeparacaoColunaAno.png)

    * Na segunda linha, utilizo o *fillna* para completar as colunas possuem apenas um ano para iniciar e encerrar a turnê, inserindo o valor da **Start year** na **End year**.

14. Para concluir a limpeza destas colunas, realizo a conversão dos dados, desta vez de *string* para *int* e excluo a coluna **Year(s)** que não será mais aproveitada:

    ![Etapa 1 - Conversão Coluna Year](../Evidencias/Etapa1_Conversão_ColunaYears.png)

15. Encerrando a parte de transformação dos dados, efetivo a conversão da coluna **Shows** de *string* para *int*, me certificando de que os dados estão sendo tratados de forma correta:

    ![Etapa 1 - Conversão Coluna Shows](../Evidencias/Etapa1_CoversaoColunaShows.png)

16. Já para o carregamento dos dados, elaboro a exportação dos dados tratados, para um novo arquivo *csv_limpo.csv*:

    ![Etapa 1 - Carregamento CSV Limpo](../Evidencias/Etapa1_ExportacaoCSVLimpo.png)

17. Após a exportação, obtive como resultado no arquivo a nova base de dados:

    ![Etapa 1 - Csv Limpo](../Evidencias/Etapa1_CsvLimpo.png)

18. Finalizados os processos de ETL, crio o script *Python* para migrar o algoritmo, desta maneira, encerrando a etapa 1 do desafio:
    
    * Primeira parte implantada:

    ![Etapa 1 - Algoritmo Python parte 1](../Evidencias/Etapa1_AlgoritmoPy1.png)

    * Segunda parte implantada:
    
    ![Etapa 1 - Algoritmo Python parte 2](../Evidencias/Etapa1_AlgoritmoPy2.png)

19. Partindo para etapa 2 do Desafio, onde é necessário realizar o processamento de dados para responder algumas questões, inicio criando outro arquivo *Jupyter* [*'job_dados.ipynb'*](./Arquivos/job/job_dados.ipynb) para a organização das etapas, assim como na etapa anterior.

20. Antes de implantar a solução das questões, executo a importação das bibliotecas **Pandas** e **Matplotlib**:

    ![Etapa 2 - Importação Bibliotecas](../Evidencias/Etapa2_ImportacaoBibliotecas.png)

21. Efetuo a leitura do dataset no arquivo *CSV* que foi tratado na etapa anterior:

    ![Etapa 2 - Leitura CSV](../Evidencias/Etapa2_LeituraCsv.png)

22. Iniciando a implantação das soluções das questões, vamos para a Questão 1:
    
    ![Etapa 2 - Q1 Contagem](../Evidencias/Etapa2_Q1_ContagemArtista.png)

    * Para encontrar a artista que mais aparece na lista, crio a variável para armazenar a contagem, utilizando *value_counts* para esse fim.

23. Em seguida, realizo o cálculo da média de faturamento das artistas e isolo a artista com a maior média, criando variáveis para ambos os processos.

    ![Etapa 2 - Q1 Cálculos](../Evidencias/Etapa2_Q1_Calculos.png)

    * Uso do *groupby* para agrupar o nome das artistas com seus valores, *mean* para a média, *sort_values* para ordenar e *ascending=False* para a forma decrescente.

24. Com os dados separados, podemos introduzir o retorno das soluções dessa questão: 

    ![Etapa 2 - Q1 Retorno](../Evidencias/Etapa2_Q1_Retorno.png)
    
    * Obs: Utilização dos atributos *index*, *iloc* e *values*, para que os valores da variável sejam retornados de forma corretas na solução.

25. Finalizada a questão 1, avançamos para a segunda, em que é necessário separar as turnês que ocorreram em apenas um ano para a busca da conclusão:

    ![Etapa 2 - Q2 Separação Turnês](../Evidencias/Etapa2_Q2_SeparacaoAno.png)

26. Efetuada a separação, consigo encontrar a turnê com maior média:

    ![Etapa 2 - Q2 Maior Média](../Evidencias/Etapa2_Q2_CalculoMedia.png)

27. Ao encontrá-la, ja é possível introduzir o retorno desta questão:

    ![Etapa 2 - Q2 Retorno](../Evidencias/Etapa2_Q2_Retorno.png)

    * Obs: Busco os *values* por cada coluna como uma maneira de apresentar a solução no retorno.

28. Progredindo para a terceira questão, calculamos a divisão da renda ajustada pela quantidade de shows, para encontrar o valor unitário, criando uma nova coluna para o resultado:

    ![Etapa 2 - Q3 Cálculo Nova Coluna](../Evidencias/Etapa2_Q3_CriacaoNovaColuna.png)

    * Coluna foi criada com o nome **Gain per show**.

29. Com a criação da coluna, ordeno os dados dos shows com mais lucros:

    ![Etapa 2 - Q3 Ordenação ](../Evidencias/Etapa2_Q3_SeparacaoDados.png)

30. E para implantar o retorno da solução, emprego um laço de repetição *for* para apresentar cada linha (*row*):

    ![Etapa 2 - Q3 Retorno](../Evidencias/Etapa2_Q3_Retorno.png)

31. Dando continuidade, na questão 4 é preciso criar um gráfico para mostrar o faturamento por ano das turnês da artista com mais aparições:

    ![Etapa 2 - Q4 Soma Faturamento](../Evidencias/Etapa2_Q4_FaturamentoArtistas.png)

    * Na imagem acima, realizo um agrupamento das artistas com a soma dos faturamentos nas turnês.

32. A seguir, separo a artista com maior faturamento numa variável própria para efeito de confirmação:
    
    ![Etapa 2 - Q4 Separação](../Evidencias/Etapa2_Q4_VariavelArtista.png)

33. Depois, efetuo a divisão dos dados de cada turnê dessa artista:

    ![Etapa 2 - Q4 Dados Artista](../Evidencias/Etapa2_Q4_DadosArtista.png)

34. Após a divisão, é gerada a separação dos valores de faturamento por ano de turnê, com a soma:

    ![Etapa 2 - Q4 Soma Faturamento](../Evidencias/Etapa2_Q4_SomaFaturamento.png)

35. Com os dados delimitados, se torna viável implementar o gráfico como solução:

    ![Etapa 2 - Q4 Gráfico](../Evidencias/Etapa2_Q4_ImplantacaoGrafico.png)

    * Utilizando os fundamentos de *grid* para criar uma visualização de grade no gráfico e *savefig* para salvar a imagem com solução da 4ª questão.

36. Passando para a 5ª questão, última da etapa, que consiste em demonstrar os artistas com mais shows numa visualização de gráfico:

    ![Etapa 2 - Q5 Contagem](../Evidencias/Etapa2_Q5_DivisaoDados.png)

    * É implantada numa variável a soma das quantidades de shows das artistas com a ordem decrescente.

37. Para finalizar as soluções das questões, implementamos o gráfico:

    ![Etapa 2 - Q5 Gráfico](../Evidencias/Etapa2_Q5_ImplantacaoGrafico.png)

    * Com a finalidade de facilitar a visualização do gráfico, crio uma váriavel para as colunas do gráfico (*barras*), servindo para identificá-las na apresentação dos valores das colunas (*bar_label*).
    
    * Também é estabelecida uma rotação nos nomes das artistas do eixo X *(xticks)*.

38. Antes de completar a etapa 2 com a migração do algoritmo para o script *Python*, é necessário gerar um arquivo *txt* com as respostas das três primeiras questões. Para isso, emprego variáveis no retorno das soluções para cada resposta da seguinte maneira:
    
    * Para a resposta da Q1:

    ![Etapa 2 - Variável Q1](../Evidencias/Etapa2_VariavelQ1.png)

    * Para a resposta da Q2:

    ![Etapa 2 - Variável Q2](../Evidencias/Etapa2_VariavelQ2.png)

    * Para a resposta da Q3:

    ![Etapa 2 - Variável Q3](../Evidencias/Etapa2_VariavelQ3.png)

    * E escrevo as respostas na geração do arquivo *txt*:

    ![Etapa 2 - Gerar Respostas](../Evidencias/Etapa2_Respostas_txt.png)

39. Segue a conclusão com a migração para o script *Python*:

    * Primeira parte implantada:

    ![Etapa 2 - Algoritmo Python 1](../Evidencias/Etapa2_AlgoritmoPy1.png)

    * Segunda parte implantada:

    ![Etapa 2 - Algoritmo Python 2](../Evidencias/Etapa2_AlgoritmoPy2.png)

    * Terceira parte implantada: 

    ![Etapa 2 - Algoritmo Python 3](../Evidencias/Etapa2_AlgoritmoPy3.png)

40. Posteriormente a conclusão destas etapas, chego a etapa 3 em que é solicitada a execução do script por meio de um arquivo Docker. Segue a estrutura do **Dockerfile** criada na pasta do ETL: 

    ![Etapa 3 - Arquivo Docker](../Evidencias/Etapa3_ArquivoDocker.png)

41. A fim de executar o script, elaboro inicialmente o comando da *build* da imagem:

    ![Etapa 3 - Criação da Imagem](../Evidencias/Etapa3_CriacaoImagem.png)

    * Nomeei a imagem como *desafio-etl*.

42. E realizo a execução do container:

    ![Etapa 3 - Rodando Container](../Evidencias/Etapa3_ExecucaoContainer.png)

    * Obs: Aplicação de um volume para persistir a criação do *csv_limpo.csv* no diretório, pós execução e encerramento do container.

43. Já na etapa 4, atendendo ao mesmo processo de criação do **Dockerfile**, desta vez na pasta *job*, segue a estrutura do arquivo:

    ![Etapa 4 - Arquivo Docker](../Evidencias/Etapa4_ArquivoDocker.png)

    * Nesta ocasião, diferente da etapa anterior, os arquivos *job.py* e *csv_limpo.csv* são copiados da raiz de arquivos para que não ocorra problemas em encontrá-los durante a criação da imagem e execução do container.

44. Segue a criação da imagem com o nome de *desafio-job*:

    ![Etapa 4 - Criação da Imagem](../Evidencias/Etapa4_CriacaoImagem.png)

45. Em seguida, efetuo a execução de mais um container:

    ![Etapa 4 - Rodando Container](../Evidencias/Etapa4_ExecucaoContainer.png)

    * Para que a mudança no passo 43 faça sentido, executo o container a partir da pasta Arquivos, com a montagem de dois volumes: um de escrita e um de saída. Esses volumes possibilitam a leitura correta do *csv* na pasta raiz, em conjunto com a escrita da cópia para a atual e a saída das respostas do script de maneira correta.

46. Segue a demonstração dos arquivo de solução criados com sucesso:

    * *respostas.txt*:

    ![Etapa 4 - Solução txt](../Evidencias/Etapa4_SolucaoRespostas.png)

    * *Q4.png*

    ![Etapa 4 - Solução Q4](../Evidencias/Etapa4_SolucaoQ4.png)

    * *Q5.png*

    ![Etapa 4 - Solução Q5](../Evidencias/Etapa4_SolucaoQ5.png)

47. E deste modo fica o diretório após a conclusão e implantação das etapas 3 e 4, o mesmo sendo modificado na próxima etapa:

    ![Etapa 4 - Diretório Antigo](../Evidencias/Etapa4_DiretorioAntigo.png)

48. Avançando para a última etapa, inicio a elaboração do arquivo Compose com a finalidade de conectar os dois containers e rodar a aplicação completa:

    ![Etapa 5 - Arquivo Compose](../Evidencias/Etapa5_ArquivoCompose.png)

    * Explicação: O arquivo consiste em dois serviços, um para o *etl* e outro para o *job*. Possui a implantação das builds com os contextos localizados em suas pastas específicas e os volumes com as saídas na pasta própria *'/volume'*. Ambos serviços possuem um comando para serem inicializados, com o serviço *job* dependendo do *etl* para tal feito.

49. Em seguida, utilizo o seguinte comando para rodar o Compose:

    ![Etapa 5 - Execucao Compose](../Evidencias/Etapa5_ExecucaoCompose1.png)

    * Obtendo o seguinte resultado:

    ![Etapa 5 - Resultado Erro](../Evidencias/Etapa5_ExecucaoErro.png)

    * Pode se observar que houve um erro no serviço *job* em que não reconheceu o arquivo *csv_limpo*.

50. Para solucionar o problema, emprego o comando para derrubar o Compose:

    ![Etapa 5 - Derrubando Compose](../Evidencias/Etapa5_DerrubandoCompose.png)

51. E no código do arquivo *job.py* como solução, insiro a pasta volume na leitura do *csv*:

    ![Etapa 5 - Solução Erro](../Evidencias/Etapa5_SolucaoErro.png)

52. Realizo o mesmo procedimento para as partes do algoritmo em que há  escrita de arquivos a fim de que estes fiquem localizados na pasta */volume* ao executar o Compose:

    * Exportação do *csv_limpo.csv* no ETL:

    ![Etapa 5 - Volume ETL ](../Evidencias/Etapa5_VolumeArquivo1.png)

    * Exportação do *respostas.txt* no Job:

    ![Etapa 5 - Volume Respostas ](../Evidencias/Etapa5_VolumeArquivo2.png)

    * Exportação do *Q4.png*:

    ![Etapa 5 - Volume Q4 ](../Evidencias/Etapa5_VolumeArquivo3.png)
    
    * Exportação do *Q5.png*:

    ![Etapa 5 - Volume Q5 ](../Evidencias/Etapa5_VolumeArquivo4.png)

53. Consequentemente, para finalizar a etapa, inicializo novamente o comando do Compose, desta vez sendo executado com sucesso:

    ![Etapa 5 - Compose Correto ](../Evidencias/Etapa5_ExecucaoCompose2.png)

54. Segue a confirmação da criação das imagens com o Compose rodando normalmente:

    ![Etapa 5 - Compose Rodando](../Evidencias/Etapa5_ComposeRodando.png)

55. Realizados todos os processos, o desafio é finalizado com o seguinte diretório:

    ![Etapa 5 - Diretório Finalizado](../Evidencias/Etapa5_DiretorioNovo.png)

56. Desta maneira, chegamos ao fim do **Desafio da Sprint 3**. Agradeço por acompanharem o guia passo a passo do projeto que foi desenvolvido para aplicar os conhecimentos adquiridos na *sprint*. Estou aberto a novas sugestões de melhoria e feedback, que contribuirão muito para meu desenvolvimento profissional.