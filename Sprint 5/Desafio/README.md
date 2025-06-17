# Desafio 
Nesta Sprint, o desafio consistiu em juntar todos os conhecimentos adquiridos no Programa de Bolsas para realizar a construção de um Data Lake de Filmes e Séries, iniciando nas etapas de Ingestão de Dados. A finalidade era introduzir bases de dados de filmes/séries num *Bucket* na **AWS** e complementá-las com o auxílio de requisições da API **TMDB** por meio de uma função *AWS lambda*, se baseando em questionamentos criados para análise nas próximas etapas. 

## Etapas
Abaixo consta o passo a passo de como foram aplicadas as etapas de atividades sobre o projeto desde a Ingestão dos arquivos locais no Bucket até a Ingestão com a API a partir da função Lambda na AWS.

1. O primeiro passo do desafio foi realizar o download do arquivo *Filmes+e+Series.zip*, extraí-lo e verificar os arquivos *movies.csv.* e *series.csv*.

2. Em seguida depois de obter a compreensão completa do desafio, efetuei a preparação do ambiente com a criação do arquivo Python [*ingestaocsv.py*](./Arquivos/etapa1/ingestaocsv.py).


3. Logo depois, na AWS elaborei a criação de um novo bucket para o Desafio com o nome de *"datalake-lucasrafael"*:

![Criação do Bucket](../Evidencias/Etapa1_CriacaoBucket.png)

4. Iniciando a etapa 1 com a importação das bibliotecas:

    ![Etapa 1 - Importação](../Evidencias/Etapa1_Imports.png)

5. Efetuo a leitura dos dados dos arquivos CSV:
    * *movies.csv*:
    ![Etapa 1 - Leitura Movies](../Evidencias/Etapa1_LeituraMovies.png)

    * Base de dados com mais de um milhão de linhas com 15 colunas de informação.

    * *series.csv*:
    ![Etapa 1 - Leitura Series](../Evidencias/Etapa1_LeituraSeries.png)

    * Base de dados com mais de 400 mil de linhas com 16 colunas de informação.

6. Posteriormente implanto a conexão com o S3 na AWS:

    ![Etapa 1 - Conexão AWS](../Evidencias/Etapa1_ConexaoS3.png)

7. Pós efetuada a conexão, crio um algoritmo para Upload dos arquivos no Bucket, testando localmente antes de restringir a execução apenas ao Dockerfile:

    ![Etapa 1 - Upload Arquivos](../Evidencias/Etapa1_UploadArquivos.png)

8. Com a execução, é possível verificar no Bucket se os arquivos foram enviados com sucesso:

    * Arquivo Movies:

    ![Etapa 1 - S3 Movies](../Evidencias/Etapa1_ArquivoMoviesS3.png)

    * Arquivo Series:

    ![Etapa 1 - S3 Series](../Evidencias/Etapa1_ArquivoSeriesS3.png)

    * Repare que o caminho dos diretórios dentro do Bucket foram formados corretamente para cada um dos arquivos:

    ![Etapa 1 - Caminho S3 Movies](../Evidencias/Etapa1_CaminhoMovies.png)

    ![Etapa 1 - Caminho S3 Series](../Evidencias/Etapa1_CaminhoSeries.png)

9. Depois que o teste foi executado com êxito, podemos criar o container no Docker. Primeiro, elaboramos o arquivo Docker:

    ![Etapa 1 - Dockerfile](../Evidencias/Etapa1_Dockerfile.png)

    * Copia os arquivos: *ingestaocsv.py* com o algoritmo, *requirements.txt* com as bibliotecas para instalação e *.env* com as credenciais de acesso da Amazon.

10. Criamos a imagem:

    ![Etapa 1 - Build](../Evidencias/Etapa1_Build.png)

11. E posso rodar o container com sucesso:

    ![Etapa 1 - Container](../Evidencias/Etapa1_Container.png)

12. Como apresentado na conclusão da execução do container, podemos conferir no Bucket se os arquivos realmente foram transmitidos:
    
    Movies:

    ![Etapa 1 - Arquivo Movies no S3 Docker](../Evidencias/Etapa1_MoviesS3Docker.png)

    Series:

    ![Etapa 1 - Arquivos Series no S3 Docker](../Evidencias/Etapa1_SeriesS3Docker.png)

    * OBS: Enviado com diferença de data em relação ao teste.

13. Finalizada a etapa 1, segue o código completo implementado:

    * Parte 1:

    ![Etapa 1 - Código Completo pt.1](../Evidencias/Etapa1_CodCompleto_Pt1.png)

    * Parte 2:

    ![Etapa 1 - Código Completo pt.2](../Evidencias/Etapa1_CodCompleto_pt2.png)

14. Partindo para a etapa 2, realizei a escolha dos questionamentos necessários para as análises que serão utilizadas nas etapas consecutivas, optando por abordar questões associadas a filmes que envolvessem comparações entre os dois gêneros exigidos(Crime e Guerra) e abrangessem a busca de informações pertinentes relacionadas a momentos importantes da história e do cinema.
    ### Questões
    1. Quais são as franquias mais populares nos gêneros Crime e Guerra e como se comparam entre si?
       
    2. Quais grandes guerras são as mais retratadas nos filmes ao longo do tempo?

    3. Da grande guerra mais recorrente nos filmes, quais possuem a melhor nota de crítica?

    4. No período de guerra, houve maior ou menor produção de filmes de crime? Como isso impactou na avaliação do público?

    5. Entre os filmes do gênero Crime, qual tema/crime é mais retratado?

    6. Como a popularidade de filmes de Crime evoluiu no decorrer das décadas? 

* Levando em consideração as possibilidades que a API do TMDB oferece, priorizei por buscar informações que pudessem abranger boa parte das soluções das questões de análise. Com base nesse propósito, realizei a seleção de filmes dos dois gêneros que pertencessem à franquias, focando em solucionar a primeira questão. Para as questões 2 e 3, selecionei dados dos filmes de guerra, com a adoção de atributos que permitissem a análise de informações de sinopse, notas, tempo e palavras-chave. Por fim, para as questões restantes (4, 5 e 6), foi constituída uma seleção semelhante à última citada, nesta ocasião orientada para os filmes do gênero crime. Pretendi ser o mais abrangente possível, mas com a alternativa de alterar as análises em caso de necessidades no prosseguimento do desafio final.

15. Como próximo passo desta etapa, para realizar a ingestão das requisições da API, iniciei criando o arquivo [*ingestaoapi.py*](./Arquivos/etapa2/ingestaoapi.py) e importando as bibliotecas necessárias:

    ![Etapa 2 - Imports](../Evidencias/Etapa2_Imports.png)

    * O import *tmdbv3api* remete a API do TMDB, com seus mecanismos de busca que utilizaremos durante o algoritmo: *TMDb*, *Movie*, *Collection*, *Genre*, *Discover* e *Keyword*.

16. Pretendendo facilitar os testes na AWS Lambda em passos posteriores, busquei testar localmente o algoritmo assim como na etapa 1. Para isso implantei a conexão com a AWS e instanciei os requisitos da API:

    ![Etapa 2 - Conexão AWS](../Evidencias/Etapa2_ConexaoS3_e_API.png)

    * *dotenv* será removido na AWS Lambda.

17. Seguindo, solicitei uma requisição para encontrar a identificação dos gêneros que precisamos na solução:

    ![Etapa 2 - Busca Gêneros](../Evidencias/Etapa2_BuscaGeneros.png)

    * Podemos identificar o gênero Crime com ID: 80 e o gênero guerra(War) com ID: 10752. Estes IDS serão importantes nos passos seguintes, por isso já instaciamos:

    ![Etapa 2 - Variaveis Genero](../Evidencias/Etapa2_VariaveisGenero.png)

18. Partindo para a busca focalizada nas análises, começamos com os filmes do gênero Crime:

    * Esse algoritmo busca filmes por meio do gênero, ordenando por popularidade em ordem decrescente. No *try* adquire os detalhes dos filmes, busca as palavras-chave(*keywords*) e insere todos os dados numa lista(no segmento do *append*). A variável *batch_tamanho* serve para limitar 100 registros num único arquivo JSON..  

    ![Etapa 2 - Busca Crime Testes 1](../Evidencias/Etapa2_Busca_Crimes_Teste1.png)

     * No segmento seguinte, segue o código utilizado para exemplo de teste local salvando num arquivo JSON: 

    ![Etapa 2 - Busca Crime Testes 2](../Evidencias/Etapa2_Busca_Crimes_Teste2.png)

    * Como resultado de exemplo de arquivo JSON, confiro se os dados foram retornados de forma correta:

    ![Etapa 2 - JSON Teste](../Evidencias/Etapa2_ExemploJSON_Crimes.png)

19. Considerando que o salvamento do arquivo JSON foi realizado com sucesso, podemos modificar o código para implantar o teste direto no Bucket:

    ![Etapa 2 - Busca Crime Teste](../Evidencias/Etapa2_Busca_Crimes_pt1.png)

    ![Etapa 2 - Busca Crime Teste 2](../Evidencias/Etapa2_Busca_Crimes_pt2.png)

    * No final deste algoritmo temos a instância do nome de arquivo em conjunto com o diretório exigido e o carregamento ao Bucket.

20. Procedendo para os próximos passos, escolho empreender os métodos restantes de busca para depois efetuar os testes na AWS. Agora, segue o método apontado para os filmes de guerra, de forma semelhante ao anterior, executado com êxito:  

    * Parte 1:

    ![Etapa 2 - Busca Guerra pt1](../Evidencias/Etapa2_Busca_Guerra_pt1.png)

    * Parte 2:

    ![Etapa 2 - Busca Guerra pt2](../Evidencias/Etapa2_Busca_Guerra_pt2.png)

21. Como último teste, realizo a busca baseada nas Franquias de Filmes:  
    
    * Com algumas diferenças em relação aos dois últimos algoritmos, com um dicionário para dividir os gêneros e as franquias, a propriedade *belongs_to_connection* para apresentar os filmes que pertencem a coleções(franquias) e o batch(limite) sendo representado com outra manipulação que reconheça o formato de dicionário:

    * Parte 1

    ![Etapa 2 - Busca Franquias pt1](../Evidencias/Etapa2_BuscaFranquias_pt1.png)

    * Parte 2

    ![Etapa 2 - Busca Franquias pt2](../Evidencias/Etapa2_BuscaFranquias_pt2.png)

    * Segue o resultado da execução:

    ![Etapa 2 - Resultado Testes Franquias](../Evidencias/Etapa2_ResultadoTeste_Franquias.png)

22. Com os arquivos locais exercendo suas funções corretamente, adquiro a possibilidade de migrar para a Função Lambda. Por isso, exerço a criação de uma camada na AWS Lambda com o objetivo de implementar as libs do *tmdbv3api*:

    ![Etapa 2 - Criação Camada AWS](../Evidencias/Etapa2_Criacao_Camada_AWS.png)

23. Para que a camada seja operada adequadamente criamos as pastas e comprimimos os arquivos no terminal do sistema local:
    
    * Criação da pasta *tmdb_layer*:

    ![Etapa 2 - Criando Pastas Lib](../Evidencias/Etapa2_Criando_Pasta_Lib.png)

    * Criação da pasta *python*:

    ![Etapa 2 - Criando Pastas Lib Python](../Evidencias/Etapa2_Criacao_Pasta_Lib2.png)

    * Instalação dos arquivos na pasta:

    ![Etapa 2 - Instalação Arquivos na Pasta](../Evidencias/Etapa2_Instalando_Lib.png)

    * Compressão dos arquivos no *tmdb_layer.zip*:

    ![Etapa 2 - Compressão Arquivos](../Evidencias/Etapa2_CompressaoCamada.png)

24. Seguindo para a criação da Lambda na AWS:

    ![Etapa 2 - Criação Lambda](../Evidencias/Etapa2_Criacao_Func_Lambda.png)

    
25. Com a camada implantada, configuro a função, aumentando a memória de armazenamento para 1024 MB e o tempo limite de execução de 12 minutos:

    ![Etapa 2 - Configuração Lambda](../Evidencias/Etapa2_Config_Funcao.png)

26. Em seguida, adiciono a camada elaborada para a função:

    ![Etapa 2 - Adição Camada](../Evidencias/Etapa2_Camada_Funcao.png)

27. Concluindo a preparação do ambiente da AWS, adicionamos uma política de IAM à função Lambda para obter a permissão de acesso à plataforma do S3:

    ![Etapa 2 - Política IAM](../Evidencias/Etapa2_Role_IAM_AWS.png)

28. Com o ambiente pronto, copio o código local para o espaço da função na Lambda realizo o deploy e o teste, obtendo o seguinte erro:

    ![Etapa 2 - Erro Lambda](../Evidencias/Etapa2_Erro_AWS_Lambda.png)

    * Com este retorno, é possível identificar que o erro representa um problema de importação da API, a função Lambda não reconhece o módulo.

29. Para realizar o tratamento do erro e concluir o projeto, localmente modifico o algoritmo, encapsulando as funções de processamento dos dados, impondo uma apresentação melhorada do código final:

    * Código de Importação, Conexão com Bucket, API e funções do TMDB:

    ![Etapa 2 - Código Final](../Evidencias/Etapa2_CodFinal.png)

    * Implementação da Função Lambda com o uso da váriavel *event* para fins de execução:

    ![Etapa 2 - Função Lambda](../Evidencias/Etapa2_FuncLambda.png) 

     * Funcão para o processamento das franquias:

     ![Etapa 2 - Função Franquias](../Evidencias/Etapa2_FuncFranquias.png)

     ![Etapa 2 - Função Franquias Parte 2](../Evidencias/Etapa2_FuncFranquias_Cont.png)

     ![Etapa 2 - Função Franquias Final](../Evidencias/Etapa2_FuncFranquias_Final.png)

     * Implementação da Função de processamento dos filmes de guerra:

     ![Etapa 2 - Função Guerra](../Evidencias/Etapa2_FuncGuerra.png)

     ![Etapa 2 - Função Guerra Final](../Evidencias/Etapa2_FuncGuerra_Final.png)

    * Implementação da Função de processamento dos filmes de crime:

    ![Etapa 2 - Função Crime](../Evidencias/Etapa2_FuncCrimes.png)

    ![Etapa 2 - Função Crime Final](../Evidencias/Etapa2_FuncCrimes_Final.png)

30. Continuando para os testes na Lambda, realizamos a execução da função por partes:

    * Função das franquias:

    ![Etapa 2 - Teste AWS Franquias](../Evidencias/Etapa2_Teste_AWS_Franquias.png)

    * Função dos filmes de guerra:

    ![Etapa 2 - Teste AWS Guerra](../Evidencias/Etapa2_Teste_AWS_Guerra.png)

    * Função dos filmes de crime:

    ![Etapa 2 - Teste AWS Crime](../Evidencias/Etapa2_Teste_AWS_Crimes.png)

31. Finalizando as etapas do desafio, com o encaminhamento conluído, no S3, verifico se os arquivos foram endereçados corretamente:

    * Diretório de franquias de crime:

    ![Etapa 2 - Bucket Franquias Crime](../Evidencias/Etapa2_FranquiasAWS_1.png)

    * Diretório de franquias de guerra:

    ![Etapa 2 - Bucket Franquias Guerra](../Evidencias/Etapa2_FranquiasAWS_2.png)

    * Diretório de filmes de guerra:

    ![Etapa 2 - Bucket Filmes Guerra](../Evidencias/Etapa2_GuerraAWS.png)

    * Diretório de filmes de crime:

    ![Etapa 2 - Bucket Filmes Crime](../Evidencias/Etapa2_CrimeAWS.png)

32. Encerradas as explicações, chego ao fim do **Desafio da Sprint 5**. Agradeço por acompanharem o guia passo a passo do projeto que foi desenvolvido para aplicar os conhecimentos adquiridos nesta *sprint*. Estou aberto a novas sugestões de melhoria e feedback, que contribuirão muito para meu desenvolvimento profissional.