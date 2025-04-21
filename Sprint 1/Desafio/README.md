# Desafio 
Nesta Sprint, o desafio consistiu em analisar uma base de dados, aplicar conceitos de normalização(formas normais), gerar uma modelagem relacional para depois converter em uma modelagem dimensional.

## Etapas
Abaixo consta o passo a passo de como foram aplicadas as formas normais, juntamente com o modelo lógico da modelagem relacional, tal como a modelagem dimensional solicitada no desafio.

1. O primeiro passo foi a preparação do ambiente e o download do arquivo *concessionaria.zip* e extração dos arquivos. 

2. Logo em seguida foi a criação da conexão com o banco *concessionaria.sqlite* pelo DBeaver.

3. Na base de dados só constava uma tabela com o nome **tb_locacao** com as seguintes colunas:
    
    ![Colunas tb_locacao](../Evidencias/ColunasTb_locacao.png)

4. Crio o script SQL "[Desafio1Normalizacao.sql](./Arquivos/Desafio1Normalização.sql)" para realizar as soluções de normalização. Ao fazer a consulta da tabela para iniciar a normalização, são retornados os seguintes dados:

    ![Normalização Análise](../Evidencias/Normalizacao_AnaliseBanco.png)

5. Após fazer algumas consultas para verificar se havia dados repetitivos me deparo com um repetição de algumas colunas com o uso do **DISTINCT**:
    * Identifiquei que a coluna **kmCarro** possui mais de uma locação de dados para o mesmo **idCarro**, criando registros a mais, o que poderia causar um problema na separação entre as entidades das tabelas.

    ![Normalização - Verificação de Repeticão](../Evidencias/NormalizacaoAnalise1FN.png)
    
    * Para resolver essa situação, foi criada uma solução nos passos 12 ao 17.

6. Posteriormente foram realizadas as criações de tabelas para efetuar a separação de entidades e aplicar a Segunda Forma Normal. Primeiramente foi realizado o  **CREATE TABLE** da *TbCliente*:
    * Obs: Utilizei o **DROP TABLE** como uma boa prática para deletar a tabela caso já exista no respectivo banco de dados.

    ![Normalização - Criação TbCliente](../Evidencias/Normalizacao_Criacao_TbCliente.png)

7. Repeti o processo criando a tabela *TbCarro*: 

    ![Normalização - Criação TbCarro](../Evidencias/Normalizacao_CriacaoTabelas.png)

8. Copiei o código de criação para utilizar nas tabelas seguintes:
    * Tabela para os itens de Combustível(*TbCombustivel*):

    ![Normalização - Criacao TbCombustivel](../Evidencias/Normalizacao_Criacao_TbCombustivel.png)

    * E a tabela para os dados dos vendedores:
    
    ![Normalização - Criacao TbVendedor](../Evidencias/Normalizacao_Criacao_TbVendedor.png)

9. A seguir era necessário inserir os dados da *tb_locacao* nas tabelas criadas para preenchê-las, sempre utilizando o **DISTINCT**, começando pela *TbCliente*:
    
    ![Normalização - Inserção Dados TbCliente](../Evidencias/Normalizacao_InsercaoDados_TbCliente.png)

10. Ao inserir faço o **SELECT** certificando de que dados estão corretos:

    ![Normalização - Conferência Dados TbCliente](../Evidencias/Normalizacao_ConferenciaDados_TbCliente.png)

11. Seguidamente repeti o caminho de inserção para a *TbCarro*, acabando por reconhecer o problema que havia identificado nos passos anteriores:
    
    ![Normalização - Erro Dados TbCarro](../Evidencias/Normalizacao_Erro_Repeticao.png)

    * O erro mostrado na execução da consulta constata que a coluna *idCarro* por ser chave primária e possuir a *constraint UNIQUE* falhou, pois havia dados repetidos na tabela. Como a chave primária serve como identificador exclusivo para cada linha, não permite duplicadas.

12. Para solucionar o erro causado pela coluna *kmCarro* e eliminar duplicatas, efetuei a separação criando uma tabela exclusiva para o registro das Kilometragens:
    * Utilizando o *idCarro* para poder juntas as tabelas numa eventual consulta:

    ![Normalização Criacão TbKmCarro](../Evidencias/Normalizacao_Criacao_TbKmCarro.png)
    
13. Continuando na solução, deletei os dados da *TbCarro* para criar uma nova sem o uso da coluna *kmCarro*:
    
    ![Normalização Criação TbCarro](../Evidencias/Normalizacao_Criacao_TbCarro.png)

14. Depois, novamente refiz o processo de inserção dos dados na *TbCarro* se concluindo da forma correta:

    ![Normalização Inserção Dados TbCarro](../Evidencias/Normalizacao_InsercaoDados_TbCarro.png)

15. Me certifico de que os dados estão realmente corretos:

    ![Normalização Conferência Dados TbCarro](../Evidencias/Normalizacao_ConferenciaDados_TbCarro.png)

16. Implementei o mesmo processo de inserção e conferência através de consulta para a nova tabela *TbKmCarro*:
    * Inserção dos dados: 

    ![Normalização Inserção Dados TbKmCarro](../Evidencias/Normalizacao_InsercaoDados_TbKmCarro.png)

    * Consulta para Conferência dos respectivos dados:

    ![Normalização Conferência Dados TbKmCarro](../Evidencias/Normalizacao_ConferenciaDados_TbKmCarro.png)

17. Completada a solução do pequeno problema, preparei uma consulta com *JOIN* para apresentar os dados de Kilometragem em conjunto com os dados do Carro:

    ![Normalização Conferência Dados TbCarro](../Evidencias/Normalizacao_ConferenciaDados_TbCarro_JOIN_TbKmCarro.png)

18. Após esse processo, é necessário prosseguir com a inserção de dados nas tabelas restantes, continuando com a *TbCombustivel*:

    ![Normalização Inserção Dados TbCombustivel](../Evidencias/Normalizacao_InsercaoDados_TbCombustivel.png)

19. Conferindo se os dados estão corretos:

    ![Normalização Conferência Dados TbCombustivel](../Evidencias/Normalizacao_ConferenciaDados_TbCombustivel.png)

20. Repetindo o método de inserção para a tabela restante *TbVendedor*:

    ![Normalização Inserção Dados TbVendedor](../Evidencias/Normalizacao_InsercaoDados_TbVendedor.png)

21. Efetuando a conferência pela consulta:

    ![Normalização Conferência Dados TbVendedor](../Evidencias/Normalizacao_ConferenciaDados_TbVendedor.png)

22. Agora que foram separadas as entidades e eliminadas as dependências parciais, o próximo passo é eliminar as dependências transitivas. Para este processo criarei uma nova tabela com os dados de locação sem colunas desnecessárias:

    ![Normalização Criação TbLocacao](../Evidencias/Normalizacao_Criacao_TbLocacao.png)
    * A tabela *TbLocacao* possui **FOREIGN KEY**s para referenciar as outras tabela da nova base de dados

23. Próximo de completar a normalização, insiro os dados na nova tabela de Locação: 
    
    ![Normalização Inserção Dados TbLocacao](../Evidencias/Normalizacao_InsercaoDados_TbLocacao.png)

24. Por último, realizo a consulta para confirmar os dados, completando assim a aplicação da normalização:

    ![Normalização Conferência Dados TbLocacao](../Evidencias/Normalizacao_ConferenciaDados_TbLocacao.png)
    
    * Por fim, utilizo o **DROP TABLE** para deletar os dados da antiga *tb_locacao*

25. Finalizada a etapa, criei o modelo lógico do processo utilizando o [draw.io](https://app.diagrams.net/):

    ![Modelagem Relacional](./Arquivos/1ModelagemRelacional.png)

26. O passo seguinte para a conclusão do desafio é criar uma modelagem dimensional a partir da base Relacional implantada, por meio da criação de Views para o Fato e as Dimensões. Portanto, crio um novo Script SQL "[Desafio2ModelagemDimensional.sql](./Arquivos/Desafio2ModelagemDimensional.sql)" e inicio a modelagem criando uma View da Dimensão Carro:

    ![M. Dimensional Criação DimCarro](../Evidencias/M_Dimensional_Criacao_DimCarro.png)

    * Assim como no passo 6, utilizo o **DROP VIEW** como boa prática para deletar a View caso seja existente antes de criar uma nova.

27. Após a criação, realizo o processo de conferência dos dados da View empregando o **SELECT**:

    ![M. Dimensional Conferência Dados DimCarro](../Evidencias/M_Dimensional_ConferenciaDados_DimCarro.png)

28. Em seguida, sigo efetuando a criação das Dimensões, sendo desta vez a Dimensão KmCarro, servindo como uma hierarquia de Dimensão da *Dim_Carro*(Modelo Snow Flake):

    ![M. Dimensional Criação DimKmCarro](../Evidencias/M_Dimensional_Criacao_DimKmCarro.png)

29. Realizo a certificação de que a View da *Dim_KmCarro* está correta:

    ![M. Dimensional Conferência Dados DimKmCarro](../Evidencias/M_Dimensional_ConferenciaDados_DimKmCarro.png)

30. Seguindo com o processo de criação das Views, retorno com o **CREATE VIEW** da *Dim_Cliente*:

    ![M. Dimensional Criação DimCliente](../Evidencias/M_Dimensional_Criacao_DimCliente.png)

31. Conferindo se os dados da View condizem com os necessários:

    ![M. Dimensional Conferência Dados DimCliente](../Evidencias/M_Dimensional_ConferenciaDados_DimCliente.png)

32. Criando a próxima View da *Dim_Combustivel*:

    ![M. Dimensional Criação DimCombustivel](../Evidencias/M_Dimensional_Criacao_DimCombustivel.png)

33. Analisando os dados da View criada:

    ![M. Dimensional Conferência Dados DimCombustivel](../Evidencias/M_Dimensional_ConferenciaDados_DimCombustivel.png)

34. Efetuando a criação da View subsequente *Dim_Vendedor*:

    ![M. Dimensional Criação DimVendedor](../Evidencias/M_Dimensional_Criacao_DimVendedor.png)

    * Implementei um **CASE** para diferenciar o sexo do Vendedor entre o masculino e o feminino, pois na consulta retornava apenas os números 0 e 1 para diferenciação.

35. Realizo a conferência para me assegurar de que a View está sendo retornada corretamente juntamente com a modificação da coluna *sexodoVendedor*:

    ![M. Dimensional Conferência ](../Evidencias/M_Dimensional_ConferenciaDados_DimVendedor.png)

36. Em seguida, tenho a ideia de criar um recurso que seria a Dimensão Data, para organizar as datas de Locação e Entrega de uma forma mais clara, facilitando a manipulação destes dados na nova View:

    ![M. Dimensional Criação DimData](../Evidencias/M_Dimensional_Criacao_DimData.png)
    
    * Faço o uso da *strftime*, função que serve para formatar valores de data numa string específica.  

37. Ocorrendo a criação, verifico se os dados estão corretos, identificando um novo problema:

    ![M. Dimensional Erro DimData](../Evidencias/M_Dimensional_Problema_DimData.png)

    * As colunas de Dia/Mes/Ano estão retornando valores nulos e os formatos das colunas *DataLocacao* e *DataEntrega* não estão sendo reconhecidos como **DATETIME**.

38. Para solucionar estas disfunções do código, confiro qual tipo de dados os atributos *DataLocacao* e *DataEntrega* estão reconhecendo:

    ![M. Dimensional Conferência FormatoDatas](../Evidencias/M_Dimensional_Conferencia_FormatoDatas.png)

    * Percebe-se que o formato reconhecido pelos atributos é o **INTEGER**.

39. Realizando uma pesquisa, é possível observar que se torna necessário realizar uma modificação no formato **INTEGER** para que se torne uma data novamente. Com a utilização do **UPDATE**  na *TbLocacao* acrescentando a função *substr*, é permitida a formatação destes dados nas duas colunas:

    ![M. Dimensional Correção Formato Datas](../Evidencias/M_Dimensional_Correcao_FormatoDatas.png)

40. Em seguida, executo um novo teste de conferência confirmando que os dados da Dimensão estão corretos e a solução foi concluída:

    ![M. Dimensional Conferencia Dim_Data](../Evidencias/M_Dimensional_ConferenciaDados_DimData.png)

41. Como últimos passos da Modelagem Dimensional, elaboro a View que compreende ao *FatoLocacao* sintetizando os relacionamentos existentes entre as outras dimensões:
    
    ![M. Dimensional Criação FatoLocacao](../Evidencias/M_Dimensional_Criacao_FatoLocacao.png)

42. Finalizo este processo, me certificando de que os dados da Tabela Fato estão corretos:

    ![M. Dimensional Conferência Dados FatoLocacao](../Evidencias/M_Dimensional_ConferenciaDados_FatoLocacao.png)

43. E completo o desafio com a criação do Modelo Lógico da Modelagem Dimensional utilizado o padrão SnowFlake:

    ![M. Dimensional Modelo Lógico](./Arquivos/2ModelagemDimensional.png)

44. Em virtude de todos os conceitos que foram aprendidos até o momento e de todos os passos que foram apresentados e mencionadas para as soluções, chego a conclusão do **Desafio da Sprint 1**.