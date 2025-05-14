1. Criar Base de Dados:
`scenario-convert.sh --sumo2db -i proj.net.xml`

2. Adicionar rotas à base de dados:
`scenario-convert.sh --sumo2db -i proj.rou.xml -d proj.db`

3. Criar cenário mosaic (altera o .net.xml, guardar e copiar o original depois do comando):
`scenario-convert.sh --db2mosaic -d .\proj.db`

Executar cenário:
`./mosaic.bat -s scenario -w 0`
ou
`mosaic.sh -s scenario -w 0`

Notas:
- Alterar infos dos carros no `mapping/mapping_config.json`
- Atualizar a base de dados se alterar o .net.xml ou as rotas

"C:\apache-maven\apache-maven-3.9.9\bin\mvn" clean package
