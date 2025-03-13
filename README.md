1. Criar Base de Dados:
`scenario-convert.sh --sumo2db -i sievekingplatz.net.xml`

2. Adicionar rotas à base de dados:
`scenario-convert.sh --sumo2db -i sievekingplatz.rou.xml -d sievekingplatz.db`

3. Criar cenário mosaic (altera o .net.xml, guardar e copiar o original depois do comando):
`scenario-convert.sh --db2mosaic -d .\steglitz.db`

Executar cenário:
`mosaic.bat -s tutorial -w 0`
ou
`mosaic.sh -s tutorial -w 0`

Notas:
- Alterar infos dos carros no `mapping/mapping_config.json`
- Atualizar a base de dados se alterar o .net.xml ou as rotas
