scenario-convert.sh --db2mosaic -d .\steglitz.db (CRIA PASTA DO MOSAIC)

scenario-convert.sh --sumo2db -i sievekingplatz.net.xml

scenario-convert.sh --sumo2db -i sievekingplatz.rou.xml -d sievekingplatz.db

"C:\apache-maven\apache-maven-3.9.9\bin\mvn" clean package
