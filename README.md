# spark-tests
Example of spark application to find soccer players of a certain position and country in a fifa data set

**Copiar o dataset e o jar do app (Em resources) para o diretório tmp no HDFS**
```
hadoop fs -put /tmp/fifa19.csv /tmp
```
```
hadoop fs -put /tmp/spark-tests.jar /tmp
```
**Executar o job**
```
spark-submit --class spark.tests.PlayersByCountrySpark --master local /tmp/spark-tests.jar /tmp/fifa19.csv saida_job ST Portugal
```
