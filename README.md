# spark-tests
Example of spark application to find soccer players of a certain position and country in a fifa data set
*Usando vm cloudera (CDH 5.13) com spark 1.6.0*

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
**Visualizar a saída**
```
hadoop fs -cat /user/cloudera/saida_job/part-00000
```
