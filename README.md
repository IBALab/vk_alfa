# vk_alfa

scalaMatch :
Usage: scalaMatch <path to directory with vk users> <path to file with bank users> <output path>
при запуске проверить чтобы не существовало папки Output path : hdfs dfs -ls /tmp
если в путях явно не указывается file:// , то автоматически пути ищутся в hdfs.
Запуск на всех людях(количество файлов с различнами датами рождения 15871)
./usr/bin/spark-submit  --master yarn --class scalaMatch  /opt/users/vk/lib/vk_alfa-0.0.1-SNAPSHOT-jar-with-dependencies.jar  /tmp/users_10_20161205 /tmp/bank.txt /tmp/users_10_20161205_result_yarn


interestsClusterization :
Usage: interestsClusterization <path to matched file > <path to directory with vk users> <output path>
path to matched file - это путь к файлу <output path>/part-00000 после выполнения scalaMatch 
./usr/bin/spark-submit  --master yarn --class interestsClusterization /opt/users/vk/lib/vk_alfa-0.0.1-SNAPSHOT-jar-with-dependencies.jar  /tmp/users_15871_20161205_res_yarn_save/part-00000 /tmp/users_15871_20161205 /tmp/interestsClusterization_15871

joinAllVkBank  :
Usage: joinAllVkBank <path to matched file> <path to directory with vk users> <path to files with vk description fields>  <path to interest clusterization file > <output-path>
./usr/bin/spark-submit  --master yarn --class joinAllVkBank  /opt/users/vk/lib/vk_alfa-0.0.1-SNAPSHOT-jar-with-dependencies.jar  /tmp/users_15871_20161205_result_yarn/part-00000 /tmp/users_15871_20161205 /tmp/vk_databases /tmp/interestsClusterization_15871/id_topic/part-00000 /tmp/joinAllVkAndBank_15871


