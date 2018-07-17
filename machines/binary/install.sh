#!/bin/sh

systemctl start ntpd

echo " "
echo "---------------------------------------------------------------------------------------------------------------"
echo "----- updating hosts file"
echo "---------------------------------------------------------------------------------------------------------------"
echo " "

sudo mv /tmp/install/hosts /etc/hosts
sudo chown root:root /etc/hosts

echo " "
echo "---------------------------------------------------------------------------------------------------------------"
echo "----- updating hostname"
echo "---------------------------------------------------------------------------------------------------------------"
echo " "

hostnamectl --static set-hostname jdbc-binary

echo " "
echo "---------------------------------------------------------------------------------------------------------------"
echo "----- get ambari repo"
echo "---------------------------------------------------------------------------------------------------------------"
echo " "

wget -nv http://public-repo-1.hortonworks.com/ambari/centos7/2.x/updates/2.7.0.0/ambari.repo -O /etc/yum.repos.d/ambari.repo

echo " "
echo "---------------------------------------------------------------------------------------------------------------"
echo "----- running yum install"
echo "---------------------------------------------------------------------------------------------------------------"
echo " "

yum install ambari-server ambari-agent -y

echo " "
echo "---------------------------------------------------------------------------------------------------------------"
echo "----- updating ambari-agent.ini"
echo "---------------------------------------------------------------------------------------------------------------"
echo " "

sed -i "s/^hostname=localhost/hostname=jdbc-binary.hdp.local/g" /etc/ambari-agent/conf/ambari-agent.ini

# echo " "
# echo "---------------------------------------------------------------------------------------------------------------"
# echo "----- install overops"
# echo "---------------------------------------------------------------------------------------------------------------"
# echo " "

# curl -sL /dev/null http://get.takipi.com | sudo bash /dev/stdin -i --sk=S36579#bLFSWU3y+KL7s5kP#Ulu1V6TctQOeemsAKFeWiK09lzHRHEkX5HGbx7pL3N4=#386e

# export JAVA_TOOL_OPTIONS="-agentlib:TakipiAgent"

echo " "
echo "---------------------------------------------------------------------------------------------------------------"
echo "----- install mysql jdbc driver"
echo "---------------------------------------------------------------------------------------------------------------"
echo " "

sudo yum install mysql-connector-java* -y
ln -s /usr/share/java/mysql-connector-java.jar /var/lib/ambari-server/resources/mysql-connector-java.jar


echo " "
echo "---------------------------------------------------------------------------------------------------------------"
echo "----- running ambari setup"
echo "---------------------------------------------------------------------------------------------------------------"
echo " "

ambari-server setup --silent --java-home=$JAVA_HOME

echo " "
echo "---------------------------------------------------------------------------------------------------------------"
echo "----- starting ambari server and agent"
echo "---------------------------------------------------------------------------------------------------------------"
echo " "

ambari-server start
ambari-agent start

echo " "
echo "---------------------------------------------------------------------------------------------------------------"
echo "----- sleep before calling ambari REST apis"
echo "---------------------------------------------------------------------------------------------------------------"
echo " "

sleep 30

curl --silent --show-error -H "X-Requested-By: ambari" -X POST -d '@/tmp/install/blueprint.json' -u admin:admin http://jdbc-binary.hdp.local:8080/api/v1/blueprints/generated
curl --silent --show-error -H "X-Requested-By: ambari" -X POST -d '@/tmp/install/create-cluster.json' -u admin:admin http://jdbc-binary.hdp.local:8080/api/v1/clusters/jdbc-binary

echo " "
echo "---------------------------------------------------------------------------------------------------------------"
echo "----- sleep before checking progress"
echo "---------------------------------------------------------------------------------------------------------------"
echo " "

sleep 30

PROGRESS=0
until [ $PROGRESS -eq 100 ]; do
    PROGRESS=`curl --silent --show-error -H "X-Requested-By: ambari" -X GET -u admin:admin http://jdbc-binary.hdp.local:8080/api/v1/clusters/jdbc-binary/requests/1 2>&1 | grep -oP '\"progress_percent\"\s+\:\s+\K[0-9]+'`
    TIMESTAMP=$(date "+%m/%d/%y %H:%M:%S")
    echo -ne "$TIMESTAMP - $PROGRESS percent complete!"\\r
    sleep 60
done

echo " "
echo "---------------------------------------------------------------------------------------------------------------"
echo "----- adding users"
echo "---------------------------------------------------------------------------------------------------------------"
echo " "

useradd -G hdfs admin
usermod -a -G users admin
usermod -a -G hadoop admin
usermod -a -G hive admin

usermod -a -G users vagrant
usermod -a -G hdfs vagrant
usermod -a -G hadoop vagrant
usermod -a -G hive vagrant

echo " "
echo "---------------------------------------------------------------------------------------------------------------"
echo "----- doing some hdfs view stuff"
echo "---------------------------------------------------------------------------------------------------------------"
echo " "

sudo su - hdfs << EOF
    hadoop fs -mkdir /user/admin
    hadoop fs -chown admin:hadoop /user/admin
EOF

echo " "
echo "---------------------------------------------------------------------------------------------------------------"
echo "----- final yum cleanup"
echo "---------------------------------------------------------------------------------------------------------------"
echo " "

yum clean all
rm -rf /var/cache/yum