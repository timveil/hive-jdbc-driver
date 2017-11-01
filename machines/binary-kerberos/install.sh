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

hostnamectl --static set-hostname jdbc-binary-kerberos

echo " "
echo "---------------------------------------------------------------------------------------------------------------"
echo "----- get ambari repo"
echo "---------------------------------------------------------------------------------------------------------------"
echo " "

wget -nv http://public-repo-1.hortonworks.com/ambari/centos7/2.x/updates/2.6.0.0/ambari.repo -O /etc/yum.repos.d/ambari.repo


echo " "
echo "---------------------------------------------------------------------------------------------------------------"
echo "----- running yum install"
echo "---------------------------------------------------------------------------------------------------------------"
echo " "

yum install rng-tools krb5-libs krb5-server krb5-workstation ambari-server ambari-agent -y && yum clean all

echo " "
echo "---------------------------------------------------------------------------------------------------------------"
echo "----- updating ambari-agent.ini"
echo "---------------------------------------------------------------------------------------------------------------"
echo " "

sed -i "s/^hostname=localhost/hostname=jdbc-binary-kerberos.hdp.local/g" /etc/ambari-agent/conf/ambari-agent.ini

echo " "
echo "---------------------------------------------------------------------------------------------------------------"
echo "----- starting rngd to increase entropy for kdb5_util create"
echo "---------------------------------------------------------------------------------------------------------------"
echo " "

systemctl start rngd
systemctl enable rngd

echo " "
echo "---------------------------------------------------------------------------------------------------------------"
echo "----- updating kerberos config"
echo "---------------------------------------------------------------------------------------------------------------"
echo " "

sudo mv /tmp/install/kadm5.acl /var/kerberos/krb5kdc/kadm5.acl
sudo chown root:root /var/kerberos/krb5kdc/kadm5.acl

sudo mv /tmp/install/kdc.conf /var/kerberos/krb5kdc/kdc.conf
sudo chown root:root /var/kerberos/krb5kdc/kdc.conf

sudo mv /tmp/install/krb5.conf /etc/krb5.conf
sudo chown root:root /etc/krb5.conf

echo " "
echo "---------------------------------------------------------------------------------------------------------------"
echo "----- creating kerberos database"
echo "---------------------------------------------------------------------------------------------------------------"
echo " "

kdb5_util create -s -P password

echo " "
echo "---------------------------------------------------------------------------------------------------------------"
echo "----- adding kerberos principal"
echo "---------------------------------------------------------------------------------------------------------------"
echo " "

kadmin.local -q 'addprinc -pw password admin/admin' -w password

echo " "
echo "---------------------------------------------------------------------------------------------------------------"
echo "----- starting kerberos services"
echo "---------------------------------------------------------------------------------------------------------------"
echo " "

systemctl start krb5kdc
systemctl start kadmin
systemctl enable krb5kdc
systemctl enable kadmin

echo " "
echo "---------------------------------------------------------------------------------------------------------------"
echo "----- running ambari setup"
echo "---------------------------------------------------------------------------------------------------------------"
echo " "

ambari-server setup -s

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

curl --silent --show-error -H "X-Requested-By: ambari" -X POST -d '@/tmp/install/blueprint.json' -u admin:admin http://jdbc-binary-kerberos.hdp.local:8080/api/v1/blueprints/generated
curl --silent --show-error -H "X-Requested-By: ambari" -X POST -d '@/tmp/install/create-cluster.json' -u admin:admin http://jdbc-binary-kerberos.hdp.local:8080/api/v1/clusters/jdbc-binary-kerberos

echo " "
echo "---------------------------------------------------------------------------------------------------------------"
echo "----- sleep before checking progress"
echo "---------------------------------------------------------------------------------------------------------------"
echo " "

sleep 30

PROGRESS=0
until [ $PROGRESS -eq 100 ]; do
    PROGRESS=`curl --silent --show-error -H "X-Requested-By: ambari" -X GET -u admin:admin http://jdbc-binary-kerberos.hdp.local:8080/api/v1/clusters/jdbc-binary-kerberos/requests/1 2>&1 | grep -oP '\"progress_percent\"\s+\:\s+\K[0-9]+'`
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