#!/bin/sh

kadmin.local -q 'xst -norandkey -k /tmp/admin.keytab admin/admin' -w password

chown admin:admin /tmp/admin.keytab

sudo su - admin << EOF
    kinit admin/admin -k -t /tmp/admin.keytab

    hadoop fs -mkdir /tmp/array_test
    hadoop fs -copyFromLocal /vagrant/data/array-test.csv /tmp/array_test

    hadoop fs -mkdir /tmp/data_type_test
    hadoop fs -copyFromLocal /vagrant/data/data-type-test.csv /tmp/data_type_test

    hadoop fs -mkdir /tmp/date_time_test
    hadoop fs -copyFromLocal /vagrant/data/date-time-test.csv /tmp/date_time_test

    hadoop fs -mkdir /tmp/map_test
    hadoop fs -copyFromLocal /vagrant/data/map-test.csv /tmp/map_test

    hadoop fs -mkdir /tmp/struct_test
    hadoop fs -copyFromLocal /vagrant/data/struct-test.csv /tmp/struct_test
EOF

kinit admin/admin -k -t /tmp/admin.keytab

beeline -u 'jdbc:hive2://jdbc-binary-kerberos.hdp.local:10000/default;principal=hive/jdbc-binary-kerberos.hdp.local@HDP.LOCAL' -e 'create database if not exists tests'
beeline -u 'jdbc:hive2://jdbc-binary-kerberos.hdp.local:10000/tests;principal=hive/jdbc-binary-kerberos.hdp.local@HDP.LOCAL' -f /vagrant/data/array-test.sql
beeline -u 'jdbc:hive2://jdbc-binary-kerberos.hdp.local:10000/tests;principal=hive/jdbc-binary-kerberos.hdp.local@HDP.LOCAL' -f /vagrant/data/data-type-test.sql
beeline -u 'jdbc:hive2://jdbc-binary-kerberos.hdp.local:10000/tests;principal=hive/jdbc-binary-kerberos.hdp.local@HDP.LOCAL' -f /vagrant/data/date-time-test.sql
beeline -u 'jdbc:hive2://jdbc-binary-kerberos.hdp.local:10000/tests;principal=hive/jdbc-binary-kerberos.hdp.local@HDP.LOCAL' -f /vagrant/data/map-test.sql
beeline -u 'jdbc:hive2://jdbc-binary-kerberos.hdp.local:10000/tests;principal=hive/jdbc-binary-kerberos.hdp.local@HDP.LOCAL' -f /vagrant/data/struct-test.sql
