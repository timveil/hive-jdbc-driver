#!/bin/sh

sudo su - admin << EOF
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

beeline -u jdbc:hive2://localhost:10000/default -e 'create database if not exists tests' -n admin -p admin

beeline -u jdbc:hive2://localhost:10000/tests -f /vagrant/data/array-test.sql -n admin -p admin
beeline -u jdbc:hive2://localhost:10000/tests -f /vagrant/data/data-type-test.sql -n admin -p admin
beeline -u jdbc:hive2://localhost:10000/tests -f /vagrant/data/date-time-test.sql -n admin -p admin
beeline -u jdbc:hive2://localhost:10000/tests -f /vagrant/data/map-test.sql -n admin -p admin
beeline -u jdbc:hive2://localhost:10000/tests -f /vagrant/data/struct-test.sql -n admin -p admin
