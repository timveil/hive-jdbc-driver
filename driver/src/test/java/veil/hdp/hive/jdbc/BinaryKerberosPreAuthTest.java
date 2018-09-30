/*
 *    Copyright 2018 Timothy J Veil
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package veil.hdp.hive.jdbc;

import veil.hdp.hive.jdbc.security.KerberosService;
import veil.hdp.hive.jdbc.test.AbstractConnectionTest;
import veil.hdp.hive.jdbc.utils.PrincipalUtils;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class BinaryKerberosPreAuthTest extends AbstractConnectionTest {

        /*
        on windows:
            -Djava.security.krb5.conf=C:\ProgramData\MIT\Kerberos5\krb5.ini

        must have JCE installed in proper directory <java_home>/jre/lib/security

     */

    @Override
    public Connection createConnection(String host) throws SQLException {
        Properties properties = new Properties();

        String url = "jdbc:hive2://" + host + ":10000/tests?authMode=KERBEROS&krb5Mode=PREAUTH&krb5ServerPrincipal=hive/hdp2.lab.local@LAB.LOCAL";

        try {
            return Subject.doAs(KerberosService.loginWithPassword(PrincipalUtils.parseUserPrincipal("timve@LAB.LOCAL"), "password", true), new PrivilegedExceptionAction<Connection>() {
                @Override
                public Connection run() throws Exception {
                    return new HiveDriver().connect(url, properties);
                }
            });
        } catch (PrivilegedActionException | LoginException e) {
            throw new SQLException(e);
        }


    }

}