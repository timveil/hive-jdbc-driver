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

package veil.hdp.hive.jdbc.security.http;

import org.apache.hc.client5.http.auth.BasicUserPrincipal;
import org.apache.hc.client5.http.auth.Credentials;

import java.security.Principal;

class AnonymousCredentials implements Credentials {

    private static final String ANONYMOUS = "anonymous";

    @Override
    public Principal getUserPrincipal() {
        return new BasicUserPrincipal(ANONYMOUS);
    }

    @Override
    public char[] getPassword() {
        return ANONYMOUS.toCharArray();
    }

}
