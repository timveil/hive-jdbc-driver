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

package veil.hdp.hive.jdbc.security;

public class ServicePrincipal {

    private final String service;
    private final String host;
    private final String realm;

    public ServicePrincipal(String service, String host, String realm) {
        this.service = service;
        this.host = host;
        this.realm = realm;
    }

    public String getService() {
        return service;
    }

    public String getHost() {
        return host;
    }

    public String getRealm() {
        return realm;
    }

    @Override
    public String toString() {
        return service + '/' + host + '@' + realm;
    }
}
