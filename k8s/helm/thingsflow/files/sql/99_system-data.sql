--
-- Copyright © 2016-2020 The Thingsboard Authors
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--

/** SYSTEM **/

-- pgcrypto ships with vanilla postgres:16 (contrib), so this stays within the
-- no-extra-extensions constraint. Needed below to hash the sysadmin password.
CREATE EXTENSION IF NOT EXISTS pgcrypto;

/** System admin **/
INSERT INTO tb_user ( id, created_time, tenant_id, customer_id, email, authority )
VALUES ( '5a797660-4612-11e7-a919-92ebcb67fe33', 1592576748000, '13814000-1dd2-11b2-8080-808080808080', '13814000-1dd2-11b2-8080-808080808080', 'sysadmin@thingsboard.org', 'SYS_ADMIN' );

-- The sysadmin account exists so the platform boots with a SYS_ADMIN row, but
-- its password is a PER-INSTALL RANDOM value nobody knows — we never ship the
-- well-known upstream ThingsBoard bcrypt hash (public repo => one-line
-- exploit). A non-demo install therefore has no usable sysadmin login until an
-- operator resets it (or bootstraps a password out of band). The KNOWN demo
-- password (sysadmin/sysadmin) is applied only in demo mode, from
-- zz_demo-credentials.sql, which is excluded from production Helm renders.
INSERT INTO user_credentials ( id, created_time, user_id, enabled, password )
VALUES ( '61441950-4612-11e7-a919-92ebcb67fe33', 1592576748000, '5a797660-4612-11e7-a919-92ebcb67fe33', true,
         crypt(encode(gen_random_bytes(32), 'base64'), gen_salt('bf', 10)) );

/** System settings **/
INSERT INTO admin_settings ( id, created_time, tenant_id, key, json_value )
VALUES ( '6a2266e4-4612-11e7-a919-92ebcb67fe33', 1592576748000, '13814000-1dd2-11b2-8080-808080808080', 'general', '{
	"baseUrl": "http://localhost:8080"
}' );

INSERT INTO admin_settings ( id, created_time, tenant_id, key, json_value )
VALUES ( '6eaaefa6-4612-11e7-a919-92ebcb67fe33', 1592576748000, '13814000-1dd2-11b2-8080-808080808080', 'mail', '{
	"mailFrom": "Thingsboard <sysadmin@localhost.localdomain>",
	"smtpProtocol": "smtp",
	"smtpHost": "localhost",
	"smtpPort": "25",
	"timeout": "10000",
	"enableTls": false,
	"tlsVersion": "TLSv1.2",
	"username": "",
	"password": ""
}' );

INSERT INTO admin_settings ( id, created_time, tenant_id, key, json_value )
VALUES ( '23199d80-6e7e-11ee-8829-ef9fd52a6141', 1697719852888, '13814000-1dd2-11b2-8080-808080808080', 'connectivity', '{
    "http":{"enabled":true,"host":"","port":"8080"},
    "https":{"enabled":false,"host":"","port":"443"},
    "mqtt":{"enabled":true,"host":"","port":"1883"},
    "mqtts":{"enabled":false,"host":"","port":"8883"},
    "coap":{"enabled":true,"host":"","port":"5683"},
    "coaps":{"enabled":false,"host":"","port":"5684"}
}' );

-- The JWT signing key is generated per install, never shipped in the repo.
-- A fixed key here would mean every deployment seeded from this file shares
-- one publicly known signing key, and anyone holding it can forge admin
-- tokens. pgcrypto ships with vanilla postgres:16 (contrib), so this stays
-- within the no-extra-extensions constraint. Production deployments override
-- this row anyway via JWT_TOKEN_SIGNING_KEY from a Kubernetes Secret.
CREATE EXTENSION IF NOT EXISTS pgcrypto;

INSERT INTO admin_settings ( id, created_time, tenant_id, key, json_value )
VALUES ( '1e33c6f0-061e-11ef-b5b7-dba0ee077a1b', 1714391189727, '13814000-1dd2-11b2-8080-808080808080', 'jwt',
    json_build_object(
        'tokenExpirationTime', 9000,
        'refreshTokenExpTime', 604800,
        'tokenIssuer', 'thingsboard.io',
        'tokenSigningKey', encode(gen_random_bytes(64), 'base64')
    )::text );

INSERT INTO queue ( id, created_time, tenant_id, name, topic, poll_interval, partitions, consumer_per_partition, pack_processing_timeout, submit_strategy, processing_strategy )
VALUES ( '6eaaefa6-4612-11e7-a919-92ebcb67fe33', 1592576748000 ,'13814000-1dd2-11b2-8080-808080808080', 'Main' ,'tb_rule_engine.main', 25, 10, true, 2000,
        '{"type": "BURST", "batchSize": 1000}',
        '{"type": "SKIP_ALL_FAILURES", "retries": 3, "failurePercentage": 0.0, "pauseBetweenRetries": 3, "maxPauseBetweenRetries": 3}'
);

INSERT INTO tb_schema_settings (schema_version, product) VALUES (999999000000, 'CE');
