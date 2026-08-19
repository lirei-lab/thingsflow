import pathlib
import subprocess
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[2]


class NATSConfigTest(unittest.TestCase):
    def test_values_define_default_durable_nats_twin_state(self):
        values = (ROOT / "k8s/helm/thingsflow/values.yaml").read_text()

        self.assertIn("nats:", values)
        self.assertIn("nats:2.10", values)
        self.assertNotIn(
            'storage: "memory"', values,
            "JetStream must default to file storage: with memory storage and no "
            "PVC, a NATS restart destroys the streams and the twin_state bucket, "
            "and nothing recreates them -- they come from a Helm hook, so a "
            "running release never rebuilds them and the platform does not "
            "self-heal.",
        )
        self.assertIn("tf.ingest.mqtt.raw.>", values)
        self.assertIn("tf.ingest.http.raw.>", values)
        self.assertIn('rawSubject: "tf.ingest.*.raw.>"', values)
        self.assertIn("httpIngest:", values)
        self.assertIn('envoy: "envoyproxy/envoy:', values)
        self.assertIn('subjectPrefix: "tf.ingest.http.raw"', values)
        self.assertIn("jwksUrl:", values)
        self.assertIn("gateway:", values)
        self.assertIn("bento:", values)
        self.assertIn("tf.device.telemetry.latest.>", values)
        self.assertIn("twin_state", values)
        self.assertIn("maxValueSize: 1048576", values)
        self.assertIn("twinState:", values)
        self.assertIn("twinState:\n    enabled: true", values)
        self.assertIn('store: "nats"', values)
        self.assertIn("natsBucket: \"twin_state\"", values)

    def test_file_backed_stream_caps_fit_the_pvc_budget(self):
        """The retention guard refuses an install whose streams overcommit the PVC.

        verify.sh asserts that the sum of every file-backed stream cap plus the
        KV fits in 70% of the NATS PVC, and fails the post-install hook when it
        does not. Making the streams durable by default put those caps on disk
        for the first time, so the defaults have to satisfy that budget or a
        plain `helm install` fails at the hook.
        """
        import yaml

        nats = yaml.safe_load(
            (ROOT / "k8s/helm/thingsflow/values.yaml").read_text()
        )["nats"]

        def gib(v):
            v = str(v)
            return float(v[:-2]) if v.endswith("Gi") else float(v) / 2**30

        pvc = gib(nats["persistence"]["size"])
        total = sum(
            gib(s["maxBytes"])
            for key in ("rawStream", "entityStream", "alarmIntentStream", "latestStream")
            for s in [nats.get(key) or {}]
            if s.get("storage") == "file" and s.get("enabled", True) and s.get("maxBytes")
        )
        budget = 0.70 * pvc
        self.assertLessEqual(
            total, budget,
            f"file-backed stream caps total {total}Gi against a {budget}Gi budget "
            f"(70% of a {pvc}Gi PVC); the post-install guard would reject this",
        )

    def test_default_chart_is_nats_first_without_legacy_chain(self):
        result = subprocess.run(
            ["helm", "template", "thingsflow", "k8s/helm/thingsflow"],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=True,
        )

        rendered = result.stdout
        self.assertIn("name: thingsflow-nats", rendered)
        self.assertIn("name: thingsflow-http-ingest", rendered)
        self.assertIn("name: thingsflow-nats-latest-kv", rendered)
        self.assertIn("name: thingsflow-greptimedb", rendered)
        self.assertIn("name: thingsflow-nats-greptimedb", rendered)
        self.assertNotIn("name: thingsflow-nats-questdb", rendered)
        self.assertIn('EVENT_BROKER: "nats"', rendered)
        self.assertNotIn("legacy-broker", rendered)

    def test_chart_contains_nats_jetstream_memory_and_kv_bootstrap(self):
        template = (ROOT / "k8s/helm/thingsflow/templates/nats.yaml").read_text()

        self.assertIn(".Values.nats.enabled", template)
        self.assertIn("--jetstream", template)
        self.assertIn("--store_dir={{", template)
        self.assertIn("/tmp/nats/jetstream", template)
        self.assertIn("emptyDir:", template)
        self.assertIn("medium: Memory", template)
        self.assertIn("nats --server \"$NATS_URL\" stream add", template)
        self.assertIn('default "TF_RAW"', template)
        self.assertIn('$raw.storage | default "memory"', template)
        self.assertIn('$latest.storage | default "memory"', template)
        self.assertIn('$alarms.storage | default "memory"', template)
        self.assertIn('$kv.storage | default "memory"', template)
        self.assertIn("nats --server \"$NATS_URL\" kv add", template)
        self.assertIn('default "twin_state"', template)
        self.assertIn("--history", template)
        self.assertIn("--max-value-size", template)
        self.assertIn("TF_LATEST", template)

    def test_flow_core_can_be_wired_to_nats_kv_as_twin_state_source(self):
        template = (ROOT / "k8s/helm/thingsflow/templates/flow-core.yaml").read_text()

        self.assertIn("TWIN_STATE_STORE", template)
        self.assertIn("NATS_URL", template)
        self.assertIn("NATS_KV_BUCKET", template)
        self.assertIn("TWIN_STATE_WATCH_ENABLED", template)
        self.assertIn(".Values.nats.enabled", template)
        self.assertIn('include "thingsflow.natsURL"', template)

    def test_nats_only_profile_excludes_legacy_chain(self):
        result = subprocess.run(
            [
                "helm",
                "template",
                "thingsflow",
                "k8s/helm/thingsflow",
                "--set",
                "nats.enabled=true",
                "--set",
                "flowCore.twinState.enabled=true",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=True,
        )

        rendered = result.stdout
        self.assertIn("name: thingsflow-nats", rendered)
        self.assertIn("name: thingsflow-flow-core", rendered)
        self.assertIn("name: thingsflow-rmqtt-edge", rendered)
        self.assertNotIn("data-plane-history-sink", rendered)
        self.assertIn('EVENT_BROKER: "nats"', rendered)
        self.assertNotIn("legacy-broker", rendered)
        self.assertIn("name: thingsflow-http-ingest", rendered)
        self.assertIn("envoy.filters.http.jwt_authn", rendered)
        self.assertIn("remote_jwks", rendered)
        self.assertIn("/api/noauth/device-jwks", rendered)
        self.assertIn("/bento", rendered)
        self.assertIn("tf.ingest.http.raw", rendered)

    def test_http_ingest_template_is_nats_only_and_uses_gateway_bento(self):
        template = (ROOT / "k8s/helm/thingsflow/templates/http-ingest.yaml").read_text()
        bento = (ROOT / "k8s/helm/thingsflow/files/bento-http-ingest-nats.yaml").read_text()
        dockerfile = (ROOT / "flow-core/Dockerfile").read_text()

        self.assertIn(".Values.httpIngest.enabled", template)
        self.assertIn("envoy.filters.http.jwt_authn", template)
        self.assertIn("remote_jwks", template)
        self.assertIn("claim_to_headers", template)
        self.assertIn("envoy.filters.http.buffer", template)
        self.assertIn("bento-http-ingest-nats.yaml", template)
        self.assertIn("/api/noauth/device-jwks", template)
        self.assertIn("BENTO_HTTP_PORT", template)
        self.assertIn("NATS_HTTP_SUBJECT_PREFIX", template)
        self.assertIn("readOnlyRootFilesystem: true", template)
        self.assertIn("http_server:", bento)
        self.assertIn("path: /api/v1/telemetry", bento)
        self.assertIn("output:", bento)
        self.assertIn("nats:", bento)
        self.assertIn('env("NATS_HTTP_SUBJECT_PREFIX")', bento)
        self.assertNotIn("go build -o http-ingest ./cmd/http-ingest", dockerfile)
        self.assertNotIn("/app/http-ingest", dockerfile)

    def test_compose_nats_stack_uses_nats_edges_without_legacy_dependency(self):
        compose = (ROOT / "docker/docker-compose-nats.yml").read_text()
        rmqtt = (ROOT / "docker/rmqtt/rmqtt.toml").read_text()
        nats_bridge = (ROOT / "docker/rmqtt/rmqtt-bridge-egress-nats.toml").read_text()

        self.assertIn("http-ingest-bento:", compose)
        self.assertIn("http-ingest:", compose)
        self.assertIn("nats-latest-kv:", compose)
        self.assertIn("greptimedb:", compose)
        self.assertIn("nats-greptimedb:", compose)
        self.assertIn("envoyproxy/envoy", compose)
        self.assertIn("bento-http-ingest-nats.yaml", compose)
        self.assertIn("http-ingest-envoy.yaml", compose)
        self.assertIn("bento-nats-greptimedb.yaml", compose)
        self.assertIn("NATS_HTTP_SUBJECT_PREFIX: tf.ingest.http.raw", compose)
        self.assertIn("NATS_RAW_SUBJECT: \"tf.ingest.*.raw.>\"", compose)
        self.assertIn("rmqtt-bridge-egress-nats.toml", compose)
        self.assertNotIn("/app/http-ingest", compose)
        self.assertIn("rmqtt-bridge-egress-nats", rmqtt)
        self.assertIn("rmqtt-bridge-egress-nats", rmqtt)
        self.assertIn("servers = \"nats://nats:4222\"", nats_bridge)

    def test_compose_edge_gateway_uses_mosquitto_telegraf_and_local_generator(self):
        platform_compose = (ROOT / "docker/docker-compose-nats.yml").read_text()
        compose = (ROOT / "docker/docker-compose-edge-gateway.yml").read_text()
        provisioner = (ROOT / "docker/edge/provision_edge_gateway.py").read_text()
        telegraf = (ROOT / "docker/edge/telegraf.conf.template").read_text()

        self.assertNotIn("edge-mosquitto:", platform_compose)
        self.assertNotIn("edge-telegraf:", platform_compose)
        self.assertNotIn("edge-device-generator:", platform_compose)
        self.assertIn("edge-mosquitto:", compose)
        self.assertIn("eclipse-mosquitto:2", compose)
        self.assertIn("edge-telegraf-init:", compose)
        self.assertIn("edge-telegraf:", compose)
        self.assertIn("telegraf:1.34", compose)
        self.assertIn("edge-device-generator:", compose)
        self.assertIn('profiles: ["edge-gateway"]', compose)
        self.assertIn('SKIP_PROVISIONING: "true"', compose)
        self.assertIn("MQTT_AUTH_MODE: none", compose)
        self.assertIn("MQTT_TOPIC_TEMPLATE:", compose)
        self.assertIn("edge/devices/{device_name}/telemetry", compose)
        self.assertIn("EDGE_FLOW_CORE_URL", compose)
        self.assertIn("EDGE_FLOW_CORE_URL", compose)
        self.assertIn("EDGE_UPSTREAM_HTTP_URL", compose)
        self.assertIn("edge-telegraf-runtime:", compose)
        self.assertIn("EDGE_PROVISION_SECRET", provisioner)
        self.assertIn("EDGE_UPSTREAM_HTTP_URL", provisioner)
        self.assertIn("EDGE_TELEGRAF_UID", provisioner)
        self.assertIn("os.chown", provisioner)
        self.assertIn("ThingsFlowDeviceClient", provisioner)
        self.assertIn("render_config", provisioner)
        self.assertIn("${EDGE_LOCAL_TOPIC_FILTER}", telegraf)
        self.assertIn("${EDGE_UPSTREAM_HTTP_URL}", telegraf)
        self.assertIn("tcp://edge-mosquitto:1883", telegraf)
        self.assertIn("[[outputs.http]]", telegraf)
        self.assertIn("Authorization = \"Bearer __DEVICE_JWT__\"", telegraf)
        self.assertIn("data_format = \"json\"", telegraf)
        self.assertNotIn("rmqtt-edge:\n        condition: service_started", compose)
        self.assertNotIn("flow-core:\n        condition: service_started", compose)

    def test_nats_materializers_accept_mqtt_and_http_canonical_events(self):
        latest = (ROOT / "k8s/helm/thingsflow/files/bento-nats-latest-kv.yaml").read_text()
        greptime = (ROOT / "k8s/helm/thingsflow/files/bento-nats-greptimedb.yaml").read_text()
        questdb = (ROOT / "k8s/helm/thingsflow/files/bento-nats-questdb.yaml").read_text()

        for config in (latest, greptime, questdb):
            self.assertIn("this.tenantId.or($claims.tenantId)", config)
            self.assertIn("this.deviceId.or($topic_device_id)", config)
            self.assertIn("this.values.or(this.fields).or(this)", config)
            self.assertIn("transport", config)

        # The latest-value writer must land in the twin_state KV. This has now had
        # three spellings, and the point of the assertions below is unchanged
        # across all of them: pin the DESTINATION, not the plugin name, so this
        # still fails if the writer is ever pointed somewhere that is not the KV.
        #   1. `output.nats_kv` (original).
        #   2. Milestone 2 Phase 2 (commit 96d2ee3c): publish straight to the
        #      bucket's underlying stream subject, because a KV Put IS a JetStream
        #      publish to $KV.<bucket>.<key>.
        #      See benchmarks/FINDING-twin-state-rung1-verification.md.
        #   3. Milestone 4 Phase 3 (current): one whole-device document per write
        #      instead of N per-key writes, so the write is a `cache set` through a
        #      nats_kv cache resource bound to the same bucket, and the per-key
        #      $KV.<bucket>.<key> publish no longer exists. The output stage is
        #      `drop` because the write happens in the pipeline.
        #      See benchmarks/FINDING-twin-state-merge-spike.md "## Option A".
        self.assertIn("nats_kv:", latest)  # the cache resource...
        self.assertIn('bucket: "${NATS_KV_BUCKET}"', latest)  # ...bound to the KV bucket
        self.assertIn("resource: kvcache", latest)  # ...and actually used
        self.assertIn("operator: set", latest)  # ...to write
        # The doc key is the device's, and it addresses the WHOLE doc. The
        # fan-out's signature was a per-key suffix concatenated onto that key
        # (`... + ".telemetry." + pair.key`); its absence is what proves one
        # write per message rather than N. Matched as the concatenation, not
        # as the bare word, which still appears in comments and in Bloblang
        # field access on the fetched document.
        self.assertIn('meta kv_key = "DEVICE." + this.tenantId + "." + this.deviceId', latest)
        self.assertNotIn('+ ".telemetry." +', latest)
        self.assertIn("http_client:", greptime)
        self.assertIn("GREPTIMEDB_INFLUX_URL", greptime)
        self.assertIn("questdb:", questdb)
        self.assertNotIn("event_ts", questdb)
        self.assertNotIn("designated_timestamp_field", questdb)

    def test_nats_benchmark_runner_deploys_benchmarks_and_cleans_up(self):
        runner = (ROOT / "benchmarks/scripts/run-nats-benchmark.sh").read_text()

        self.assertIn("helm template", runner)
        self.assertIn("nats.enabled=true", runner)
        self.assertIn("kubectl", runner)
        self.assertIn("bench", runner)
        self.assertIn("--js", runner)
        self.assertIn("--kv", runner)
        self.assertIn("--storage memory", runner)
        self.assertIn("twin_state", runner)
        self.assertIn("app.kubernetes.io/part-of=nats-benchmark", runner)
        self.assertIn("cleanup_benchmark_jobs", runner)

    def test_nats_benchmark_runner_uses_isolated_subjects_and_buckets(self):
        runner = (ROOT / "benchmarks/scripts/run-nats-benchmark.sh").read_text()

        self.assertIn('NATS_JS_STREAM="${NATS_JS_STREAM:-TF_BENCH_RAW}"', runner)
        self.assertIn('NATS_JS_SUBJECT="${NATS_JS_SUBJECT:-tf.bench.raw}"', runner)
        self.assertIn('NATS_KV_BUCKET="${NATS_KV_BUCKET:-bench_twin_state}"', runner)
        self.assertIn("ensure_benchmark_stream", runner)
        self.assertIn("ensure_benchmark_kv_bucket", runner)
        self.assertIn("stream add", runner)
        self.assertIn("kv add", runner)
        self.assertNotIn('NATS_RAW_STREAM="${NATS_RAW_STREAM:-TF_RAW}"', runner)
        self.assertNotIn('NATS_KV_BUCKET="${NATS_KV_BUCKET:-twin_state}"', runner)
        self.assertNotIn('NATS_JS_SUBJECT="${NATS_JS_SUBJECT:-tf.ingest.mqtt.raw.bench}"', runner)

    def test_nats_materializers_drop_malformed_non_json_messages(self):
        latest = (ROOT / "k8s/helm/thingsflow/files/bento-nats-latest-kv.yaml").read_text()
        greptime = (ROOT / "k8s/helm/thingsflow/files/bento-nats-greptimedb.yaml").read_text()
        questdb = (ROOT / "k8s/helm/thingsflow/files/bento-nats-questdb.yaml").read_text()
        alarms = (ROOT / "k8s/helm/thingsflow/files/bento-nats-alarms.yaml").read_text()

        for config in (latest, greptime, questdb, alarms):
            self.assertIn("meta malformed_reason", config)
            self.assertIn("deleted()", config)
            self.assertIn("$parsed.type() == \"object\"", config)
            self.assertIn("$parsed.type() == \"array\"", config)
            self.assertIn("parse_json().catch", config)

    def test_nats_data_plane_resources_are_sized_for_1000_device_ladder(self):
        values = (ROOT / "k8s/helm/thingsflow/values.yaml").read_text()

        self.assertIn("latestKv:", values)
        self.assertIn('memory: "512Mi"', values)
        self.assertIn('cpu: "750m"', values)
        self.assertIn("greptimedb:", values)
        self.assertIn('store: "greptimedb"', values)
        self.assertIn("questdb:", values)

    def test_production_chart_accepts_external_flow_core_key_secrets(self):
        result = subprocess.run(
            [
                "helm",
                "template",
                "thingsflow",
                "k8s/helm/thingsflow",
                "--set",
                "production=true",
                "--set",
                "flowCore.allowedOrigin=https://ui.example.test",
                "--set",
                "postgres.passwordExistingSecret.name=thingsflow-postgres",
                "--set",
                "flowCore.jwtTokenSigningKeyExistingSecret.name=thingsflow-platform-keys",
                "--set",
                "flowCore.jwtTokenSigningKeyExistingSecret.key=jwt-token-signing-key",
                "--set",
                "flowCore.deviceJwt.privateKeyExistingSecret.name=thingsflow-device-jwt",
                "--set",
                "flowCore.deviceJwt.privateKeyExistingSecret.key=device-jwt-es256-private-key-pem-b64",
                "--set",
                "flowCore.deviceJwt.additionalPublicJwksExistingSecret.name=thingsflow-device-jwt",
                "--set",
                "flowCore.deviceJwt.additionalPublicJwksExistingSecret.key=previous-public-jwks-b64",
                "--set",
                "nats.auth.enabled=true",
                "--set",
                "nats.auth.existingSecret.name=thingsflow-nats-auth",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=True,
        )

        rendered = result.stdout
        self.assertIn("name: JWT_TOKEN_SIGNING_KEY", rendered)
        self.assertIn("name: DEVICE_JWT_ES256_PRIVATE_KEY_PEM_B64", rendered)
        self.assertIn("name: DEVICE_JWT_ADDITIONAL_PUBLIC_JWKS_B64", rendered)
        self.assertIn("name: thingsflow-platform-keys", rendered)
        self.assertIn("key: jwt-token-signing-key", rendered)
        self.assertIn("name: thingsflow-device-jwt", rendered)
        self.assertIn("key: device-jwt-es256-private-key-pem-b64", rendered)
        self.assertIn("key: previous-public-jwks-b64", rendered)

    def test_nats_auth_wires_authenticated_internal_nats_urls(self):
        result = subprocess.run(
            [
                "helm",
                "template",
                "thingsflow",
                "k8s/helm/thingsflow",
                "--set",
                "nats.auth.enabled=true",
                "--set",
                "nats.auth.user=zt_internal",
                "--set",
                "nats.auth.password=zt_internal_secret",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=True,
        )

        rendered = result.stdout
        self.assertIn("name: thingsflow-nats-auth", rendered)
        self.assertIn("--user=$(NATS_USER)", rendered)
        self.assertIn("--pass=$(NATS_PASSWORD)", rendered)
        self.assertIn('value: "nats://$(NATS_USER):$(NATS_PASSWORD)@thingsflow-nats:4222"', rendered)
        self.assertIn("name: NATS_USER", rendered)
        self.assertIn("name: NATS_PASSWORD", rendered)

    def test_production_requires_nats_auth_for_builtin_nats_event_plane(self):
        result = subprocess.run(
            [
                "helm",
                "template",
                "thingsflow",
                "k8s/helm/thingsflow",
                "--set",
                "production=true",
                "--set",
                "flowCore.allowedOrigin=https://ui.example.test",
                "--set",
                "flowCore.jwtTokenSigningKeyExistingSecret.name=thingsflow-platform-keys",
                "--set",
                "flowCore.deviceJwt.privateKeyExistingSecret.name=thingsflow-device-jwt",
                "--set",
                "postgres.passwordExistingSecret.name=thingsflow-postgres",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("nats.auth.enabled=true is required", result.stderr)

    def test_production_rejects_default_postgres_password(self):
        result = subprocess.run(
            [
                "helm",
                "template",
                "thingsflow",
                "k8s/helm/thingsflow",
                "--set",
                "production=true",
                "--set",
                "flowCore.allowedOrigin=https://ui.example.test",
                "--set",
                "flowCore.jwtTokenSigningKeyExistingSecret.name=thingsflow-platform-keys",
                "--set",
                "flowCore.deviceJwt.privateKeyExistingSecret.name=thingsflow-device-jwt",
                "--set",
                "nats.auth.enabled=true",
                "--set",
                "nats.auth.existingSecret.name=thingsflow-nats-auth",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("postgres.password must not use the dev default", result.stderr)

    def test_production_rejects_bundled_dex_test_auth(self):
        result = subprocess.run(
            [
                "helm",
                "template",
                "thingsflow",
                "k8s/helm/thingsflow",
                "--set",
                "production=true",
                "--set",
                "testAuth.enabled=true",
                "--set",
                "flowCore.allowedOrigin=https://ui.example.test",
                "--set",
                "postgres.passwordExistingSecret.name=thingsflow-postgres",
                "--set",
                "flowCore.jwtTokenSigningKeyExistingSecret.name=thingsflow-platform-keys",
                "--set",
                "flowCore.deviceJwt.privateKeyExistingSecret.name=thingsflow-device-jwt",
                "--set",
                "nats.auth.enabled=true",
                "--set",
                "nats.auth.existingSecret.name=thingsflow-nats-auth",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("testAuth.enabled=true is for automated demos/tests only", result.stderr)

    def test_oidc_can_use_existing_secrets_without_inline_secret_values(self):
        result = subprocess.run(
            [
                "helm",
                "template",
                "thingsflow",
                "k8s/helm/thingsflow",
                "--set",
                "oidc.enabled=true",
                "--set",
                "oidc.clientId=thingsflow-ui",
                "--set",
                "oidc.issuer=https://idp.example.test/realms/thingsflow",
                "--set",
                "oidc.clientSecretExistingSecret.name=thingsflow-oidc",
                "--set",
                "oidc.clientSecretExistingSecret.key=client-secret",
                "--set",
                "oidc.stateSigningKeyExistingSecret.name=thingsflow-oidc",
                "--set",
                "oidc.stateSigningKeyExistingSecret.key=state-signing-key",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=True,
        )

        rendered = result.stdout
        self.assertIn("name: OIDC_CLIENT_SECRET", rendered)
        self.assertIn("name: OIDC_STATE_SIGNING_KEY", rendered)
        self.assertIn("name: thingsflow-oidc", rendered)
        self.assertIn("key: client-secret", rendered)
        self.assertIn("key: state-signing-key", rendered)

    def test_oidc_secrets_are_not_required_when_oidc_is_disabled(self):
        result = subprocess.run(
            [
                "helm",
                "template",
                "thingsflow",
                "k8s/helm/thingsflow",
                "--set",
                "oidc.enabled=false",
                "--set",
                "testAuth.enabled=false",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=True,
        )

        rendered = result.stdout
        self.assertIn('OIDC_ENABLED: "false"', rendered)
        self.assertNotIn("OIDC_CLIENT_SECRET", rendered)
        self.assertNotIn("OIDC_STATE_SIGNING_KEY", rendered)
        self.assertNotIn("thingsflow-oidc", rendered)

    def test_test_auth_profile_wires_dex_to_flow_core_oidc(self):
        result = subprocess.run(
            [
                "helm",
                "template",
                "thingsflow",
                "k8s/helm/thingsflow",
                "--set",
                "testAuth.enabled=true",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=True,
        )

        rendered = result.stdout
        self.assertIn("name: thingsflow-test-auth-dex-config", rendered)
        self.assertIn("name: thingsflow-test-auth-dex", rendered)
        self.assertIn("image: \"ghcr.io/dexidp/dex:", rendered)
        self.assertIn("issuer: http://localhost:5556/dex", rendered)
        self.assertIn("id: thingsflow-ui", rendered)
        self.assertIn("http://localhost:8082/login/oauth2/code/dex", rendered)
        self.assertIn('OIDC_ENABLED: "true"', rendered)
        self.assertIn('OIDC_PROVIDER_ID: "dex"', rendered)
        self.assertIn('OIDC_PROVIDER_TITLE: "Dex"', rendered)
        self.assertIn('OIDC_ISSUER: "http://localhost:5556/dex"', rendered)
        self.assertIn('OIDC_AUTHORIZATION_URL: "http://localhost:5556/dex/auth"', rendered)
        self.assertIn('OIDC_TOKEN_URL: "http://thingsflow-test-auth-dex:5556/dex/token"', rendered)
        self.assertIn('OIDC_JWKS_URL: "http://thingsflow-test-auth-dex:5556/dex/keys"', rendered)
        self.assertIn("name: thingsflow-test-auth-oidc", rendered)
        self.assertIn("key: client-secret", rendered)
        self.assertIn("key: state-signing-key", rendered)
        self.assertNotIn("name: thingsflow-keycloak", rendered)

    def test_ingress_routes_dex_when_test_auth_is_enabled(self):
        result = subprocess.run(
            [
                "helm",
                "template",
                "thingsflow",
                "k8s/helm/thingsflow",
                "--set",
                "ingress.enabled=true",
                "--set",
                "ingress.host=thingsflow.example.com",
                "--set",
                "testAuth.enabled=true",
                "--set",
                "testAuth.dex.issuer=http://thingsflow.example.com/dex",
                "--set",
                "testAuth.dex.redirectUrl=http://thingsflow.example.com/login/oauth2/code/dex",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=True,
        )

        rendered = result.stdout
        self.assertIn("host: thingsflow.example.com", rendered)
        self.assertIn("- path: /login/oauth2/code/", rendered)
        self.assertIn("- path: /dex/", rendered)
        self.assertIn("name: thingsflow-test-auth-dex", rendered)
        self.assertIn("number: 5556", rendered)
        self.assertIn('OIDC_ISSUER: "http://thingsflow.example.com/dex"', rendered)
        self.assertIn('OIDC_AUTHORIZATION_URL: "http://thingsflow.example.com/dex/auth"', rendered)
        self.assertIn(
            'OIDC_REDIRECT_URL: "http://thingsflow.example.com/login/oauth2/code/dex"',
            rendered,
        )

    def test_demo_values_enable_dex_test_auth_automatically(self):
        result = subprocess.run(
            [
                "helm",
                "template",
                "thingsflow",
                "k8s/helm/thingsflow",
                "-f",
                "k8s/helm/thingsflow/values-demo.yaml",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=True,
        )

        rendered = result.stdout
        self.assertIn("name: thingsflow-test-auth-dex", rendered)
        self.assertIn("tenant@thingsflow.local", rendered)
        self.assertIn('OIDC_ALLOW_USER_CREATION: "true"', rendered)

    def test_rmqtt_nats_bridge_credentials_are_rendered_from_secret_not_configmap(self):
        result = subprocess.run(
            [
                "helm",
                "template",
                "thingsflow",
                "k8s/helm/thingsflow",
                "--set",
                "nats.auth.enabled=true",
                "--set",
                "nats.auth.existingSecret.name=thingsflow-nats-auth",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=True,
        )

        rendered = result.stdout
        self.assertIn("name: render-rmqtt-config", rendered)
        self.assertIn('servers = "nats://thingsflow-nats:4222"', rendered)
        self.assertIn('auth.username = "__NATS_USER__"', rendered)
        self.assertIn('auth.password = "__NATS_PASSWORD__"', rendered)
        self.assertIn("sed -i", rendered)
        self.assertIn("name: config-rendered", rendered)
        self.assertNotIn("zt_internal_secret", rendered)

    def test_production_example_values_render_with_persistent_nats(self):
        result = subprocess.run(
            [
                "helm",
                "template",
                "thingsflow",
                "k8s/helm/thingsflow",
                "-f",
                "k8s/helm/thingsflow/values-production.example.yaml",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=True,
        )

        rendered = result.stdout
        self.assertIn('value: "production"', rendered)
        self.assertIn("name: thingsflow-nats-auth", rendered)
        self.assertIn("name: data", rendered)
        self.assertIn("storage: 20Gi", rendered)
        self.assertIn("--storage file", rendered)
        self.assertIn("--ttl \"24h\"", rendered)

    def test_pilot_example_values_render_secure_profile(self):
        result = subprocess.run(
            [
                "helm",
                "template",
                "thingsflow",
                "k8s/helm/thingsflow",
                "-f",
                "k8s/helm/thingsflow/values-pilot.example.yaml",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=True,
        )

        rendered = result.stdout
        self.assertIn('value: "production"', rendered)
        self.assertIn("name: thingsflow-nats-auth", rendered)
        self.assertIn("storage: 50Gi", rendered)
        self.assertIn("storage: 100Gi", rendered)
        self.assertIn("--storage file", rendered)
        self.assertIn("--ttl \"24h\"", rendered)
        self.assertIn("name: thingsflow-oidc", rendered)
        self.assertIn("key: client-secret", rendered)
        self.assertIn("name: thingsflow-postgres-backup", rendered)
        self.assertIn("name: thingsflow-backup-s3", rendered)
        self.assertNotIn("name: thingsflow-test-auth-dex", rendered)
        self.assertNotIn("S3_SECRET_KEY: \"", rendered)

    def test_flow_core_nats_mode_does_not_backfill_device_latest_from_postgres(self):
        reader = (ROOT / "flow-core/internal/telemetry/reader.go").read_text()
        ws = (ROOT / "flow-core/internal/ws/ws.go").read_text()
        entityquery = (ROOT / "flow-core/internal/entityquery/entityquery.go").read_text()
        twin = (ROOT / "flow-core/internal/twin/twin.go").read_text()
        startup = (ROOT / "flow-core/twin_state.go").read_text()

        self.assertIn('getEnv("TWIN_STATE_STORE", "")', reader)
        self.assertIn("http.StatusServiceUnavailable", reader)
        self.assertIn("twinstore.ErrNotFound", reader)
        self.assertIn("NATS twin state unavailable", reader)
        self.assertIn("natsTwinStateAuthoritative(entityType)", ws)
        self.assertIn("natsTwinStateAuthoritative(entityType)", entityquery)
        self.assertIn("natsTwinStateAuthoritative(entityType)", twin)
        self.assertIn("Postgres latest telemetry is not authoritative", startup)

    def test_no_legacy_zt_prefix_in_chart(self):
        chart = pathlib.Path(__file__).resolve().parents[2] / "k8s" / "helm" / "thingsflow"
        offenders = []
        for p in chart.rglob("*.yaml"):
            text = p.read_text(errors="ignore")
            if "zt." in text or "ZT_" in text:
                # nats.yaml keeps the legacy literals in its migration block only
                if p.name == "nats.yaml":
                    continue
                offenders.append(str(p))
        self.assertEqual(offenders, [], f"legacy zt./ZT_ prefix in: {offenders}")


if __name__ == "__main__":
    unittest.main()
