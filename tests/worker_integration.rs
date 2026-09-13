mod common;

use aura_osm::config::{Environment, Runtime};
use aura_osm::format::hash_bytes;
use aura_osm::incremental::{self, ApplyOptions, InitOptions};
use aura_osm::pipeline::cleanup_region;
use aura_osm::publish::{PublishConfig, Publisher};
use aura_osm::source::Region;
use flate2::{Compression, write::GzEncoder};
use reqwest::blocking::Client;
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;

struct Endpoints {
    worker: String,
    r2: String,
}

impl Endpoints {
    fn load() -> Self {
        let endpoint = |name| {
            let value = std::env::var(name)
                .unwrap_or_else(|_| panic!("{name} must name a running loopback test service"));
            let url = reqwest::Url::parse(&value).expect("Test endpoint must be a URL");
            assert!(
                url.scheme() == "http"
                    && matches!(url.host_str(), Some("127.0.0.1" | "[::1]"))
                    && url.username().is_empty()
                    && url.password().is_none()
                    && url.path() == "/"
                    && url.query().is_none()
                    && url.fragment().is_none(),
                "{name} must be a loopback HTTP origin"
            );
            value.trim_end_matches('/').to_owned()
        };
        Self {
            worker: endpoint("OSM_TEST_WORKER_URL"),
            r2: endpoint("OSM_TEST_R2_ENDPOINT"),
        }
    }
}

fn query(client: &Client, origin: &str, text: Option<&str>) -> Value {
    let mut url = reqwest::Url::parse(&format!("{origin}/api/osm/scan")).unwrap();
    url.query_pairs_mut().extend_pairs([
        ("lat", "-33.8688"),
        ("lng", "151.209"),
        ("radius", "100"),
        ("limit", "5"),
    ]);
    if let Some(text) = text {
        url.query_pairs_mut().append_pair("q", text);
    }
    let response = client.get(url).send().unwrap();
    assert!(
        response.status().is_success(),
        "query rejected: {}",
        response.status()
    );
    response.json().unwrap()
}

fn ids(result: &Value) -> BTreeSet<String> {
    result["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|poi| poi["id"].as_str().unwrap().to_owned())
        .collect()
}

fn object_keys(client: &Client, origin: &str) -> BTreeSet<String> {
    let value: Value = client
        .get(format!("{origin}/__test/objects"))
        .send()
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .unwrap();
    value["keys"]
        .as_array()
        .unwrap()
        .iter()
        .map(|key| key.as_str().unwrap().to_owned())
        .collect()
}

#[test]
#[ignore = "Requires isolated loopback services in OSM_TEST_WORKER_URL and OSM_TEST_R2_ENDPOINT"]
fn rust_init_publish_query_update_reuse_cleanup_preserves_cloud_data() {
    let endpoints = Endpoints::load();
    let work = common::work("worker-integration-");
    let xml = r#"<osm version="0.6">
      <node id="1" lat="-33.8688" lon="151.209"><tag k="name:ja" v="喫茶店"/><tag k="name:en" v="Coffee"/><tag k="amenity" v="cafe"/></node>
      <node id="2" lat="-33.8686" lon="151.209"/>
      <node id="3" lat="-33.8687" lon="151.209"><tag k="railway" v="subway_entrance"/></node>
      <way id="10"><nd ref="1"/><nd ref="2"/><tag k="name" v="Library"/><tag k="amenity" v="library"/></way>
      <relation id="20"><member type="way" ref="10" role="outer"/><tag k="name" v="Park"/><tag k="leisure" v="park"/></relation>
      <relation id="306382"><member type="way" ref="10" role="outer"/><member type="way" ref="972806795" role="outer"/><tag k="name" v="Lamington National Park"/><tag k="type" v="boundary"/><tag k="leisure" v="nature_reserve"/></relation>
    </osm>"#;
    let pbf = common::write_pbf(
        work.path(),
        "source",
        xml,
        &[
            ("osmosis_replication_timestamp", "2026-09-10T00:00:00Z"),
            ("osmosis_replication_sequence_number", "12"),
        ],
    );
    let original = fs::read(&pbf).unwrap();
    let coverage = work.path().join("coverage.json");
    fs::write(&coverage, serde_json::to_vec(&json!({"type":"Polygon","coordinates":[[[151,-34],[152,-34],[152,-33],[151,-33],[151,-34]]]})).unwrap()).unwrap();
    let environment = Environment::from_values(
        [
            ("R2_ACCOUNT_ID".into(), "a".repeat(32)),
            ("R2_BUCKET".into(), "test-osm".into()),
            ("AWS_ACCESS_KEY_ID".into(), "test-access-key".into()),
            ("AWS_SECRET_ACCESS_KEY".into(), "test-secret-key".into()),
            ("OSM_WORKER_URL".into(), endpoints.worker.clone()),
            (
                "OSM_PUBLISH_TOKEN".into(),
                "test-publish-token-32-characters-minimum".into(),
            ),
            ("OSM_COMPUTE_WORKERS".into(), "2".into()),
            ("OSM_HTTP_RETRY_DELAY_MS".into(), "0".into()),
        ]
        .into(),
    );
    let runtime = Runtime::from_json(
        work.path(),
        include_str!("../config/runtime.json"),
        &environment,
    )
    .unwrap();
    let compute = runtime.compute_options();
    let publisher = Publisher::new(
        &runtime,
        &PublishConfig::from_environment(&environment).unwrap(),
    )
    .unwrap()
    .with_local_r2_endpoint(&endpoints.r2)
    .unwrap();
    let client = Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    assert!(
        publisher.read_state().unwrap().is_none(),
        "Test service must start with empty isolated storage"
    );
    assert!(
        object_keys(&client, &endpoints.worker).is_empty(),
        "Test service must start with empty isolated storage"
    );
    let entry = Region {
        id: "au-nsw".into(),
        extract: "australia-oceania/australia/new-south-wales".into(),
    };
    let data = work.path().join("data");
    let state = data.join(&entry.id);
    let initial = incremental::initialize(
        &InitOptions {
            input: pbf.clone(),
            state: state.clone(),
            coverage: coverage.clone(),
            region: entry.id.clone(),
            source_timestamp: "2026-09-10T00:00:00Z".into(),
            source_sequence: 12,
            source_sha256: hash_bytes(&original),
            replication_url: entry.updates_url(),
        },
        &compute,
    )
    .unwrap();
    assert_eq!(initial["count"], 3);
    assert_eq!(initial["excludedIncompleteRelationCount"], 1);
    let output = PathBuf::from(initial["output"].as_str().unwrap());
    let exclusions = fs::read_to_string(output.join("excluded-relations.jsonl")).unwrap();
    let excluded: Value = serde_json::from_str(exclusions.lines().next().unwrap()).unwrap();
    assert_eq!(excluded["id"], "osm_relation_306382");
    assert_eq!(
        query(&client, &endpoints.worker, None)["coverage"],
        "uncovered"
    );
    let mut verified_unpublished = false;
    let dispatch = publisher.coordinator();
    dispatch
        .start("bootstrap", std::slice::from_ref(&entry))
        .unwrap();
    let device = aura_osm::dispatch::device_id(&data).unwrap();
    let first_lease = dispatch.claim(&device, &[], 0).unwrap().lease.unwrap();
    let first = publisher
        .publish(&output, &first_lease, |event| {
            if event.stage == "publish" {
                assert_eq!(
                    query(&client, &endpoints.worker, None)["coverage"],
                    "uncovered",
                    "upload alone must not publish"
                );
                let keys = object_keys(&client, &endpoints.worker);
                assert!(keys.contains(&format!(
                    "manifests/{}.json",
                    initial["manifest"].as_str().unwrap()
                )));
                verified_unpublished = true;
            }
        })
        .unwrap();
    assert!(verified_unpublished);
    assert!(first["uploaded"].as_u64().unwrap() > 0);
    for _ in 0..2 {
        let result = query(&client, &endpoints.worker, None);
        assert_eq!(result["coverage"], "covered");
        assert_eq!(result["count"], 3);
        assert_eq!(
            ids(&result),
            ["osm_node_1", "osm_way_10", "osm_relation_20"]
                .map(String::from)
                .into()
        );
    }
    assert_eq!(
        ids(&query(&client, &endpoints.worker, Some("喫茶"))),
        ["osm_node_1".to_owned()].into()
    );
    let first_manifest = format!("manifests/{}.json", first["manifest"].as_str().unwrap());
    let first_objects = object_keys(&client, &endpoints.worker);

    let change = work.path().join("change.osc.gz");
    let mut compressed = GzEncoder::new(Vec::new(), Compression::default());
    compressed.write_all(br#"<osmChange version="0.6">
      <modify><node id="1" version="2" lat="-33.88" lon="151.209"><tag k="name" v="Moved Coffee"/><tag k="amenity" v="cafe"/></node></modify>
      <create><node id="50" version="1" lat="-33.8689" lon="151.209"><tag k="name" v="New Shop"/><tag k="shop" v="books"/></node></create>
    </osmChange>"#).unwrap();
    fs::write(&change, compressed.finish().unwrap()).unwrap();
    let updated = incremental::apply_diff(
        &ApplyOptions {
            input: change,
            state: state.clone(),
            sequence: 13,
            timestamp: "2026-09-10T00:02:00Z".into(),
            coverage: None,
        },
        &compute,
    )
    .unwrap();
    assert_eq!(updated["affectedObjects"], 5);
    assert_eq!(updated["count"], 4);
    let updated_output = PathBuf::from(updated["output"].as_str().unwrap());
    dispatch
        .start("update", std::slice::from_ref(&entry))
        .unwrap();
    let update_lease = dispatch.claim(&device, &[], 1).unwrap().lease.unwrap();
    assert_eq!(update_lease.slot, 1);
    let uploaded = publisher
        .publish(&updated_output, &update_lease, |_| {})
        .unwrap();
    assert!(uploaded["uploaded"].as_u64().unwrap() > 0);
    let result = query(&client, &endpoints.worker, None);
    assert_eq!(
        ids(&result),
        ["osm_node_50".to_owned()].into(),
        "moving a node must move the unchanged way and relation outside the old scan"
    );
    assert_eq!(result["revision"], uploaded["revision"]);
    assert!(object_keys(&client, &endpoints.worker).contains(&first_manifest));
    let resumed = publisher
        .publish(&updated_output, &update_lease, |_| {})
        .unwrap();
    assert_eq!(resumed["uploaded"], 0);
    assert!(resumed["reused"].as_u64().unwrap() > 0);
    let confirmed = publisher.read_state().unwrap().unwrap();
    let manifest = &confirmed
        .regions
        .iter()
        .find(|region| region.region == entry.id)
        .unwrap()
        .manifest;
    assert_eq!(manifest, resumed["manifest"].as_str().unwrap());

    let download = data.join(".downloads/au-nsw-integration");
    fs::create_dir_all(&download).unwrap();
    fs::write(
        download.join("source.url"),
        format!("{}\n", entry.source_url()),
    )
    .unwrap();
    fs::write(download.join("source.osm.pbf"), &original).unwrap();
    let retained = data.join(".downloads/au-nsw-other");
    fs::create_dir(&retained).unwrap();
    fs::write(
        retained.join("source.url"),
        "https://download.geofabrik.de/europe/andorra-latest.osm.pbf\n",
    )
    .unwrap();
    fs::write(retained.join("marker"), b"keep").unwrap();
    let cloud_objects = object_keys(&client, &endpoints.worker);
    assert!(first_objects.is_subset(&cloud_objects));
    cleanup_region(&entry, &data, manifest).unwrap();
    assert!(!state.exists());
    assert!(!download.exists());
    assert!(!data.join(".cleanup-au-nsw.json").exists());
    assert_eq!(fs::read(retained.join("marker")).unwrap(), b"keep");
    assert_eq!(fs::read(&pbf).unwrap(), original);
    let result = query(&client, &endpoints.worker, None);
    assert_eq!(result["coverage"], "covered");
    assert_eq!(ids(&result), ["osm_node_50".to_owned()].into());
    assert_eq!(result["revision"], resumed["revision"]);
    assert_eq!(publisher.read_state().unwrap().unwrap(), confirmed);
    assert_eq!(object_keys(&client, &endpoints.worker), cloud_objects);
}
