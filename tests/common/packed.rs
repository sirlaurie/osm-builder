use aura_osm::format::hash_bytes;
use serde_json::{Map, Value, json};
use std::{fs, path::Path};

pub fn packed_view(output: &Path, manifest: &Value) -> Value {
    if manifest["schema"] != 3 {
        return manifest.clone();
    }
    let packs = manifest["packs"].as_array().expect("manifest packs");
    let mut cells = Map::new();
    for (group, digest) in manifest["groups"].as_object().expect("manifest groups") {
        let digest = digest.as_str().expect("group index hash");
        let bytes = fs::read(output.join(format!("indexes/{digest}.json"))).expect("group index");
        assert_eq!(hash_bytes(&bytes), digest);
        let index: Value = serde_json::from_slice(&bytes).expect("group index JSON");
        assert_eq!(index["group"], *group);
        for (cell, pages) in index["cells"].as_object().expect("group index cells") {
            let pages = pages
                .as_array()
                .expect("cell pages")
                .iter()
                .map(|page| {
                    let pack = &index["packs"][page[1].as_u64().expect("pack index") as usize];
                    let global = packs
                        .iter()
                        .position(|candidate| candidate == pack)
                        .expect("group index pack is listed in the manifest");
                    json!([page[0], global, page[2], page[3]])
                })
                .collect::<Vec<_>>();
            assert!(cells.insert(cell.clone(), json!(pages)).is_none());
        }
    }
    let mut view = manifest.clone();
    view["cells"] = Value::Object(cells);
    view
}
