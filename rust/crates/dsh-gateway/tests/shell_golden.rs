//! The page the Rust shell renders must equal the one the TypeScript Host served for the
//! same boot graph, byte for byte, apart from the one documented deviation below.

use dsh_gateway::shell::{render_index, CAPTURED_BOOT};

/**
 * The single intentional difference from the TypeScript page: this Host renders no OS
 * directory dialog on a display, so the boot graph loads the browse picker
 * (`directoryPicker/list` and `directoryPicker/createDirectory`) instead of the native
 * one that calls `directoryPicker/pick`. Everything else must stay byte-identical.
 */
fn with_browse_picker(page: &str) -> String {
    page.replace("directory-picker-native", "directory-picker-browse")
}

#[test]
fn rendered_index_matches_the_typescript_host() {
    let dist = include_str!("golden/dist-index.html");
    let shipped = include_str!("golden/ts-index.html");
    let expected = with_browse_picker(shipped);
    assert!(
        expected.contains("directory-picker-browse"),
        "the recorded page must contain the picker this test substitutes",
    );
    let boot: serde_json::Value = serde_json::from_str(CAPTURED_BOOT).unwrap();
    let got = render_index(dist, &boot);
    if got != expected {
        let at = got
            .bytes()
            .zip(expected.bytes())
            .position(|(a, b)| a != b)
            .unwrap_or(got.len().min(expected.len()));
        panic!(
            "first difference at byte {at} (got {} bytes, expected {} bytes), beyond the documented picker deviation:\n got: {:?}\nwant: {:?}",
            got.len(), expected.len(),
            &got[at.saturating_sub(60)..(at + 80).min(got.len())],
            &expected[at.saturating_sub(60)..(at + 80).min(expected.len())],
        );
    }
}
