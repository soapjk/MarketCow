import importlib.util
from pathlib import Path
import tempfile
import tomllib
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("sdk_export", ROOT / "scripts/export_sdk_repository.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class RepositorySplitTests(unittest.TestCase):
    def test_public_proto_has_one_source_and_no_service_dependencies(self):
        wire = ROOT / "sdk/crates/marketcow-wire"
        manifest = tomllib.loads((wire / "Cargo.toml").read_text())
        self.assertEqual(set(manifest["dependencies"]), {"prost"})
        self.assertFalse((ROOT / "proto/marketcow/btc").exists())
        self.assertFalse((ROOT / "proto/marketcow/common").exists())
        self.assertFalse((ROOT / "proto/marketcow/facts").exists())
        self.assertIn("pub use marketcow_wire::*", (ROOT / "crates/marketcow-contracts/src/typed_v3.rs").read_text())

    def test_all_paths_in_sdk_workspace_stay_inside_sdk(self):
        sdk = ROOT / "sdk"
        for manifest_path in sdk.glob("**/Cargo.toml"):
            if "target" in manifest_path.parts:
                continue
            manifest = tomllib.loads(manifest_path.read_text())
            for section in ("dependencies", "build-dependencies", "dev-dependencies"):
                for dependency in manifest.get(section, {}).values():
                    if isinstance(dependency, dict) and "path" in dependency:
                        self.assertTrue((manifest_path.parent / dependency["path"]).resolve().is_relative_to(sdk.resolve()))

    def test_export_contains_no_binary_credentials_or_server(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "candidate"
            count = module.export(target)
            self.assertGreater(count, 40)
            self.assertTrue((target / "LICENSE").is_file())
            self.assertTrue((target / "python/src/marketcow_sdk/py.typed").is_file())
            self.assertTrue((target / "crates/marketcow-wire/proto/marketcow/btc/v3/price.proto").is_file())
            self.assertFalse(any(p.suffix in {".so", ".whl", ".sqlite3"} for p in target.rglob("*")))
            self.assertFalse((target / ".git").exists())
            self.assertFalse((target / "crates/marketcowd").exists())
            with self.assertRaisesRegex(ValueError, "must not exist"):
                module.export(target)

    def test_export_refuses_a_linked_input_before_creating_destination(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            original = root / "real"
            original.write_text("not an export input")
            linked = root / "link"
            linked.symlink_to(original)
            # Exercise the same source-file validation on a minimal fixed list.
            fake = root / "repository"
            (fake / "sdk").mkdir(parents=True)
            (fake / "sdk/Cargo.toml").symlink_to(original)
            with patch.object(module, "SOURCE_ROOTS", ()), patch.object(module, "SOURCE_FILES", ("Cargo.toml",)):
                with self.assertRaisesRegex(ValueError, "linked SDK input"):
                    list(module.sdk_files(fake))
