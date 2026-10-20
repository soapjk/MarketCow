//! Generate complete read-only field views from the canonical descriptor.
//! No runtime reflection, JSON, hash or second state machine. Cold Proto export
//! and decode are explicit opt-in methods, never part of an ordinary getter.
use prost::Message;
use prost_types::{DescriptorProto, FieldDescriptorProto as Field, FileDescriptorSet};
use std::{collections::BTreeMap, env, fmt::Write, path::PathBuf};

fn snake(s: &str) -> String {
    let mut out = String::new();
    for (i, ch) in s.chars().enumerate() {
        if ch.is_uppercase() && i > 0 {
            out.push('_');
        }
        out.extend(ch.to_lowercase());
    }
    out
}
fn camel(s: &str) -> String {
    s.split('_')
        .map(|s| {
            let mut chars = s.chars();
            chars
                .next()
                .map(|c| c.to_uppercase().collect::<String>() + chars.as_str())
                .unwrap_or_default()
        })
        .collect()
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-env-changed=MARKETCOW_GENERATE_PY_STUB");
    let descriptors = FileDescriptorSet::decode(marketcow_wire::FILE_DESCRIPTOR_SET)?;
    let mut messages: BTreeMap<String, (String, String, DescriptorProto)> = BTreeMap::new();
    for f in descriptors.file {
        let package = f.package.unwrap_or_default();
        let alias = match package.as_str() {
            "marketcow.common.v1" => "common",
            "marketcow.btc.v3" => "btc",
            "marketcow.btc.history.v3" => "wire_history",
            "marketcow.polymarket.btc5m.v7" => "market",
            "marketcow.facts.v1" => "facts",
            _ => continue,
        };
        for m in f.message_type {
            let name = m.name.clone().unwrap();
            // Private policy tokens are not part of a public fact view.
            if matches!(name.as_str(), "SdkAccessPolicy" | "SdkCustomer") {
                continue;
            }
            let python = format!("Fact{}{}", camel(alias), name);
            messages.insert(
                format!(".{package}.{name}"),
                (format!("{alias}::{name}"), python, m),
            );
        }
    }
    let mut rust = String::from(
        "use pyo3::prelude::*;\nuse std::sync::Arc;\nuse marketcow_sdk::{btc,common,facts,market,wire_history};\n",
    );
    let mut stub =
        String::from("# Generated from the canonical Proto descriptor; do not edit.\n\n");
    let mut register =
        String::from("pub(crate) fn register(module: &Bound<'_,PyModule>) -> PyResult<()> {\n");
    for (rust_type, python, m) in messages.values() {
        writeln!(
            rust,
            "#[pyclass(frozen,module=\"marketcow_sdk._native\")]\n#[derive(Clone)]\npub(crate) struct {python}(pub(crate) Arc<{rust_type}>);\n#[pymethods]\nimpl {python} {{"
        )?;
        writeln!(stub, "class {python}:")?;
        writeln!(
            rust,
            "/// Explicit cold export. Not called by getters, subscriptions or validators.\nfn proto_bytes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py,pyo3::types::PyBytes>> {{ super::cold::export(py,self.0.clone()) }}\n#[staticmethod]\nfn from_proto_bytes(py: Python<'_>, data: &Bound<'_,pyo3::types::PyBytes>) -> PyResult<Self> {{ super::cold::decode::<{rust_type}>(py,data).map(|v|Self(Arc::new(v))) }}"
        )?;
        writeln!(
            stub,
            "    def proto_bytes(self) -> bytes: ...\n    @staticmethod\n    def from_proto_bytes(data: bytes) -> {python}: ..."
        )?;
        for field in &m.field {
            let name = field.name.as_deref().unwrap();
            let repeated = field.label == Some(3);
            let oneof = field
                .oneof_index
                .filter(|_| !field.proto3_optional.unwrap_or(false));
            let message = field.r#type == Some(11);
            let optional =
                !repeated && (message || field.proto3_optional.unwrap_or(false) || oneof.is_some());
            let (base, pybase, expression) = field_type(field, &messages);
            let rust_return = if repeated {
                format!("Vec<{base}>")
            } else if optional {
                format!("Option<{base}>")
            } else {
                base
            };
            let py_return = if repeated {
                format!("list[{pybase}]")
            } else if optional {
                format!("{pybase} | None")
            } else {
                pybase
            };
            let expression = if let Some(index) = oneof {
                let group = m.oneof_decl[index as usize].name.as_deref().unwrap();
                let module = snake(m.name.as_deref().unwrap());
                let alias = rust_type.split("::").next().unwrap();
                format!(
                    "match self.0.{group}.as_ref() {{ Some({alias}::{module}::{}::{}(v)) => Some({expression}), _=>None }}",
                    camel(group),
                    camel(name)
                )
            } else if repeated {
                format!("self.0.{name}.iter().map(|v| {expression}).collect()")
            } else if optional {
                format!("self.0.{name}.as_ref().map(|v| {expression})")
            } else {
                format!("{{ let v=&self.0.{name}; {expression} }}")
            };
            writeln!(
                rust,
                "#[getter]\nfn r#{name}(&self) -> {rust_return} {{ {expression} }}"
            )?;
            writeln!(
                stub,
                "    @property\n    def {name}(self) -> {py_return}: ..."
            )?;
        }
        writeln!(rust, "}}")?;
        writeln!(stub)?;
        writeln!(register, "module.add_class::<{python}>()?;")?;
    }
    register.push_str("Ok(())\n}\n");
    rust.push_str(&register);
    std::fs::write(
        PathBuf::from(env::var("OUT_DIR")?).join("fact_views.rs"),
        rust,
    )?;
    if env::var_os("MARKETCOW_GENERATE_PY_STUB").is_some() {
        std::fs::write("../python/src/marketcow_sdk/_facts.pyi", stub)?;
    }
    Ok(())
}
fn field_type(
    field: &Field,
    messages: &BTreeMap<String, (String, String, DescriptorProto)>,
) -> (String, String, String) {
    if field.r#type == Some(11) {
        let (_, name, _) = &messages[field.type_name.as_ref().unwrap()];
        return (
            name.clone(),
            name.clone(),
            format!("{name}(Arc::new(v.clone()))"),
        );
    }
    let (rust, py, expr) = match field.r#type.unwrap() {
        1 => ("f64", "float", "*v"),
        2 => ("f32", "float", "*v"),
        3 | 16 | 18 => ("i64", "int", "*v"),
        4 | 6 => ("u64", "int", "*v"),
        5 | 15 | 17 | 14 => ("i32", "int", "*v"),
        7 | 13 => ("u32", "int", "*v"),
        8 => ("bool", "bool", "*v"),
        9 => ("String", "str", "v.clone()"),
        12 => (
            "Py<pyo3::types::PyBytes>",
            "bytes",
            "Python::attach(|py| pyo3::types::PyBytes::new(py,v).unbind())",
        ),
        other => panic!("unsupported field type {other}"),
    };
    (rust.into(), py.into(), expr.into())
}
