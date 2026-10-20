use marketcow_contracts::typed_v3;

#[test]
fn service_import_is_the_same_type_not_a_second_codec() {
    let service = typed_v3::btc::Trade::default();
    let sdk_wire: marketcow_wire::btc::Trade = service;
    let _service: typed_v3::btc::Trade = sdk_wire;
    assert_eq!(typed_v3::MARKET_SCHEMA, marketcow_wire::MARKET_SCHEMA);
}
