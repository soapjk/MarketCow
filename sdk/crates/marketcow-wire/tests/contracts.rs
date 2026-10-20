use marketcow_wire::{btc, FILE_DESCRIPTOR_SET, MARKET_SUBPROTOCOL, PRICE_SUBPROTOCOL};
use prost::Message;

#[test]
fn protocol_identity_and_presence_are_preserved() {
    assert_eq!(MARKET_SUBPROTOCOL, "marketcow.btc5m.market-local.v7");
    assert_eq!(PRICE_SUBPROTOCOL, "marketcow.btc-price.v3");
    assert!(!FILE_DESCRIPTOR_SET.is_empty());
    let absent = btc::Trade::default();
    let explicit_false = btc::Trade { buyer_is_maker: Some(false), ..Default::default() };
    assert_ne!(absent.encode_to_vec(), explicit_false.encode_to_vec());
    let decoded = btc::Trade::decode(explicit_false.encode_to_vec().as_slice()).unwrap();
    assert_eq!(decoded.buyer_is_maker, Some(false));
}
