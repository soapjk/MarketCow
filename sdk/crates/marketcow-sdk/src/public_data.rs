//! Explicit public REST/facts reads, never an execution prerequisite. No order,
//! account or signing operation is part of this client.
use crate::{Client, Result, btc, common, error::require, facts, market};
use prost::Message;
use reqwest::Method;

impl Client {
    pub async fn terms(&self, key: market::MarketKey) -> Result<facts::ExecutionFactsResponse> {
        let request = facts::MarketRequest {
            request_id: uuid::Uuid::new_v4().to_string(),
            key: Some(key.clone()),
        };
        let (value, _) = self
            .request::<facts::ExecutionFactsResponse>(
                Method::POST,
                "/v1/facts/polymarket/execution",
                &[],
                Some(request.encode_to_vec()),
                true,
                false,
            )
            .await?;
        require(
            value.schema_version == "marketcow.facts.execution.v1"
                && value.request_id == request.request_id
                && value.meta.as_ref().and_then(|m| m.key.as_ref()) == Some(&key),
            "public terms identity",
        )?;
        Ok(value) // Absent supplier fields stay absent; policies remain separate.
    }
    /// Real upstream read. This is not the MarketCow in-memory order book and
    /// not mandatory for trading; trading clients may contact exchanges directly.
    pub async fn official_book(
        &self,
        key: market::MarketKey,
        stream_id: String,
        token_id: String,
    ) -> Result<facts::OfficialBookResponse> {
        require(
            !stream_id.is_empty() && (token_id == key.up_token_id || token_id == key.down_token_id),
            "official book market/token binding",
        )?;
        let request = facts::OfficialBookRequest {
            request_id: uuid::Uuid::new_v4().to_string(),
            key: Some(key.clone()),
            stream_id: stream_id.clone(),
            token_id: token_id.clone(),
            representation: "typed".into(),
        };
        let (value, _) = self
            .request::<facts::OfficialBookResponse>(
                Method::POST,
                "/v1/facts/polymarket/official-book",
                &[],
                Some(request.encode_to_vec()),
                true,
                false,
            )
            .await?;
        require(
            value.schema_version == "marketcow.facts.official-book.v1"
                && value.request_id == request.request_id
                && value.acquisition == "official_rest"
                && !value.cache_used
                && !value.ws_dependency,
            "official book acquisition",
        )?;
        require(
            value.dispatch.as_ref().is_some_and(|b| {
                b.key.as_ref() == Some(&key) && b.stream_id == stream_id && b.token_id == token_id
            }),
            "official book dispatch identity",
        )?;
        require(
            matches!(&value.body, Some(facts::official_book_response::Body::Book(b))
            if b.market == key.condition_id && b.asset_id == token_id),
            "official book response identity",
        )?;
        Ok(value)
    }
    pub async fn official_btc_trade(
        &self,
        source: common::Source,
    ) -> Result<facts::BtcRestResponse> {
        let request = facts::BtcRestRequest {
            request_id: uuid::Uuid::new_v4().to_string(),
            source: source as i32,
        };
        let (value, _) = self
            .request::<facts::BtcRestResponse>(
                Method::POST,
                "/v1/facts/btc/rest",
                &[],
                Some(request.encode_to_vec()),
                true,
                false,
            )
            .await?;
        require(
            value.schema_version == "marketcow.facts.btc-rest.v1"
                && value.request_id == request.request_id
                && value.source == source as i32
                && source == common::Source::BinanceSpotTrade
                && value.instrument_id == "BTCUSDT.BINANCE"
                && value.acquisition == "official_rest"
                && !value.cache_used
                && !value.ws_dependency,
            "official BTC REST acquisition",
        )?;
        crate::price::validate_trade(
            value.latest_trade.as_ref().ok_or_else(|| {
                crate::Error::Protocol("official REST latest trade absent".into())
            })?,
        )?;
        Ok(value)
    }
    /// A single explicit snapshot read, sharing an existing subscription where
    /// present. This does not download history or subscribe other sources.
    pub async fn latest_price(
        &self,
        source: common::Source,
    ) -> Result<std::sync::Arc<btc::PriceFrame>> {
        let mut stream = self.shared_prices(source)?;
        let result = tokio::time::timeout(self.limits().stream_idle_timeout, async {
            loop {
                match stream.next().await? {
                    Some(crate::PriceDelivery::Frame { frame, .. }) => return Ok(frame),
                    Some(crate::PriceDelivery::Recovering { reason, .. }) => {
                        return Err(crate::Error::Protocol(format!(
                            "source recovering: {reason}"
                        )));
                    }
                    None => return Err(crate::Error::Cancelled),
                }
            }
        })
        .await
        .unwrap_or_else(|_| Err(crate::Error::Limit("latest price deadline")));
        stream.close();
        result
    }
}
