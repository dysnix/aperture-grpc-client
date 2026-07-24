use {
    aperture_grpc_client::{
        ApertureClientConfig, ApertureGrpcClient, SimulationStatus, SubscribeFilters, VoteFilter,
    },
    futures_util::StreamExt,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "aperture_grpc_client=info,subscribe=info".to_string()),
        )
        .init();

    let endpoint = std::env::var("APERTURE_GRPC_ENDPOINT")
        .unwrap_or_else(|_| "https://aperture-txstream.rpcfast.com:443".to_string());
    let mut config = ApertureClientConfig::new(endpoint);
    if let Ok(token) = std::env::var("APERTURE_X_TOKEN") {
        config = config.with_x_token(token);
    }
    let client = ApertureGrpcClient::new(config);
    let filters = SubscribeFilters::default()
        .vote(VoteFilter::NonVoteOnly)
        .include_simulation();
    let mut stream = Box::pin(client.subscribe_with_reconnect(filters));

    while let Some(message) = stream.next().await {
        let transaction = message?;
        let Some(simulation) = transaction.simulation else {
            continue;
        };
        let primary_signature = transaction
            .signatures
            .first()
            .map(|signature| bs58::encode(signature).into_string())
            .unwrap_or_else(|| "<missing>".to_string());

        let status =
            SimulationStatus::try_from(simulation.status).unwrap_or(SimulationStatus::Unspecified);

        println!(
            "slot={} sig={} status={status:?} bank_slot={:?} error={:?}",
            transaction.slot, primary_signature, simulation.bank_slot, simulation.error
        );
    }

    Ok(())
}
