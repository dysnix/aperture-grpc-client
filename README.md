# aperture-grpc-client

Rust client for Aperture's lightweight decoded transaction gRPC stream.

It wraps the generated [`aperture-grpc-proto`](https://github.com/dysnix/aperture-grpc-proto)
bindings with tuned HTTP/2 defaults, keepalives, byte-safe filters, and an
automatic reconnecting transaction stream. It supports both single-transaction
and batched txstream RPCs.

## Transaction versions

Version 0.5 adds v1 transactions and `TransactionConfig`. V1 is delivered
by default on both RPCs with the existing subscription request: no opt-in or
request-schema change is needed. Upgrade generated clients to understand the
new enum value and config; older protobuf decoders ignore the additional field.

`TransactionVersion` values remain `Legacy = 0`, `V0 = 1`; `V1 = 2` is new.
Full v1 responses carry `transaction_config`, even if all its fields are absent.
Priority fee is **total lamports**. Absent CU and loaded account data limits
mean zero; absent heap size means 32 KiB. Legacy/v0 have no config. In v1 all
accounts are static; no ALT addresses are loaded. Signatures-only responses
retain the version but omit config along with the message payload.

## Install

```toml
[dependencies]
aperture-grpc-client = "0.5.0"
```

Publish `aperture-grpc-proto` before publishing this crate; the client depends
on the matching proto crate version.

For unreleased development builds:

```toml
[dependencies]
aperture-grpc-client = { git = "https://github.com/dysnix/aperture-grpc-client" }
```

## Example

```rust,no_run
use aperture_grpc_client::{
    ApertureClientConfig, ApertureGrpcClient, SubscribeFilters, VoteFilter,
};
use futures_util::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ApertureClientConfig::new("https://aperture-txstream.rpcfast.com:443")
        .with_x_token("rpcfast-token");
    let client = ApertureGrpcClient::new(config);
    let filters = SubscribeFilters::default().vote(VoteFilter::NonVoteOnly);
    let mut stream = Box::pin(client.subscribe_with_reconnect(filters));

    while let Some(next) = stream.next().await {
        let tx = next?;
        println!(
            "slot={} index={} sigs={} alt_resolution={:?}",
            tx.slot,
            tx.index,
            tx.signatures.len(),
            tx.alt_resolution
        );
    }

    Ok(())
}
```

`DecodedTransaction.alt_resolution` is `Some("FULL")` when the emitted account
list is complete, `Some("PARTIAL")` when one or more ALT entries could not be
resolved, and `None` when the server does not provide resolution status. The
field is additive on the wire, so older protobuf clients continue to decode
responses and ignore it.

For lower per-message overhead, subscribe to batches:

```rust,no_run
use aperture_grpc_client::{
    ApertureClientConfig, ApertureGrpcClient, SubscribeFilters, VoteFilter,
};
use futures_util::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ApertureClientConfig::new("https://aperture-txstream.rpcfast.com:443")
        .with_x_token("rpcfast-token");
    let client = ApertureGrpcClient::new(config);
    let filters = SubscribeFilters::default()
        .vote(VoteFilter::NonVoteOnly)
        .signatures_only();
    let mut stream = Box::pin(client.subscribe_batches_with_reconnect(filters));

    while let Some(next) = stream.next().await {
        let batch = next?;
        println!("transactions={}", batch.transactions.len());
    }

    Ok(())
}
```

Request real-time transaction simulation when you need a predicted execution
result alongside each deshredded transaction:

```rust,no_run
use aperture_grpc_client::{
    ApertureClientConfig, ApertureGrpcClient, SimulationStatus, SubscribeFilters,
    VoteFilter,
};
use futures_util::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ApertureClientConfig::new("https://aperture-txstream.rpcfast.com:443")
        .with_x_token("rpcfast-token");
    let client = ApertureGrpcClient::new(config);
    let filters = SubscribeFilters::default()
        .vote(VoteFilter::NonVoteOnly)
        .include_simulation();
    let mut stream = Box::pin(client.subscribe_with_reconnect(filters));

    while let Some(next) = stream.next().await {
        let tx = next?;
        if let Some(simulation) = tx.simulation {
            let status = SimulationStatus::try_from(simulation.status)
                .unwrap_or(SimulationStatus::Unspecified);
            println!(
                "slot={} status={status:?} bank_slot={:?} error={:?}",
                tx.slot, simulation.bank_slot, simulation.error
            );
        }
    }

    Ok(())
}
```

Run the included example:

```bash
APERTURE_GRPC_ENDPOINT=https://aperture-txstream.rpcfast.com:443 APERTURE_X_TOKEN=rpcfast-token cargo run --example subscribe
```

## Defaults

- endpoint: `https://aperture-txstream.rpcfast.com:443`
- TCP connect timeout: 3s
- HTTP/2 keepalive interval: 10s
- HTTP/2 keepalive timeout: 3s
- keepalive while idle: enabled
- TCP_NODELAY: enabled
- HTTP/2 adaptive window: enabled
- initial stream and connection windows: 16 MiB
- protobuf encode/decode limits: 16 MiB
- HTTPS endpoints use native TLS trust roots with the `ring` crypto provider by default
- authentication metadata: optional `X-Token` header only
- reconnect: enabled forever, exponential backoff from 100ms to 5s

## Filters

Filters use raw Solana bytes:

- `signature`: optional 64-byte primary signature filter.
- `account_include`: 32-byte pubkeys, match any static or loaded account.
- `account_exclude`: 32-byte pubkeys, reject if any static or loaded account matches.
- `account_required`: 32-byte pubkeys, require all listed accounts.
- `vote`: all, vote-only, or non-vote-only.
- `signatures_only`: omit account/instruction payloads and keep only
  slot/index/vote/timestamp/version/signatures.
- `include_simulation`: wait for transaction simulation and append status,
  error, simulation slot, and timing to each transaction.
  It can be combined with `signatures_only` for a compact
  signature-and-result stream.

Instruction indexes are resolved by concatenating:

```text
static_account_keys + loaded_writable_addresses + loaded_readonly_addresses
```

The stream is pre-execution by default and does not include confirmed execution
metadata such as balances, rewards, or inner instructions. An
`include_simulation` subscription adds predicted simulation status, error,
simulation slot, and timing; it is not confirmation or finality.

### Configurable simulation details

These APIs are part of unreleased version 0.6. Local builds require the matching
`../aperture-grpc-proto` checkout. Publish proto 0.6 before the client and switch
consumers to the registry dependency after publication.

Presence of `simulation_config` enables simulation on either TxStream RPC and
requires the same `simulation` entitlement as `include_simulation`. Without a
config, `include_simulation: true` retains status, error, consumed compute units,
bank slot and timing; all additional details remain disabled.

```json
{
  "simulation_config": {
    "include": {
      "compute_units": true,
      "lamport_deltas": true,
      "account_deltas": false,
      "token_balance_deltas": true,
      "inner_instructions": false,
      "logs": false,
      "return_data": false
    },
    "account_include": [],
    "owner_include": [],
    "changed_only": true,
    "account_data": { "pre": false, "post": false }
  }
}
```

All `include` flags default to false. Status, error, bank slot and timing are
always returned. An explicit config overrides the legacy detail defaults even
when `include_simulation` is also true. `account_data.pre/post` require
`include.account_deltas`; absent data differs from present empty data.

`simulation.simulation_state_deltas` contains the requested lamport, account and
token balance lists for writable accounts. Missing pre/post account or token
state represents creation/closure or conversion to/from a token account.
Token amounts are raw unsigned integers with mint, token authority owner and
program ID; collection performs no mint lookup and returns no decimals or UI
amounts. Token-2022 amounts describe the base token balance, not every extension
(e.g. withheld fees or confidential balances). Pre-state includes the fee payer
balance before fee deduction. Failed simulations report effective rollback
state, preserving applicable fee and nonce changes rather than failed writes.
These are predictions against `bank_slot`, not confirmed transaction effects.

The nested filters affect deltas only, independently of transaction subscription
filters. Keys are 32-byte pubkeys (base64 in protobuf JSON). Empty lists are
unrestricted; account selection and owner selection are ANDed. Owner matching
accepts either pre- or post-account owner program, not the token authority.
`changed_only` defaults to true; false includes unchanged selected writable
accounts (and unchanged token accounts for token balances). Account deltas carry
`changed` even when data bytes are omitted. Lamport and token lists compare their
own balance/state values. Inner instructions carry resolved keys and stack
heights and work with `signatures_only` and Aperture ALT cache misses; delta
filters do not filter logs or CPI.
