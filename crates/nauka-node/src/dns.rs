//! The built-in geo-DNS front door.
//!
//! Every node answers authoritative DNS on :53 (opt out with --no-dns /
//! NAUKA_NO_DNS=true): delegate any name to a few nodes (NS + glue at your
//! registrar, nothing else) and the cluster becomes its own GeoDNS —
//! queries are answered with the closest ALIVE nodes to the asker,
//! straight from the live membership. No zone file, no region buckets,
//! no third-party API: the map IS the cluster.
//!
//! Geography comes from one mmdb file (DB-IP City Lite, free, no
//! account), refreshed monthly, used on BOTH sides: the asker's IP
//! (EDNS Client Subnet when the resolver sends it, the resolver's own
//! address otherwise — that is the client's ISP, the standard GeoDNS
//! approximation) and the members' advertised IPs. Great-circle
//! distance sorts the members; the three closest living ones are the
//! answer. Before the database is ready, or for askers it cannot place,
//! the answer degrades to the first three living members: reachable is
//! better than optimal.
//!
//! The responder is deliberately minimal: A (the point), NS and SOA
//! (synthesized, so delegation checks pass), empty NOERROR for the
//! rest. No recursion, no zone transfers, tiny responses — the
//! amplification surface of an authoritative-only server.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, RwLock};

use hickory_server::net::runtime::Time;
use hickory_server::proto::op::{Header, HeaderCounts, Metadata, ResponseCode};
use hickory_server::proto::rr::{rdata, Name, RData, Record, RecordType};
use hickory_server::server::{Request, RequestHandler, ResponseHandler, ResponseInfo};
use hickory_server::zone_handler::MessageResponseBuilder;

use crate::api::{ApiState, NodeLocation};

/// Answer TTL: short enough that a dead node leaves the world's caches
/// within a minute, long enough that resolvers do the caching work.
const DNS_TTL: u32 = 60;
/// How many of the closest members one answer carries, at most.
const ANSWER_NODES: usize = 3;
/// A member joins the answer only within this many kilometres of the
/// CLOSEST one: clients pick any returned address at random, so a far
/// filler would get real traffic.
const NEIGHBORHOOD_KM: f64 = 2000.0;
/// The database is refreshed once it is older than this many days
/// (DB-IP publishes monthly).
const MMDB_MAX_AGE_DAYS: u64 = 40;

pub struct GeoDns {
    state: Arc<ApiState>,
    db: RwLock<Option<maxminddb::Reader<Vec<u8>>>>,
    /// Member IP → coordinates, resolved lazily against the database.
    positions: RwLock<HashMap<String, Option<(f64, f64)>>>,
}

impl GeoDns {
    pub fn new(state: Arc<ApiState>) -> Arc<Self> {
        Arc::new(Self {
            state,
            db: RwLock::new(None),
            positions: RwLock::new(HashMap::new()),
        })
    }

    fn lookup(&self, ip: IpAddr) -> Option<(f64, f64)> {
        let guard = self.db.read().ok()?;
        let reader = guard.as_ref()?;
        let city: maxminddb::geoip2::City = reader.lookup(ip).ok()?.decode().ok()??;
        Some((city.location.latitude?, city.location.longitude?))
    }

    /// Publishes the location of THIS node for the public HTTP endpoint.
    /// The data comes from the exact database used to place DNS clients,
    /// so the UI describes the storage node it actually reached.
    fn node_location(&self) -> Option<NodeLocation> {
        let ip = self
            .state
            .self_id
            .parse::<std::net::SocketAddr>()
            .ok()
            .map(|addr| addr.ip())?;
        let guard = self.db.read().ok()?;
        let reader = guard.as_ref()?;
        let city: maxminddb::geoip2::City = reader.lookup(ip).ok()?.decode().ok()??;
        let name = city.city.names.english.or(city.city.names.french)?;
        NodeLocation::new(name, city.country.iso_code?)
    }

    fn publish_node_location(&self) {
        if let (Some(location), Ok(mut current)) =
            (self.node_location(), self.state.node_location.write())
        {
            *current = Some(location);
        }
    }

    /// A member's coordinates, cached — the member set is small and
    /// stable, the database changes monthly (cache cleared on reload).
    fn member_position(&self, addr: &str) -> Option<(f64, f64)> {
        if let Some(hit) = self.positions.read().ok()?.get(addr) {
            return *hit;
        }
        let ip: IpAddr = addr.split(':').next()?.parse().ok()?;
        let pos = self.lookup(ip);
        if let Ok(mut cache) = self.positions.write() {
            cache.insert(addr.to_string(), pos);
        }
        pos
    }

    /// The closest living members to `client`, by great-circle distance
    /// — but ONLY the closest one's neighborhood. Resolvers and client
    /// OSes reorder A records at will (RFC 6724 address selection beat
    /// our carefully sorted answer on the very first real-world test:
    /// curl picked Helsinki over Singapore from Thailand), so every
    /// returned address must be a GOOD answer, not a ranked list: nodes
    /// further than the closest plus [`NEIGHBORHOOD_KM`] stay out. With
    /// no database or an unplaceable client: the first living members.
    fn best_nodes(&self, client: IpAddr) -> Vec<IpAddr> {
        let members = self.state.app.members();
        let liveness = self.state.health.snapshot();
        let mut alive: Vec<&String> = members
            .values()
            .filter(|a| liveness.get(*a).copied().unwrap_or(true))
            .collect();
        alive.sort(); // stable base order
        let client_pos = self.lookup(client);
        if let Some((clat, clon)) = client_pos {
            let mut ranked: Vec<(&String, f64)> = alive
                .iter()
                .map(|a| {
                    let d = self
                        .member_position(a)
                        .map(|(lat, lon)| haversine_km(clat, clon, lat, lon))
                        .unwrap_or(f64::MAX);
                    (*a, d)
                })
                .collect();
            ranked.sort_by(|a, b| a.1.total_cmp(&b.1));
            if let Some(&(_, closest)) = ranked.first() {
                let cutoff = closest + NEIGHBORHOOD_KM;
                return ranked
                    .into_iter()
                    .filter(|(_, d)| *d <= cutoff)
                    .filter_map(|(a, _)| a.split(':').next()?.parse().ok())
                    .take(ANSWER_NODES)
                    .collect();
            }
        }
        alive
            .into_iter()
            .filter_map(|a| a.split(':').next()?.parse().ok())
            .take(ANSWER_NODES)
            .collect()
    }
}

impl GeoDns {
    /// The living members a client standing next to THIS node would
    /// plausibly share a DNS answer with: within [`NEIGHBORHOOD_KM`]
    /// of self, self excluded. `None` when geography is unavailable
    /// (no database yet, or self unplaceable) — the caller falls back
    /// to the whole membership.
    pub fn neighborhood_of_self(&self) -> Option<Vec<String>> {
        let (slat, slon) = self.member_position(&self.state.self_id)?;
        let members = self.state.app.members();
        let liveness = self.state.health.snapshot();
        Some(
            members
                .values()
                .filter(|a| **a != self.state.self_id)
                .filter(|a| liveness.get(*a).copied().unwrap_or(true))
                .filter(|a| {
                    self.member_position(a).is_some_and(|(lat, lon)| {
                        haversine_km(slat, slon, lat, lon) <= NEIGHBORHOOD_KM
                    })
                })
                .cloned()
                .collect(),
        )
    }
}

/// Great-circle distance, kilometres. Precision is irrelevant here —
/// only the ORDER of candidates matters.
fn haversine_km(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let (la1, lo1, la2, lo2) = (
        lat1.to_radians(),
        lon1.to_radians(),
        lat2.to_radians(),
        lon2.to_radians(),
    );
    let dlat = la2 - la1;
    let dlon = lo2 - lo1;
    let a = (dlat / 2.0).sin().powi(2) + la1.cos() * la2.cos() * (dlon / 2.0).sin().powi(2);
    6371.0 * 2.0 * a.sqrt().asin()
}

/// The asker's address for geo purposes: the resolver's own source
/// address — the client's ISP, the standard GeoDNS approximation.
/// (EDNS Client Subnet support is a planned refinement: public
/// resolvers forward the client's prefix in it.)
fn asker_ip(request: &Request) -> IpAddr {
    request.src().ip()
}

#[async_trait::async_trait]
impl RequestHandler for GeoDns {
    async fn handle_request<R: ResponseHandler, T: Time>(
        &self,
        request: &Request,
        mut response_handle: R,
    ) -> ResponseInfo {
        let builder = MessageResponseBuilder::from_message_request(request);
        let mut metadata = Metadata::response_from_request(&request.metadata);
        metadata.authoritative = true;

        let query = match request.request_info() {
            Ok(info) => info.query.clone(),
            Err(_) => {
                metadata.response_code = ResponseCode::FormErr;
                let resp = builder.build_no_records(metadata);
                return match response_handle.send_response(resp).await {
                    Ok(info) => info,
                    Err(_) => ResponseInfo::from(Header {
                        metadata,
                        counts: HeaderCounts::default(),
                    }),
                };
            }
        };
        let name: Name = query.name().into();
        metrics::counter!("nauka_dns_queries_total").increment(1);

        let records: Vec<Record> = match query.query_type() {
            RecordType::A => {
                let client = asker_ip(request);
                self.best_nodes(client)
                    .into_iter()
                    .filter_map(|ip| match ip {
                        IpAddr::V4(v4) => Some(Record::from_rdata(
                            name.clone(),
                            DNS_TTL,
                            RData::A(rdata::A(v4)),
                        )),
                        IpAddr::V6(_) => None,
                    })
                    .collect()
            }
            RecordType::NS => {
                // Synthesized: ns1..ns3.<name> — the glue at the parent
                // decides which nodes those are; answering keeps
                // delegation checks happy.
                (1..=3u8)
                    .filter_map(|i| {
                        let ns = Name::from_utf8(format!("ns{i}.{name}")).ok()?;
                        Some(Record::from_rdata(
                            name.clone(),
                            DNS_TTL,
                            RData::NS(rdata::NS(ns)),
                        ))
                    })
                    .collect()
            }
            RecordType::SOA => {
                let serial = (crate::spaceauth::unix_now() / 60) as u32;
                match (
                    Name::from_utf8(format!("ns1.{name}")),
                    Name::from_utf8(format!("hostmaster.{name}")),
                ) {
                    (Ok(mname), Ok(rname)) => vec![Record::from_rdata(
                        name.clone(),
                        DNS_TTL,
                        RData::SOA(rdata::SOA::new(mname, rname, serial, 3600, 600, 86400, 60)),
                    )],
                    _ => Vec::new(),
                }
            }
            RecordType::TXT => {
                // ACME DNS-01: challenge values live in the replicated
                // state — a node writes its token, every NS serves it,
                // the CA reads it. The cluster is its own CA plumbing.
                let key = name.to_utf8().trim_end_matches('.').to_ascii_lowercase();
                self.state
                    .app
                    .app_state()
                    .acme_txt
                    .get(&key)
                    .map(|rows| {
                        rows.values()
                            .map(|v| {
                                Record::from_rdata(
                                    name.clone(),
                                    DNS_TTL,
                                    RData::TXT(rdata::TXT::new(vec![v.clone()])),
                                )
                            })
                            .collect()
                    })
                    .unwrap_or_default()
            }
            // AAAA and the rest: authoritative empty answer. The nodes
            // are IPv4-advertised today; an empty NOERROR tells the
            // resolver not to retry elsewhere.
            _ => Vec::new(),
        };

        let meta_copy = metadata;
        let resp = builder.build(metadata, records.iter(), [], [], []);
        match response_handle.send_response(resp).await {
            Ok(info) => info,
            Err(_) => ResponseInfo::from(Header {
                metadata: meta_copy,
                counts: HeaderCounts::default(),
            }),
        }
    }
}

/// Keeps `<data_dir>/geo.mmdb` present and fresh (DB-IP City Lite,
/// monthly), loading each new file into the running responder. The DNS
/// answers degrade gracefully while the first download runs.
pub async fn mmdb_keeper(dns: Arc<GeoDns>, data_dir: std::path::PathBuf) {
    let path = data_dir.join("geo.mmdb");
    loop {
        let age_ok = std::fs::metadata(&path)
            .and_then(|m| m.modified())
            .ok()
            .and_then(|t| t.elapsed().ok())
            .map(|e| e.as_secs() < MMDB_MAX_AGE_DAYS * 86_400)
            .unwrap_or(false);
        if !age_ok {
            if let Err(e) = download_mmdb(&path).await {
                eprintln!("geo-dns: database refresh failed ({e:#}) — retrying in 6h");
                tokio::time::sleep(std::time::Duration::from_secs(6 * 3600)).await;
                continue;
            }
        }
        match std::fs::read(&path) {
            Ok(bytes) => match maxminddb::Reader::from_source(bytes) {
                Ok(reader) => {
                    if let Ok(mut db) = dns.db.write() {
                        *db = Some(reader);
                    }
                    if let Ok(mut cache) = dns.positions.write() {
                        cache.clear();
                    }
                    dns.publish_node_location();
                    eprintln!("geo-dns: database loaded");
                }
                Err(e) => eprintln!("geo-dns: unreadable database: {e}"),
            },
            Err(e) => eprintln!("geo-dns: cannot read {}: {e}", path.display()),
        }
        tokio::time::sleep(std::time::Duration::from_secs(86_400)).await;
    }
}

/// Downloads the current month's DB-IP City Lite (falling back to the
/// previous month around publication day) and installs it atomically.
async fn download_mmdb(path: &std::path::Path) -> anyhow::Result<()> {
    use anyhow::Context as _;
    let now = crate::spaceauth::unix_now() as i64;
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(600))
        .build()?;
    let mut last_err = anyhow::anyhow!("no candidate month");
    for back in 0..2i64 {
        let secs = now - back * 30 * 86_400;
        let (y, m) = year_month(secs);
        let url = format!("https://download.db-ip.com/free/dbip-city-lite-{y:04}-{m:02}.mmdb.gz");
        match client.get(&url).send().await {
            Ok(resp) if resp.status().is_success() => {
                let gz = resp.bytes().await.context("downloading the database")?;
                let mut decoder = flate2::read::GzDecoder::new(&gz[..]);
                let mut raw = Vec::with_capacity(gz.len() * 3);
                std::io::Read::read_to_end(&mut decoder, &mut raw)
                    .context("decompressing the database")?;
                let tmp = path.with_extension("mmdb.tmp");
                std::fs::write(&tmp, &raw)?;
                std::fs::rename(&tmp, path)?;
                eprintln!(
                    "geo-dns: DB-IP {y:04}-{m:02} installed ({:.0} MB)",
                    raw.len() as f64 / 1e6
                );
                return Ok(());
            }
            Ok(resp) => last_err = anyhow::anyhow!("{url}: HTTP {}", resp.status()),
            Err(e) => last_err = anyhow::anyhow!("{url}: {e}"),
        }
    }
    Err(last_err)
}

/// (year, month) of a unix timestamp, UTC — enough calendar for a
/// download URL.
fn year_month(unix: i64) -> (i32, u32) {
    let days = unix / 86_400;
    // Civil-from-days (Howard Hinnant's algorithm), compact form.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y as i32, m as u32)
}

/// Binds :53 (UDP + TCP) and serves until shutdown. A bind failure is a
/// WARNING, not a crash: default-on must never take down a node that
/// lacks the capability — the operator either grants
/// CAP_NET_BIND_SERVICE or sets NAUKA_NO_DNS=1 to silence this.
pub async fn serve(dns: Arc<GeoDns>, bind_ip: IpAddr, port: u16) {
    // The advertised address, never the wildcard: 0.0.0.0:53 collides
    // with systemd-resolved's stub on 127.0.0.53, and a public front
    // door has no business on loopback anyway.
    let udp = match tokio::net::UdpSocket::bind((bind_ip, port)).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!(
                "geo-dns: cannot bind :{port} ({e}) — DNS front door disabled on this node. \
                 Grant CAP_NET_BIND_SERVICE to the service, or set NAUKA_NO_DNS=true."
            );
            return;
        }
    };
    let tcp = match tokio::net::TcpListener::bind((bind_ip, port)).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("geo-dns: cannot bind tcp :{port} ({e}) — serving UDP only");
            let mut server = hickory_server::Server::new(ArcHandler(dns));
            server.register_socket(udp);
            let _ = server.block_until_done().await;
            return;
        }
    };
    eprintln!("geo-dns: answering on {bind_ip}:{port} (udp+tcp)");
    let mut server = hickory_server::Server::new(ArcHandler(dns));
    server.register_socket(udp);
    server.register_listener(tcp, std::time::Duration::from_secs(5), 512 * 1024);
    let _ = server.block_until_done().await;
}

/// hickory wants an owned handler; ours lives behind an Arc.
#[derive(Clone)]
struct ArcHandler(Arc<GeoDns>);

#[async_trait::async_trait]
impl RequestHandler for ArcHandler {
    async fn handle_request<R: ResponseHandler, T: Time>(
        &self,
        request: &Request,
        response_handle: R,
    ) -> ResponseInfo {
        self.0
            .handle_request::<R, T>(request, response_handle)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn haversine_orders_the_world_correctly() {
        // Phuket → Singapore must beat Phuket → Helsinki.
        let phuket = (7.89, 98.40);
        let sin = (1.35, 103.82);
        let hel = (60.17, 24.94);
        assert!(
            haversine_km(phuket.0, phuket.1, sin.0, sin.1)
                < haversine_km(phuket.0, phuket.1, hel.0, hel.1)
        );
        // New York → Ashburn beats New York → Singapore.
        let ny = (40.71, -74.00);
        let ash = (39.04, -77.49);
        assert!(haversine_km(ny.0, ny.1, ash.0, ash.1) < haversine_km(ny.0, ny.1, sin.0, sin.1));
    }

    #[test]
    fn year_month_matches_known_dates() {
        assert_eq!(year_month(0), (1970, 1));
        assert_eq!(year_month(1_755_000_000), (2025, 8));
    }
}
