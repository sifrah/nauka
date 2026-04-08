//! Network validation — CIDR parsing, containment, overlap checks.

/// Validate a CIDR is a valid private range (RFC 1918) with prefix /8-/24.
pub fn private_cidr(cidr: &str) -> anyhow::Result<ipnet::Ipv4Net> {
    let net: ipnet::Ipv4Net = cidr
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid CIDR '{cidr}': {e}"))?;

    let prefix = net.prefix_len();
    if !(8..=24).contains(&prefix) {
        anyhow::bail!("CIDR prefix must be between /8 and /24, got /{prefix}");
    }

    let ip = net.network();
    let is_private = ip.octets()[0] == 10
        || (ip.octets()[0] == 172 && (16..=31).contains(&ip.octets()[1]))
        || (ip.octets()[0] == 192 && ip.octets()[1] == 168);

    if !is_private {
        anyhow::bail!(
            "CIDR must be a private range (10.0.0.0/8, 172.16.0.0/12, or 192.168.0.0/16)"
        );
    }

    Ok(net)
}

/// Validate a subnet CIDR is within a VPC CIDR.
pub fn subnet_within_vpc(subnet_cidr: &str, vpc_cidr: &str) -> anyhow::Result<ipnet::Ipv4Net> {
    let subnet: ipnet::Ipv4Net = subnet_cidr
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid subnet CIDR '{subnet_cidr}': {e}"))?;
    let vpc: ipnet::Ipv4Net = vpc_cidr
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid VPC CIDR '{vpc_cidr}': {e}"))?;

    if !vpc.contains(&subnet) {
        anyhow::bail!("subnet {subnet} is not within VPC CIDR {vpc}");
    }

    Ok(subnet)
}

/// Check that a new CIDR doesn't overlap with any existing CIDRs.
pub fn no_overlap(new_cidr: &ipnet::Ipv4Net, existing: &[String]) -> anyhow::Result<()> {
    for existing_str in existing {
        if let Ok(existing_net) = existing_str.parse::<ipnet::Ipv4Net>() {
            if new_cidr.contains(&existing_net) || existing_net.contains(new_cidr) {
                anyhow::bail!("CIDR {new_cidr} overlaps with existing {existing_net}");
            }
        }
    }
    Ok(())
}

/// Compute the gateway IP (first host address) from a CIDR.
pub fn gateway(cidr: &ipnet::Ipv4Net) -> String {
    let hosts = cidr.hosts();
    hosts
        .into_iter()
        .next()
        .map(|ip| ip.to_string())
        .unwrap_or_else(|| cidr.network().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_private_cidrs() {
        assert!(private_cidr("10.0.0.0/16").is_ok());
        assert!(private_cidr("172.16.0.0/12").is_ok());
        assert!(private_cidr("192.168.0.0/24").is_ok());
    }

    #[test]
    fn reject_public_cidr() {
        assert!(private_cidr("8.8.8.0/24").is_err());
    }

    #[test]
    fn reject_bad_prefix() {
        assert!(private_cidr("10.0.0.0/4").is_err()); // too wide
        assert!(private_cidr("10.0.0.0/28").is_err()); // too narrow
    }

    #[test]
    fn subnet_containment() {
        assert!(subnet_within_vpc("10.0.1.0/24", "10.0.0.0/16").is_ok());
        assert!(subnet_within_vpc("10.1.0.0/24", "10.0.0.0/16").is_err());
    }

    #[test]
    fn overlap_detection() {
        let net: ipnet::Ipv4Net = "10.0.1.0/24".parse().unwrap();
        assert!(no_overlap(&net, &["10.0.2.0/24".to_string()]).is_ok());
        assert!(no_overlap(&net, &["10.0.1.0/25".to_string()]).is_err());
        assert!(no_overlap(&net, &["10.0.0.0/16".to_string()]).is_err());
    }

    #[test]
    fn gateway_computation() {
        let net: ipnet::Ipv4Net = "10.0.1.0/24".parse().unwrap();
        assert_eq!(gateway(&net), "10.0.1.1");
    }
}
