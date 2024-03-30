pub mod block;
pub mod uniswap;

pub trait ToHex {
  fn to_hex(&self) -> String;
}
pub trait ToChecksumHex {
  fn to_checksum_hex(&self) -> String;
}

impl ToChecksumHex for ethers_core::types::Address {
  fn to_checksum_hex(&self) -> String {
    ethers_core::utils::to_checksum(self, None)
  }
}

macro_rules! impl_to_hex {
  ($ty:ty) => {
    impl ToHex for $ty {
      fn to_hex(&self) -> String {
        if self.is_zero() {
          "0x0".to_string()
        } else {
          format!("0x{:x}", self)
        }
      }
    }
  };
}
// impl_to_hex!(ethers_core::types::H160);
impl_to_hex!(ethers_core::types::H256);

#[test]
fn test_to_hex() {
  use ethers_core::types::{Address, H256};
  assert_eq!(H256::zero().to_hex(), "0x0");
  assert_eq!(Address::from_low_u64_be(1).to_checksum_hex(), "0x0000000000000000000000000000000000000001");
}
