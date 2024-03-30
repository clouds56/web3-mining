Contracts
=========

| name | contract | height |
| ---- | -------- | ------ |
| Uniswap V2: Factory | `0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f` | [10_000_835](https://etherscan.io/tx/0xc31d7e7e85cab1d38ce1b8ac17e821ccd47dbde00f9d57f2bd8613bff9428396) |
| Uniswap V2: USDC | `0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc` | [10_008_355](https://etherscan.io/tx/0xd07cbde817318492092cc7a27b3064a69bd893c01cb593d6029683ffd290ab3a) |



Events
======

| name | topic1 | topic2 | topic 3 | data | signature | tx |
| ---- | ------ | ------ | ------- | ---- | --------- | -- |
| PairCreated [_](## "PairCreated (index_topic_1 address token0, index_topic_2 address token1, address pair, uint256)") | token0 | token1 |  | (pair, all_pair_len) | `0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9` | [tx](https://etherscan.io/tx/0xd07cbde817318492092cc7a27b3064a69bd893c01cb593d6029683ffd290ab3a#eventlog) |
| Sync [_](## "Sync (uint112 reserve0, uint112 reserve1") |  |  |  | (uint112 reserve0, uint112 reserve1) | `0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1` | [tx](https://etherscan.io/tx/0x2ef96febd1777e0403768e45e46dbd677f21079ba5f88297b500806b6fef23cb#eventlog) |
| Swap [_](## "Swap (index_topic_1 address sender, uint256 amount0In, uint256 amount1In, uint256 amount0Out, uint256 amount1Out, index_topic_2 address to)") | address sender | address to |  | (uint256 amount0In, uint256 amount1In, uint256 amount0Out, uint256 amount1Out) | `0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822` | [tx](https://etherscan.io/tx/0x932cb88306450d481a0e43365a3ed832625b68f036e9887684ef6da594891366#eventlog) |
| Transfer [_](## "Transfer (index_topic_1 address from, index_topic_2 address to, uint256 value)") | address from | address to |  | uint256 value | `0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef` | [tx](https://etherscan.io/tx/0x2ef96febd1777e0403768e45e46dbd677f21079ba5f88297b500806b6fef23cb#eventlog) |
| Mint [_](## "Mint (index_topic_1 address sender, uint256 amount0, uint256 amount1)") | address sender |  |  | (uint256 amount0, uint256 amount1) | `0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f` | [tx](https://etherscan.io/tx/0x2ef96febd1777e0403768e45e46dbd677f21079ba5f88297b500806b6fef23cb#eventlog) |
| Approval [_](## "Approval (index_topic_1 address owner, index_topic_2 address spender, uint256 value)") | address owner | address spender |  | uint256 value | `0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925` | [tx](https://etherscan.io/tx/0x71d6574a2d743cafc42e12bd1996f18c28d6231e7bfc8268b8133f71eb82d2a4#eventlog) |
| Burn [_](## "Burn (index_topic_1 address sender, uint256 amount0, uint256 amount1, index_topic_2 address to)") | address sender | address to |  | (uint256 amount0, uint256 amount1) | `0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496` | [tx](https://etherscan.io/tx/0x4113cf142204202124affdbf911b28fcb78ea5bd853effbcec130ba33ecf5045#eventlog) |
