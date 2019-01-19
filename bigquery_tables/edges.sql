select
  txin.input_pubkey_base58 as src,
  txout.output_pubkey_base58 as dst
from
  `[txs_view]` as txs,
  unnest(txs.outputs) as txout,
  unnest(txs.inputs) as txin
where
  txin.input_pubkey_base58 is not null
  and txout.output_pubkey_base58 is not null
group by txin.input_pubkey_base58, txout.output_pubkey_base58
