select DISTINCT id from
(select
  txout.output_pubkey_base58 as id
from
  `[txs_view]` as txs,
  unnest(txs.outputs) as txout
where txout.output_pubkey_base58 is not null
union all
select
  txin.input_pubkey_base58 as address
from
  `voltaic-mode-225406.bitcoin.sample_10` as txs,
  unnest(txs.inputs) as txin
where txin.input_pubkey_base58 is not null)
