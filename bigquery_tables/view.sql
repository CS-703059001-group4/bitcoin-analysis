select *
from `bigquery-public-data.bitcoin_blockchain.transactions` as tx
where
  tx.timestamp >= 1488297600000
  and array_length(tx.outputs) = 1
  and array_length(tx.inputs) = 1
