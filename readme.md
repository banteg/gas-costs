# gas costs

a quick way to calculate gas costs for a bunch of accounts,
which works locally and privately against your ethereum client.

## how to use it

create an input file `addresses.txt` with one address on each line, then run to pull data from block
10,000,000 till head.
```
python gas_costs.py addresses.txt 10000000
```
the script will save both per-transaction and per-account reports in the `reports` folder, with
a block range in the name so you know where to pick up from next time.

the per-account file can be used for sending gas reimbursements.

## how it works

1. find all outgoing transactions using `trace_filter` using batches of 100,000 blocks.
2. fetch all receipts using jsonrpc batches and calc `gasUsed * effectiveGasPrice`.

everything is fetched in parallel and by the time traces are finished, receipts are usually finished too.
