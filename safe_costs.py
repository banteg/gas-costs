import click
import httpx
import pandas as pd

from decimal import Decimal
import csv
from dask.distributed import Client, as_completed
from rich.progress import Progress
from web3 import Web3
from pathlib import Path
from web3._utils.method_formatters import receipt_formatter
from web3.middleware.filter import block_ranges
from toolz import groupby, valmap, pluck


client = Client(processes=False)
web3 = Web3(Web3.HTTPProvider("http://127.0.0.1:8545", request_kwargs={"timeout": 60}))


def get_traces(from_block, to_block, from_address, to_address):
    return web3.manager.request_blocking(
        "trace_filter",
        [
            {
                "fromBlock": hex(from_block),
                "toBlock": hex(to_block),
                "fromAddress": from_address,
                "toAddress": to_address,
            }
        ],
    )


def jsonrpc_batch(calls):
    request = [
        {"jsonrpc": "2.0", "id": i, "method": method, "params": args}
        for i, (method, args) in enumerate(calls)
    ]
    response = httpx.post(web3.provider.endpoint_uri, json=request)
    return [item["result"] for item in response.json()]


def get_receipts(txn_hashes):
    receipts = []
    if not txn_hashes:
        return receipts
    response = jsonrpc_batch(
        [["eth_getTransactionReceipt", [txn_hash]] for txn_hash in txn_hashes]
    )

    for item in response:
        receipt = receipt_formatter(item)
        receipts.append(
            {
                "sender": receipt["from"],
                "block_number": receipt["blockNumber"],
                "txn_index": receipt["transactionIndex"],
                "txn_hash": receipt["transactionHash"].hex(),
                "status": receipt["status"],
                "gas_cost": receipt["gasUsed"] * receipt["effectiveGasPrice"],
            }
        )
    return receipts


@click.command()
@click.argument("input_file", type=click.File("rt"))
@click.argument("start_block", type=click.IntRange(min=0))
def main(input_file, start_block):
    to_address = input_file.read().splitlines()
    end_block = web3.eth.block_number
    step = 100_000

    with Progress() as progress:
        trace_tasks = []
        for a, b in block_ranges(start_block, end_block, step):
            trace_tasks.append(
                client.submit(
                    get_traces, a, b, from_address=None, to_address=to_address
                )
            )

        trace_bar = progress.add_task("traces", total=len(trace_tasks))
        receipt_tasks = []
        for futures, traces in as_completed(trace_tasks, with_results=True):
            eligible_traces = [
                trace["transactionHash"]
                for trace in traces
                if trace["type"] == "call"
                and trace["action"]["callType"] == "call"
                and trace["action"]["input"][:10] in ["0x6a761202"]
            ]
            receipt_tasks.append(client.submit(get_receipts, eligible_traces))
            progress.update(trace_bar, advance=1)

        receipt_bar = progress.add_task("receipts", total=len(receipt_tasks))
        receipts = []
        for futures, result in as_completed(receipt_tasks, with_results=True):
            receipts.extend(result)
            progress.update(receipt_bar, advance=1)

    Path("reports").mkdir(exist_ok=True)
    prefix = f"reports/{start_block}-{end_block}"
    df = pd.DataFrame(receipts).sort_values(["block_number", "txn_index"])
    df.to_csv(f"{prefix}-transactions.csv", index=False)

    costs = {
        sender: sum(item["gas_cost"] for item in items)
        for sender, items in groupby("sender", receipts).items()
    }
    costs = sorted(costs.items(), key=lambda x: x[1], reverse=True)

    for sender, cost in costs:
        print(sender, Decimal(cost) / 10**18)

    with open(f"{prefix}-gas-costs.csv", "wt") as f:
        writer = csv.writer(f)
        writer.writerow(["sender", "gas_cost"])
        writer.writerows(costs)


if __name__ == "__main__":
    main()
