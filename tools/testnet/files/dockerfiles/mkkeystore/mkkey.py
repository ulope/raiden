#!/usr/bin/env python3
import json
import logging
from datetime import datetime
from json import JSONEncoder
from pathlib import Path

import click
from eth_keyfile import create_keyfile_json
from sha3 import keccak_256

from raiden_libs.utils import to_checksum_address


class BytesJSONEncoder(JSONEncoder):
    def default(self, o):
        if isinstance(o, bytes):
            return o.decode('UTF-8')
        return super().default(o)


@click.command()
@click.option("--date-string", default=datetime.now().isoformat(), show_default=True)
@click.option("--key-label")
@click.option("-o", "--output-dir", show_default=True, default=".",
              type=click.Path(exists=True, file_okay=False, writable=True))
@click.argument("password")
@click.argument("private_key_seed", nargs=-1)
def main(password, private_key_seed, date_string, key_label, output_dir):
    output_dir = Path(output_dir)
    private_key_bin = keccak_256("".join(seed for seed in private_key_seed).encode("UTF-8"))

    password = password.encode("UTF-8")

    key = create_keyfile_json(private_key_bin, password)

    filename = "UTC--{date}--{label}".format(
        date=date_string,
        label=key_label if key_label else key['address'],
    )

    with output_dir.joinpath(filename).open("w") as f:
        json.dump(key, f, cls=BytesJSONEncoder)

    address = to_checksum_address(key['address'])

    output_dir.joinpath(f"{filename}.address").write_text(address)
    print(f"0x{address}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.ERROR)
    main()
