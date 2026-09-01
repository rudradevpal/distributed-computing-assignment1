"""
main.py
-------
Start one process of the distributed system.

    python3 main.py <process_index> [config.json]

The same code runs all four roles. Which role you get is decided by the index,
and every host/port comes from the config file, so moving from one machine to
three lab nodes needs no source change.
"""

import json
import sys

import roles


def main():
    if len(sys.argv) < 2:
        print("usage: python3 main.py <process_index> [config.json]")
        sys.exit(1)

    idx = int(sys.argv[1])
    cfg_path = sys.argv[2] if len(sys.argv) > 2 else "config.json"

    with open(cfg_path) as f:
        cfg = json.load(f)

    if idx not in roles.ROLES:
        print("no role defined for index %d" % idx)
        sys.exit(1)

    proc = roles.build(cfg, idx)
    proc.log("starting as %s" % proc.name)
    proc.run()


if __name__ == "__main__":
    main()
