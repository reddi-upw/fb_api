import sys
import argparse
import json
from functools import partial

from client import FBClient


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-g', '--group_id', required=False)
    parser.add_argument('-q', '--query', required=False)
    parser.add_argument('-t', '--access_token', required=False)
    parser.add_argument('-i', '--app_id', required=False)
    parser.add_argument('-s', '--app_secret', required=False)
    parser.add_argument(
        '-l', '--limit', required=False, type=int, default=100)
    parser.add_argument('-o', '--output', required=False)
    args = parser.parse_args()

    client = FBClient(
        access_token=args.access_token,
        app_id=args.app_id,
        app_secret=args.app_secret)

    if args.group_id:
        group_id = args.group_id
    else:
        groups = list(client.search_groups(args.query, limit=1))
        if groups and groups[0]:
            group_id = groups[0][0]['id']
        else:
            return 'no group found for query {}'.format(args.query)

    field_names = []
    conns = []
    meta = client.fetch_metadata(group_id)
    for f in meta['fields']:
        field_names.append(f['name'])
    fields = ','.join(field_names)

    group = client.fetch_group(group_id, params={'fields': fields})

    excluded_conns = ('docs', 'live_videos',)
    for name in meta['connections'].keys():
        if name in excluded_conns:
            continue

        params = {}
        if name == 'picture':
            params.update({'redirect': 0})
        conn_gen = client.fetch_group_connection(
            group_id,
            name,
            params=params)
        conns = []
        for c in conn_gen:
            conns.extend(c)
        group[name] = conns

    dumped = json.dumps({'result': group}, indent=4)

    if args.output:
        with open(args.output, 'wb') as f:
            f.write(dumped)
    else:
        sys.stdout.write(dumped)


if __name__ == '__main__':
    main()
