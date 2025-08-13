import os
import argparse
from postgres_cluster import PostgresCluster
from console import ConsoleView

def get_config_from_args():
    parser = argparse.ArgumentParser(description='Check cascade sync replication')

    parser.add_argument(
        'path_to_source',
        type=str,
        help='path to PostgreSQL source code'
    )

    parser.add_argument(
        '-o', '--out_dir',
        type=str,
        default=os.path.join(os.getcwd(), "tmp"),
        help='path to output directory'
    )

    parser.add_argument(
        '-l', '--lines',
        nargs='+',
        type=int,
        default=2,
        help='list of count ndes in line')
    
    parser.add_argument(
        '-r', '--need_rebuild',
        action='store_true',
        help='need rebuild postgres?'
    )

    parser.add_argument(
        '-i', '--need_reinit',
        action='store_true',
        help='need reinit postgres?'
    )

    parser.add_argument(
        '-s', '--sync_commit',
        type=str,
        default='write',
        choices=['write', 'flush', 'apply'],
        help='synchronous_commit setting'
    )

    return parser.parse_args()

def main():
    try:
        config = get_config_from_args()
        postgresCluster = PostgresCluster(config=config, view=ConsoleView())
        postgresCluster.main_loop()
    except Exception as e:
        print(f"Some error occurred: {e}")
        exit(-1)
    except KeyboardInterrupt:
        exit(-2)

if __name__ == '__main__':
    main()