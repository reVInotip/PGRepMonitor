import argparse
import json
from postgres_cluster import PostgresCluster

def get_args():
    parser = argparse.ArgumentParser(description='Check cascade sync replication')
    
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
        '-d', '--debug',
        action='store_true',
        help='Start in debug mode'
    )

    return parser.parse_args()

def parse_cluster_config():
    config = None
    with open('config.json', 'r') as f:
        config = json.load(f)
    
    return config

def main():
    try:
        args = get_args()
        config = parse_cluster_config()
        postgresCluster = PostgresCluster(config=config, need_rebuild=args.need_rebuild,
            need_reinit=args.need_reinit, enable_debug=args.debug)
        postgresCluster.main_loop()
    except Exception as e:
        print(f"Some error occurred: {e}")
        exit(-1)
    except KeyboardInterrupt:
        postgresCluster.stop()
        exit(-2)

if __name__ == '__main__':
    main()