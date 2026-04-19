import argparse
import json
import subprocess
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

    parser.add_argument(
        '-s', '--shutdown',
        action='store_true',
        help='Shutdown server'
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

        if args.shutdown:
            count_replicas = len(config["cluster"]) - 1
            path_to_source = config["path_to_source"]

            subprocess.run(
                ["docker", "compose", "down", "-v"],
                env={
                    "PATH_TO_SOURCE": f"{path_to_source}",
                    "END_PORT": f"{10000 + count_replicas - 1}",
                    "START_PORT": "10000",
                    "COUNT_REPLICAS": f"{count_replicas}"
                },
                check=True
            )
            
            print("Containers shutdown successfully")
        else:
            postgresCluster = PostgresCluster(config=config, need_rebuild=args.need_rebuild,
                need_reinit=args.need_reinit, enable_debug=args.debug)
            postgresCluster.main_loop()
    except Exception as e:
        print(f"Some error occurred: {e}")
        exit(-1)
    except subprocess.CalledProcessError as e:
        print(f"Can not create containers, log {e}")
        exit(-2)
    except KeyboardInterrupt:
        postgresCluster.stop()
        exit(-3)

if __name__ == '__main__':
    main()