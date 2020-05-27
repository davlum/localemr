import moto.server as server
import argparse
import sys


# Replace moto emr with the custom emr implementation
sys.modules['moto.emr.urls'] = __import__('localemr.emr.urls')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Local AWS EMR - A local service that imitates AWS EMR ")
    parser.add_argument('-p', '--port', help='Port to run the service on', type=int, default=3000)
    parser.add_argument("-H", "--host", type=str, help="Which host to bind", default="0.0.0.0")
    args = parser.parse_args()
    server.main(['emr', '-H', args.host, '--port', str(args.port)])
