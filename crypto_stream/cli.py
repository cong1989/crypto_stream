import asyncio
import os
import subprocess
import sys
import time
from multiprocessing import Process

import click

KAFKA_HOME = os.environ.get("KAFKA_HOME", "/opt/kafka")


def wait_for_zookeeper():
    """Wait for Zookeeper to be ready"""
    retries = 30
    while retries > 0:
        try:
            subprocess.run(["nc", "-z", "localhost", "2181"], check=True)
            return True
        except:
            retries -= 1
            time.sleep(1)
    return False


def wait_for_kafka():
    """Wait for Kafka to be ready"""
    retries = 30
    while retries > 0:
        try:
            subprocess.run(["nc", "-z", "localhost", "9092"], check=True)
            return True
        except:
            retries -= 1
            time.sleep(1)
    return False


def check_prerequisites():
    """Check all prerequisites are met"""
    if not os.environ.get("TM_API_KEY"):
        click.echo("Error: TARDIS_API_KEY environment variable not set")
        return False

    if not os.path.exists(KAFKA_HOME):
        click.echo(f"Error: Kafka not found at {KAFKA_HOME}")
        return False

    if not check_docker():
        click.echo("Error: Docker is not running")
        return False

    return True


@click.group()
def main():
    """Crypto Stream Data Collection Tool"""
    pass


def run_streamer():
    """Run the Kafka streamer"""
    from crypto_stream.market_data.streaming.kafka_streamer import \
        main as streamer_main

    streamer_main()


def run_consumer():
    """Run the sampling recorder consumer"""
    from crypto_stream.market_data.processing.sampling_recorder_consumer import \
        main as consumer_main

    consumer_main()


@main.command()
def start():
    """Start all required services and data collection"""
    if not check_prerequisites():
        sys.exit(1)

    try:
        # Check if Tardis is already running
        tardis_check = subprocess.run(
            ["docker", "ps", "-q", "--filter", "ancestor=tardisdev/tardis-machine"],
            capture_output=True,
            text=True,
        )
        if not tardis_check.stdout:
            click.echo("Starting Tardis Machine...")
            cache_dir = os.path.abspath("./host-cache-dir")
            os.makedirs(cache_dir, exist_ok=True)

            tardis_cmd = [
                "docker",
                "run",
                "-d",
                "--user",
                f"{os.getuid()}:{os.getgid()}",  # Run as current user
                "-v",
                f"{cache_dir}:/.cache",
                "-p",
                "8000:8000",
                "-p",
                "8001:8001",
                "-e",
                f"TM_API_KEY={os.environ.get('TM_API_KEY')}",
                "tardisdev/tardis-machine",
            ]
            subprocess.run(tardis_cmd, check=True)
            time.sleep(7)

        # Start Zookeeper if not running
        if (
            subprocess.run(["pgrep", "-f", "zookeeper"], capture_output=True).returncode
            != 0
        ):
            click.echo("Starting Zookeeper...")
            zk_cmd = [
                os.path.join(KAFKA_HOME, "bin", "zookeeper-server-start.sh"),
                os.path.join(KAFKA_HOME, "config", "zookeeper.properties"),
            ]
            subprocess.Popen(zk_cmd)

            if not wait_for_zookeeper():
                click.echo("Error: Zookeeper failed to start")
                sys.exit(1)

        # Clean up old Kafka data if needed
        kafka_logs = os.path.join(KAFKA_HOME, "logs")
        if os.path.exists(kafka_logs):
            click.echo("Cleaning old Kafka data...")
            subprocess.run(["rm", "-rf", kafka_logs])

        # Start Kafka
        if (
            subprocess.run(
                ["pgrep", "-f", "kafka.Kafka"], capture_output=True
            ).returncode
            != 0
        ):
            click.echo("Starting Kafka...")
            kafka_cmd = [
                os.path.join(KAFKA_HOME, "bin", "kafka-server-start.sh"),
                os.path.join(KAFKA_HOME, "config", "server.properties"),
            ]
            subprocess.Popen(kafka_cmd)

            if not wait_for_kafka():
                click.echo("Error: Kafka failed to start")
                sys.exit(1)

        # Start streamer and consumer
        click.echo("Starting data streamer...")
        streamer_process = Process(target=run_streamer)
        streamer_process.start()

        time.sleep(5)  # Wait for streamer to initialize

        click.echo("Starting sampling recorder consumer...")
        consumer_process = Process(target=run_consumer)
        consumer_process.start()

        # Monitor processes
        while True:
            if not streamer_process.is_alive():
                click.echo("Streamer process died, restarting...")
                streamer_process = Process(target=run_streamer)
                streamer_process.start()

            if not consumer_process.is_alive():
                click.echo("Consumer process died, restarting...")
                consumer_process = Process(target=run_consumer)
                consumer_process.start()

            time.sleep(1)

    except KeyboardInterrupt:
        click.echo("\nShutting down...")
        if "streamer_process" in locals():
            streamer_process.terminate()
        if "consumer_process" in locals():
            consumer_process.terminate()
        stop()
    except Exception as e:
        click.echo(f"Error: {e}")
        sys.exit(1)


@main.command()
def stop():
    """Stop all services"""
    click.echo("Stopping services...")

    try:
        # Stop Kafka processes
        click.echo("Stopping Kafka...")
        subprocess.run([os.path.join(KAFKA_HOME, "bin", "kafka-server-stop.sh")])
        time.sleep(2)  # Give Kafka time to stop

        # Stop Zookeeper
        click.echo("Stopping Zookeeper...")
        subprocess.run([os.path.join(KAFKA_HOME, "bin", "zookeeper-server-stop.sh")])
        time.sleep(2)  # Give Zookeeper time to stop

        # Stop Tardis - Fixed command
        click.echo("Stopping Tardis Machine...")
        tardis_containers = subprocess.run(
            ["docker", "ps", "-q", "--filter", "ancestor=tardisdev/tardis-machine"],
            capture_output=True,
            text=True,
        )
        if tardis_containers.stdout:
            for container_id in tardis_containers.stdout.splitlines():
                subprocess.run(["docker", "stop", container_id])

        # Verify all stopped
        click.echo("\nVerifying services stopped...")

        # Check Tardis
        tardis = subprocess.run(
            ["docker", "ps", "-q", "--filter", "ancestor=tardisdev/tardis-machine"],
            capture_output=True,
            text=True,
        )
        if tardis.stdout:
            click.echo("Warning: Tardis Machine still running")

        # Check Kafka
        kafka_process = subprocess.run(
            ["pgrep", "-f", "kafka.Kafka"], capture_output=True
        )
        if kafka_process.returncode == 0:
            click.echo("Warning: Kafka still running")

        # Check Zookeeper
        zk_process = subprocess.run(["pgrep", "-f", "zookeeper"], capture_output=True)
        if zk_process.returncode == 0:
            click.echo("Warning: Zookeeper still running")

        click.echo("Stop command completed")

    except Exception as e:
        click.echo(f"Error stopping services: {e}")
        raise


def force_stop_service(pattern):
    """Force stop a service using pkill"""
    try:
        subprocess.run(["pkill", "-f", pattern])
        return True
    except:
        return False


@main.command()
def force_stop():
    """Force stop all services"""
    click.echo("Force stopping all services...")

    # Force stop Kafka
    force_stop_service("kafka.Kafka")

    # Force stop Zookeeper
    force_stop_service("zookeeper")

    # Force stop Tardis
    subprocess.run(
        [
            "docker",
            "kill",
            "$(docker ps -q --filter ancestor=tardisdev/tardis-machine)",
        ],
        shell=True,
    )

    click.echo("Force stop completed")


@main.command()
def status():
    """Check status of all services"""
    click.echo("Checking service status...")

    # Check Docker/Tardis
    try:
        tardis = subprocess.run(
            ["docker", "ps", "-q", "--filter", "ancestor=tardisdev/tardis-machine"],
            capture_output=True,
            text=True,
        )
        click.echo(f"Tardis Machine: {'Running' if tardis.stdout else 'Stopped'}")
    except:
        click.echo("Tardis Machine: Error checking status")

    # Check Kafka
    try:
        kafka_process = subprocess.run(
            ["pgrep", "-f", "kafka.Kafka"], capture_output=True
        )
        click.echo(
            f"Kafka: {'Running' if kafka_process.returncode == 0 else 'Stopped'}"
        )
    except:
        click.echo("Kafka: Error checking status")

    # Check Zookeeper
    try:
        zk_process = subprocess.run(["pgrep", "-f", "zookeeper"], capture_output=True)
        click.echo(
            f"Zookeeper: {'Running' if zk_process.returncode == 0 else 'Stopped'}"
        )
    except:
        click.echo("Zookeeper: Error checking status")


def check_docker():
    """Check if Docker is running"""
    try:
        subprocess.run(["docker", "info"], capture_output=True, check=True)
        return True
    except subprocess.CalledProcessError:
        return False


if __name__ == "__main__":
    main()
