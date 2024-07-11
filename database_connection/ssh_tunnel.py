import time
from sshtunnel import SSHTunnelForwarder
import logging


class SSHTunnelManager:
    """Class to manage SSH tunneling for database connections."""

    def __init__(self, ssh_host, ssh_user, ssh_pass, ssh_port, remote_host, remote_port):
        """
        Initialize the SSH tunnel manager.

        Args:
            ssh_host (str): SSH server hostname.
            ssh_user (str): SSH username.
            ssh_pass (str): SSH password.
            ssh_port (int): SSH port.
            remote_host (str): Remote database server hostname.
            remote_port (int): Remote database server port.
        """
        self.ssh_host = ssh_host
        self.ssh_user = ssh_user
        self.ssh_pass = ssh_pass
        self.ssh_port = ssh_port
        self.remote_host = remote_host
        self.remote_port = remote_port
        self.ssh_tunnel = None
        self.logger = logging.getLogger(__name__)

    def create_ssh_tunnel(self, max_retries=5, initial_backoff=1):
        """
        Create an SSH tunnel.

        Args:
            max_retries (int): Maximum number of retries if the tunnel creation fails.
            initial_backoff (int): Initial backoff time in seconds.

        Returns:
            int: Local bind port of the SSH tunnel.

        Raises:
            Exception: If the SSH tunnel cannot be created after max retries.
        """
        retries = 0
        while True:
            try:
                self.ssh_tunnel = SSHTunnelForwarder(
                    (self.ssh_host, self.ssh_port),
                    ssh_username=self.ssh_user,
                    ssh_password=self.ssh_pass,
                    remote_bind_address=(self.remote_host, self.remote_port),
                    set_keepalive=60.0,
                )
                self.ssh_tunnel.start()
                self.logger.info("SSH tunnel started successfully.")
                return self.ssh_tunnel.local_bind_port
            except Exception as e:
                retries += 1
                backoff_time = initial_backoff * (2 ** (retries - 1))
                if retries > max_retries:
                    self.logger.error(
                        f"Exceeded max retries ({max_retries}). Failed to start SSH tunnel: {e}"
                    )
                    raise
                self.logger.warning(
                    f"Failed to start SSH tunnel (attempt {retries}). Retrying in {backoff_time} seconds..."
                )
                time.sleep(backoff_time)

    def close(self):
        """Close the SSH tunnel."""
        if self.ssh_tunnel:
            self.ssh_tunnel.close()
            self.logger.info("SSH tunnel closed.")
