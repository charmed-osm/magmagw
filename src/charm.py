#!/usr/bin/env python3

import sys

sys.path.append("lib")

from ops.charm import CharmBase
from ops.framework import StoredState
from ops.main import main
from ops.model import (
    ActiveStatus,
    BlockedStatus,
    MaintenanceStatus,
    WaitingStatus,
    ModelError,
)
import os
import subprocess
from proxy_cluster import ProxyCluster

# import charms.requirementstxt


def install_dependencies():
    # Make sure Python3 + PIP are available
    if not os.path.exists("/usr/bin/python3") or not os.path.exists("/usr/bin/pip3"):
        # This is needed when running as a k8s charm, as the ubuntu:latest
        # image doesn't include either package.

        # Update the apt cache
        subprocess.check_call(["apt-get", "update"])

        # Install the Python3 package
        subprocess.check_call(["apt-get", "install", "-y", "python3", "python3-pip"],)

    # Install the build dependencies for our requirements (paramiko)
    subprocess.check_call(["apt-get", "install", "-y", "libffi-dev", "libssl-dev"],)

    REQUIREMENTS_TXT = "{}/requirements.txt".format(os.environ["JUJU_CHARM_DIR"])
    if os.path.exists(REQUIREMENTS_TXT):
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "-r", REQUIREMENTS_TXT],
        )


try:
    from charms.osm.sshproxy import SSHProxy
except Exception as ex:
    install_dependencies()
    from charms.osm.sshproxy import SSHProxy


class SimpleHAProxyCharm(CharmBase):
    state = StoredState()

    def __init__(self, *args):
        super().__init__(*args)

        # An example of setting charm state
        # that's persistent across events
        self.state.set_default(is_started=False)

        self.peers = ProxyCluster(self, "proxypeer")

        if not self.state.is_started:
            self.state.is_started = True

        # Register all of the events we want to observe
        for event in (
            # Charm events
            self.on.config_changed,
            self.on.install,
            self.on.upgrade_charm,
            # Charm actions (primitives)
            self.on.touch_action,
            # OSM actions (primitives)
            self.on.start_action,
            self.on.stop_action,
            self.on.restart_action,
            self.on.reboot_action,
            self.on.upgrade_action,
            # SSH Proxy actions (primitives)
            self.on.generate_ssh_key_action,
            self.on.get_ssh_public_key_action,
            self.on.run_action,
            self.on.verify_ssh_credentials_action,
        ):
            self.framework.observe(event, self)

        self.framework.observe(self.on.proxypeer_relation_changed, self)

    def get_ssh_proxy(self):
        """Get the SSHProxy instance"""
        proxy = SSHProxy(
            hostname=self.model.config["ssh-hostname"],
            username=self.model.config["ssh-username"],
            password=self.model.config["ssh-password"],
        )
        return proxy

    def on_proxypeer_relation_changed(self, event):
        if self.peers.is_cluster_initialized:
            pubkey = self.peers.ssh_public_key
            privkey = self.peers.ssh_private_key
            SSHProxy.generate_ssh_key(public=pubkey, private=privkey)
            self.on_config_changed(event)
        else:
            event.defer()

    def on_config_changed(self, event):
        """Handle changes in configuration"""
        unit = self.model.unit

        # Unit should go into a waiting state until verify_ssh_credentials is successful
        unit.status = WaitingStatus("Waiting for SSH credentials")
        proxy = self.get_ssh_proxy()

        verified = proxy.verify_credentials()
        if verified:
            unit.status = ActiveStatus()
        else:
            unit.status = BlockedStatus("Invalid SSH credentials.")


    def on_install(self, event):
        """Called when the charm is being installed"""
        unit = self.model.unit

        if not SSHProxy.has_ssh_key():
            unit.status = MaintenanceStatus("Generating SSH keys...")
            try:
                self.verify_leadership()
                print("Generating SSH Keys")
                SSHProxy.generate_ssh_key()
                ssh_public_key = SSHProxy.get_ssh_public_key()
                ssh_private_key = SSHProxy.get_ssh_private_key()
                self.peers.on.cluster_initialized.emit(ssh_public_key, ssh_private_key)
                unit.status = ActiveStatus()
            except (LeadershipError) as e:
                unit.status = WaitingStatus("Waiting for leader to populate the keys")

    def on_touch_action(self, event):
        """Touch a file."""
        try:
            self.verify_leadership()
        except (LeadershipError) as e:
            event.fail(e)
            return

        try:
            filename = event.params["filename"]
            proxy = self.get_ssh_proxy()

            stdout, stderr = proxy.run("touch {}".format(filename))
            event.set_results({"output": stdout})
        except Exception as ex:
            event.fail(ex)

    def on_upgrade_charm(self, event):
        """Upgrade the charm."""
        unit = self.model.unit

        # Mark the unit as under Maintenance.
        unit.status = MaintenanceStatus("Upgrading charm")

        self.on_install(event)

        # When maintenance is done, return to an Active state
        unit.status = ActiveStatus()

    ###############
    # OSM methods #
    ###############
    def on_start_action(self, event):
        """Start the VNF service on the VM."""
        pass

    def on_stop_action(self, event):
        """Stop the VNF service on the VM."""
        pass

    def on_restart_action(self, event):
        """Restart the VNF service on the VM."""
        pass

    def on_reboot_action(self, event):
        """Reboot the VM."""
        try:
            self.verify_leadership()
            proxy = self.get_ssh_proxy()
            stdout, stderr = proxy.run("sudo reboot")
            if len(stderr):
                event.fail(stderr)
        except (LeadershipError) as e:
            event.fail(e)
            return

    def on_upgrade_action(self, event):
        """Upgrade the VNF service on the VM."""
        pass

    #####################
    # SSH Proxy methods #
    #####################
    def on_generate_ssh_key_action(self, event):
        """Generate a new SSH keypair for this unit."""
        try:
            self.verify_leadership()
            if not SSHProxy.generate_ssh_key():
                event.fail("Unable to generate ssh key")
        except (LeadershipError) as e:
            event.fail(e)
            return

    def on_get_ssh_public_key_action(self, event):
        """Get the SSH public key for this unit."""
        try:
            self.verify_leadership()
            pubkey = SSHProxy.get_ssh_public_key()
            event.set_results({"pubkey": SSHProxy.get_ssh_public_key()})
        except (LeadershipError) as e:
            event.fail(e)
            return

    def on_run_action(self, event):
        """Run an arbitrary command on the remote host."""
        try:
            self.verify_leadership()
            cmd = event.params["command"]
            proxy = self.get_ssh_proxy()
            stdout, stderr = proxy.run(cmd)
            event.set_results({"output": stdout})
            if len(stderr):
                event.fail(stderr)
        except (LeadershipError) as e:
            event.fail(e)
            return

    def on_verify_ssh_credentials_action(self, event):
        """Verify the SSH credentials for this unit."""
        try:
            self.verify_leadership()
            proxy = self.get_ssh_proxy()

            verified = proxy.verify_credentials()
            if verified:
                print("Verified!")
                event.set_results({"verified": True})
            else:
                print("Verification failed!")
                event.set_results({"verified": False})
        except (LeadershipError) as e:
            event.fail(e)
            return

    def verify_leadership(self):
        if not self.model.unit.is_leader():
            raise LeadershipError()


class LeadershipError(ModelError):
    def __init__(self):
        super().__init__("not leader")


if __name__ == "__main__":
    main(SimpleHAProxyCharm)
